"""
research/onboarding.py
Client onboarding orchestrator.
Runs the full six-layer prompt research pipeline and seeds the database.

Usage:
    python onboarding.py --brand-id 1 --seed-keyword "blueprint printing"
    python onboarding.py --brand-id 1 --seed-keyword "blueprint printing" --competitor-domain docucopies.com
"""
import argparse
import sys
sys.path.insert(0, '..')

from db.connection import db_cursor, get_brand
from config.settings import DATAFORSEO_LOGIN, OPENAI_API_KEY


def run_onboarding(brand_id: int, seed_keyword: str,
                   competitor_domain: str = None,
                   skip_dataforseo: bool = False,
                   n_ai_prompts: int = 20,
                   max_total_prompts: int = 50):
    """
    Full onboarding pipeline for a brand.
    Combines all research layers into a single validated prompt matrix.
    """
    brand = get_brand(brand_id)
    if not brand:
        print(f"Brand ID {brand_id} not found.")
        return

    print(f"\n{'='*60}")
    print(f"  Onboarding: {brand['name']}")
    print(f"  Seed keyword: {seed_keyword}")
    print(f"{'='*60}")

    all_candidates = []

    # ── Layer 1: DataForSEO keyword research ─────────────────────
    if not skip_dataforseo and DATAFORSEO_LOGIN:
        print("\n[Layer 1] DataForSEO keyword research...")
        try:
            from research.dataforseo import DataForSEOClient, keywords_to_prompts
            client = DataForSEOClient()
            keywords = client.get_keyword_ideas(seed_keyword, limit=100)
            dfs_prompts = keywords_to_prompts(keywords, brand_name=brand["name"])
            all_candidates.extend(dfs_prompts)
            print(f"  → {len(dfs_prompts)} prompts from DataForSEO")

            if competitor_domain:
                print(f"  → Pulling competitor keywords for {competitor_domain}...")
                comp_kws = client.get_competitor_keywords(competitor_domain, limit=50)
                comp_prompts = keywords_to_prompts(comp_kws, brand_name=brand["name"], max_prompts=20)
                for p in comp_prompts:
                    p["source"] = "dataforseo_competitor"
                all_candidates.extend(comp_prompts)
                print(f"  → {len(comp_prompts)} prompts from competitor keywords")
        except Exception as e:
            print(f"  DataForSEO error: {e} — skipping")
    else:
        print("[Layer 1] DataForSEO skipped (no credentials or --skip-dataforseo)")

    # ── Layer 3: AI-native query generation ──────────────────────
    if OPENAI_API_KEY:
        print("\n[Layer 3] AI-native prompt generation...")
        try:
            from research.prompt_generator import generate_natural_prompts, generate_competitor_prompts
            service_desc = f"{seed_keyword} service — {brand.get('domain', '')}"

            # General prompts
            ai_prompts = generate_natural_prompts(service_desc, n_prompts=n_ai_prompts)
            all_candidates.extend(ai_prompts)
            print(f"  → {len(ai_prompts)} AI-generated natural prompts")

            # Competitor comparison prompts
            competitors = brand.get("competitors") or []
            if competitors:
                comp_prompts = generate_competitor_prompts(
                    brand["name"], competitors[:3], seed_keyword
                )
                all_candidates.extend(comp_prompts)
                print(f"  → {len(comp_prompts)} AI-generated competitor comparison prompts")
        except Exception as e:
            print(f"  AI generation error: {e} — skipping")
    else:
        print("[Layer 3] AI generation skipped (no OpenAI key)")

    # ── Vendor-aware prompts (always include) ─────────────────────
    brand_name = brand["name"]
    domain = brand.get("domain", "")
    vendor_prompts = [
        {"prompt_text": f"{brand_name} review — is it legit?",               "intent": "vendor-aware", "source": "manual"},
        {"prompt_text": f"{brand_name} pricing and turnaround time",          "intent": "vendor-aware", "source": "manual"},
        {"prompt_text": f"Is {brand_name} good for {seed_keyword}?",          "intent": "vendor-aware", "source": "manual"},
        {"prompt_text": f"{brand_name} {seed_keyword} service nationwide review","intent":"vendor-aware","source":"manual"},
        {"prompt_text": f"{domain} review",                                    "intent": "vendor-aware", "source": "manual"},
    ]
    all_candidates.extend(vendor_prompts)
    print(f"\n[Manual] Added {len(vendor_prompts)} vendor-aware control prompts")

    # ── Deduplicate and select top prompts ────────────────────────
    seen = set()
    unique = []
    for p in all_candidates:
        pt = p.get("prompt_text", "").strip().lower()
        if pt and pt not in seen and len(pt) > 10:
            seen.add(pt)
            unique.append(p)

    # Sort by search volume (if available), keeping intent balance
    intent_targets = {
        "task-based":     int(max_total_prompts * 0.25),
        "solution-aware": int(max_total_prompts * 0.25),
        "problem-aware":  int(max_total_prompts * 0.20),
        "comparison":     int(max_total_prompts * 0.175),
        "vendor-aware":   int(max_total_prompts * 0.125),
    }

    final_prompts = []
    intent_counts = {k: 0 for k in intent_targets}

    # First pass: fill to targets
    for p in unique:
        intent = p.get("intent", "solution-aware")
        target = intent_targets.get(intent, 5)
        if intent_counts.get(intent, 0) < target:
            final_prompts.append(p)
            intent_counts[intent] = intent_counts.get(intent, 0) + 1

    # Second pass: fill remaining slots
    for p in unique:
        if len(final_prompts) >= max_total_prompts:
            break
        pt = p.get("prompt_text", "").lower()
        if not any(fp.get("prompt_text","").lower() == pt for fp in final_prompts):
            final_prompts.append(p)

    print(f"\n[Summary] {len(final_prompts)} prompts selected (target: {max_total_prompts})")
    for intent, count in intent_counts.items():
        print(f"  {intent}: {count}")

    # ── Seed into database ────────────────────────────────────────
    print(f"\n[Database] Seeding {len(final_prompts)} prompts for brand_id={brand_id}...")
    seeded = 0
    with db_cursor() as cur:
        for p in final_prompts:
            prompt_text = p.get("prompt_text", "").strip()
            if not prompt_text:
                continue
            # Check for duplicates
            cur.execute(
                "SELECT id FROM prompts WHERE brand_id=%s AND prompt_text=%s",
                (brand_id, prompt_text)
            )
            if cur.fetchone():
                continue
            cur.execute("""
                INSERT INTO prompts (brand_id, prompt_text, intent, scope, audience)
                VALUES (%s, %s, %s, 'national', %s)
            """, (brand_id, prompt_text,
                  p.get("intent", "solution-aware"),
                  p.get("audience", "General")))
            seeded += 1

    print(f"  → Seeded {seeded} new prompts ({len(final_prompts)-seeded} duplicates skipped)")
    print(f"\n  Onboarding complete. Run: python main.py --brand-id {brand_id}")
    return final_prompts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rankovi Client Onboarding")
    parser.add_argument("--brand-id",         type=int,  required=True)
    parser.add_argument("--seed-keyword",     type=str,  required=True)
    parser.add_argument("--competitor-domain",type=str,  default=None)
    parser.add_argument("--skip-dataforseo",  action="store_true")
    parser.add_argument("--max-prompts",      type=int,  default=50)
    args = parser.parse_args()

    run_onboarding(
        brand_id=args.brand_id,
        seed_keyword=args.seed_keyword,
        competitor_domain=args.competitor_domain,
        skip_dataforseo=args.skip_dataforseo,
        max_total_prompts=args.max_prompts,
    )
