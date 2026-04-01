"""
runner/run.py — v2
Multi-pass orchestrator with statistical scoring and competitor mode.
"""
import time
import traceback

from db.connection import (
    get_brand, get_active_brands, get_prompts_for_brand,
    get_active_engines, create_run, complete_run,
    insert_result, insert_citations,
)
from runner.engines import query_engine
from parser.analyzer import analyze
from config.settings import PASSES_PER_PROMPT


def run_brand(brand_id: int, triggered_by: str = "manual",
              passes: int = None, target_brand_id: int = None):
    """
    Run prompt simulation for a brand.
    target_brand_id: if set, use that brand's aliases/domain for detection
    (competitor mode — runs client prompts but looks for competitor mentions)
    """
    n_passes = passes or PASSES_PER_PROMPT
    brand    = get_brand(brand_id)
    prompts  = get_prompts_for_brand(brand_id)
    engines  = get_active_engines()

    if not prompts: print(f"[{brand['name']}] No active prompts."); return
    if not engines: print(f"[{brand['name']}] No active engines."); return

    if target_brand_id and target_brand_id != brand_id:
        tb        = get_brand(target_brand_id)
        aliases   = tb.get("brand_aliases") or [tb["name"]]
        domain    = tb.get("domain") or ""
        comp_list = []
        label     = f"{brand['name']} → target: {tb['name']}"
    else:
        aliases   = brand.get("brand_aliases") or [brand["name"]]
        domain    = brand.get("domain") or ""
        comp_list = brand.get("competitors") or []
        label     = brand["name"]

    run_id = create_run(brand_id, triggered_by, len(prompts), len(engines))
    total  = len(prompts) * len(engines) * n_passes

    print(f"\n{'='*65}")
    print(f"  Brand:   {label}")
    print(f"  Run ID:  {run_id}  |  {len(prompts)}p × {len(engines)}e × {n_passes} passes = {total} queries")
    print(f"{'='*65}")

    completed = failed = 0

    for prompt in prompts:
        for engine in engines:
            pass_results = []

            for pass_num in range(1, n_passes + 1):
                lbl = f"[P{prompt['id']} × {engine['slug']} pass{pass_num}/{n_passes}]"
                try:
                    print(f"  {lbl} ... ", end="", flush=True)
                    raw      = query_engine(engine["slug"], prompt["prompt_text"])
                    analysis = analyze(raw, aliases, domain, comp_list)
                    pass_results.append({"raw": raw, "analysis": analysis})
                    print("OK")
                    completed += 1
                except Exception as e:
                    failed += 1
                    print(f"FAILED — {type(e).__name__}: {e}")
                time.sleep(1.5)

            if not pass_results:
                continue

            n            = len(pass_results)
            mentioned_n  = sum(1 for r in pass_results if r["analysis"]["brand_mentioned"])
            mention_rate = round(mentioned_n / n, 4)
            total_count  = sum(r["analysis"]["mention_count"] for r in pass_results)

            rank = {"first":1,"second":2,"passing":3,"none":4}
            best_pos = min([r["analysis"]["position"] for r in pass_results],
                           key=lambda p: rank.get(p,4))

            sentiments = [r["analysis"]["sentiment"] for r in pass_results]
            sentiment  = max(set(sentiments), key=sentiments.count)

            comp_found = any(r["analysis"]["competitor_mentioned"] for r in pass_results)

            all_cit = {}
            for r in pass_results:
                for c in r["analysis"]["citations"]:
                    all_cit.setdefault(c["domain"], c)

            result_id = insert_result(
                run_id=run_id,
                prompt_id=prompt["id"],
                engine_id=engine["id"],
                raw_response=pass_results[0]["raw"],
                brand_mentioned=(mentioned_n > 0),
                mention_count=total_count,
                position=best_pos,
                sentiment=sentiment,
                competitor_mentioned=comp_found,
                mention_rate=mention_rate,
                passes_run=n,
                passes_mentioned=mentioned_n,
            )
            insert_citations(result_id, list(all_cit.values()))

            icon = "✓" if mentioned_n > 0 else "✗"
            print(f"    {icon} rate={mention_rate:.0%} ({mentioned_n}/{n}) "
                  f"pos={best_pos} sent={sentiment} cit={len(all_cit)}")

    complete_run(run_id, "complete" if failed==0 else "complete_with_errors")
    print(f"\n  Run {run_id} done — {completed} OK, {failed} failed.\n{'='*65}\n")
    return run_id


def run_all_brands(triggered_by="scheduler", passes=None):
    for brand in get_active_brands():
        try:
            run_brand(brand["id"], triggered_by=triggered_by, passes=passes)
        except Exception as e:
            print(f"[{brand['name']}] FAILED: {e}")
            traceback.print_exc()
