"""
research/dataforseo.py
DataForSEO API client for keyword research.
Used during client onboarding to build a validated prompt matrix
from real search demand data — no client credentials required.

API docs: https://docs.dataforseo.com/v3/keywords_data/google/
"""
import base64
import requests
from config.settings import DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD


class DataForSEOClient:
    BASE_URL = "https://api.dataforseo.com/v3"

    def __init__(self):
        creds = f"{DATAFORSEO_LOGIN}:{DATAFORSEO_PASSWORD}"
        self.auth = base64.b64encode(creds.encode()).decode()
        self.headers = {
            "Authorization": f"Basic {self.auth}",
            "Content-Type": "application/json",
        }

    def _post(self, endpoint, payload):
        resp = requests.post(
            f"{self.BASE_URL}{endpoint}",
            headers=self.headers,
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def get_keyword_ideas(self, seed_keyword: str, location_code: int = 2840,
                          language_code: str = "en", limit: int = 100) -> list[dict]:
        """
        Get keyword ideas from a seed keyword.
        location_code 2840 = United States
        Returns list of {keyword, search_volume, competition, cpc, intent}
        """
        payload = [{
            "keyword":       seed_keyword,
            "location_code": location_code,
            "language_code": language_code,
            "limit":         limit,
            "include_serp_info": True,
        }]
        data = self._post("/keywords_data/google_ads/keywords_for_keywords/live", payload)

        results = []
        for task in data.get("tasks", []):
            for item in task.get("result", []):
                if not item:
                    continue
                results.append({
                    "keyword":       item.get("keyword", ""),
                    "search_volume": item.get("search_volume", 0),
                    "competition":   item.get("competition", 0),
                    "cpc":           item.get("cpc", 0),
                    "keyword_info":  item,
                })

        # Sort by search volume descending
        results.sort(key=lambda x: x["search_volume"] or 0, reverse=True)
        return results

    def get_competitor_keywords(self, domain: str, limit: int = 100) -> list[dict]:
        """
        Get the top keywords a competitor domain ranks for.
        Use this to find prompts where competitors have organic presence.
        """
        payload = [{
            "target":        domain,
            "location_code": 2840,
            "language_code": "en",
            "limit":         limit,
            "order_by":      ["etv,desc"],
        }]
        data = self._post("/dataforseo_labs/google/ranked_keywords/live", payload)

        results = []
        for task in data.get("tasks", []):
            for item in (task.get("result") or [{}])[0].get("items", []):
                kw = item.get("keyword_data", {}).get("keyword", "")
                vol = item.get("keyword_data", {}).get("keyword_info", {}).get("search_volume", 0)
                results.append({"keyword": kw, "search_volume": vol})

        results.sort(key=lambda x: x["search_volume"] or 0, reverse=True)
        return results


def keywords_to_prompts(keywords: list[dict], brand_name: str = None,
                        max_prompts: int = 50) -> list[dict]:
    """
    Convert a keyword list into GEO-ready prompt candidates.
    Applies transformations to make keywords conversational.
    Returns list of {prompt_text, source_keyword, volume, intent}
    """
    prompts = []

    for kw_data in keywords[:max_prompts * 2]:
        kw = kw_data.get("keyword", "").strip().lower()
        vol = kw_data.get("search_volume", 0) or 0

        if not kw or len(kw) < 8:
            continue

        # Determine intent from keyword structure
        intent = "solution-aware"
        if any(w in kw for w in ["best", "top", "leading", "recommended"]):
            intent = "solution-aware"
        elif any(w in kw for w in ["vs", "versus", "alternative", "compare", "review"]):
            intent = "comparison"
        elif any(w in kw for w in ["how to", "how do", "where can", "what is"]):
            intent = "problem-aware"
        elif brand_name and brand_name.lower() in kw:
            intent = "vendor-aware"

        # Convert keyword to natural prompt variants
        prompt_variants = _keyword_to_prompts(kw, intent)
        for pv in prompt_variants:
            prompts.append({
                "prompt_text":    pv,
                "source_keyword": kw,
                "search_volume":  vol,
                "intent":         intent,
                "source":         "dataforseo",
            })

    # Deduplicate
    seen = set()
    unique = []
    for p in prompts:
        if p["prompt_text"] not in seen:
            seen.add(p["prompt_text"])
            unique.append(p)

    return unique[:max_prompts]


def _keyword_to_prompts(keyword: str, intent: str) -> list[str]:
    """Convert a keyword into 1-2 natural prompt variants."""
    kw = keyword.strip()
    prompts = []

    if intent == "problem-aware":
        prompts.append(kw.capitalize() + "?")
        if not kw.startswith(("how", "where", "what", "why")):
            prompts.append(f"How do I find {kw}?")
    elif intent == "solution-aware":
        prompts.append(f"What is the {kw}?" if kw.startswith("best") else f"Best {kw}?")
        prompts.append(f"I need a {kw} — what do you recommend?")
    elif intent == "comparison":
        prompts.append(kw.capitalize() + "?")
    elif intent == "vendor-aware":
        prompts.append(kw.capitalize() + "?")
    else:
        prompts.append(kw.capitalize() + "?")

    return prompts[:2]
