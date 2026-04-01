"""
research/prompt_generator.py
Uses the LLM APIs themselves to generate natural, conversational
prompt variants for a given service/category.
This captures how real users actually talk to AI — 
different register than keyword tools.
"""
import openai
import json
from config.settings import OPENAI_API_KEY


def generate_natural_prompts(service_description: str,
                              audience: str = "general",
                              n_prompts: int = 20) -> list[dict]:
    """
    Ask GPT-4o to generate natural prompts a real user would ask
    about the given service. Returns list of {prompt_text, intent, audience}.
    """
    client = openai.OpenAI(api_key=OPENAI_API_KEY)

    system = """You are a UX researcher studying how real people ask AI assistants about products and services.
Generate natural, conversational prompts that real users would type — not marketing language.
Return ONLY a JSON array of objects with keys: prompt_text, intent, audience.
Intent must be one of: task-based, problem-aware, solution-aware, comparison, vendor-aware.
No preamble, no markdown, just the JSON array."""

    user = f"""Generate {n_prompts} distinct prompts that someone would ask an AI assistant about:
Service: {service_description}
Audience: {audience}

Rules:
- task-based prompts (30%): first-person, immediate need ("I need to...", "I have... what should I...")
- problem-aware (20%): they have a problem but don't know the solution type
- solution-aware (25%): they know what type of service they want, looking for best option
- comparison (15%): evaluating options, mentioning alternatives
- vendor-aware (10%): already know a specific brand name, asking about it

Make them sound like real humans, not keyword phrases.
Include variation in urgency, context, and formality."""

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.8,
        max_tokens=2000,
    )

    raw = response.choices[0].message.content.strip()

    # Strip markdown fences if present
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    try:
        prompts = json.loads(raw)
        # Validate and tag
        validated = []
        for p in prompts:
            if "prompt_text" in p and "intent" in p:
                p["source"] = "ai_generated"
                p["audience"] = p.get("audience", audience)
                validated.append(p)
        return validated
    except json.JSONDecodeError:
        print(f"Warning: Could not parse AI prompt generation response")
        return []


def generate_competitor_prompts(your_brand: str, competitor_brands: list[str],
                                category: str) -> list[dict]:
    """
    Generate comparison and alternative prompts that pit your brand
    against specific competitors.
    """
    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    comp_list = ", ".join(competitor_brands)

    user = f"""Generate 10 natural comparison prompts involving these brands in the {category} space:
Your brand: {your_brand}
Competitors: {comp_list}

Include:
- Direct vs comparisons (e.g. "{your_brand} vs {competitor_brands[0]}")
- "alternatives to [competitor]" prompts
- "best [category] service" prompts where multiple brands would be mentioned
- Review/recommendation prompts

Return JSON array with keys: prompt_text, intent (always 'comparison'), brands_mentioned (array)."""

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": user}],
        temperature=0.7,
        max_tokens=1000,
    )

    raw = response.choices[0].message.content.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]

    try:
        prompts = json.loads(raw.strip())
        for p in prompts:
            p["source"] = "ai_generated_competitor"
            p["intent"] = "comparison"
        return prompts
    except json.JSONDecodeError:
        return []
