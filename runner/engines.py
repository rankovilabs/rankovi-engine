"""
runner/engines.py
One adapter per LLM engine. Each returns a raw string response.
Adding a new engine = adding a new function + entry in ADAPTERS dict.
"""
import time
import requests
import openai
import anthropic


from config.settings import (
    OPENAI_API_KEY,
    ANTHROPIC_API_KEY,
    GEMINI_API_KEY,
    PERPLEXITY_API_KEY,
    MAX_RETRIES,
    RETRY_DELAY_SECONDS,
)


# ── Retry wrapper ─────────────────────────────────────────────────────────────

def with_retry(fn, *args, **kwargs):
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_error = e
            print(f"    [retry {attempt}/{MAX_RETRIES}] {type(e).__name__}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS * attempt)
    raise last_error


# ── Engine adapters ───────────────────────────────────────────────────────────

def query_chatgpt(prompt_text: str) -> str:
    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt_text}],
        temperature=0.3,   # lower temp = more consistent, better for tracking
        max_tokens=1000,
    )
    return response.choices[0].message.content


def query_claude(prompt_text: str) -> str:
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt_text}],
    )
    return response.content[0].text


def query_gemini(prompt_text: str) -> str:
    from google import genai as google_genai
    client = google_genai.Client(api_key=GEMINI_API_KEY)
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt_text,
    )
    return response.text


def query_perplexity(prompt_text: str) -> str:
    """
    Perplexity uses an OpenAI-compatible REST API.
    Using requests directly for simplicity and control.
    """
    headers = {
        "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
        "Content-Type":  "application/json",
    }
    payload = {
        "model":    "sonar",
        "messages": [{"role": "user", "content": prompt_text}],
        "temperature": 0.3,
        "max_tokens":  1000,
    }
    resp = requests.post(
        "https://api.perplexity.ai/chat/completions",
        headers=headers,
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    # Perplexity returns citations separately — extract them alongside the text
    content = data["choices"][0]["message"]["content"]

    # Attach citation URLs as a trailing block so the parser can extract them
    citations = data.get("citations", [])
    if citations:
        citation_block = "\n\n[CITATIONS]\n" + "\n".join(citations)
        content += citation_block

    return content


# ── Dispatch map ─────────────────────────────────────────────────────────────
# Key matches engines.slug in the database

ADAPTERS = {
    "chatgpt":    query_chatgpt,
    "claude":     query_claude,
    "gemini":     query_gemini,
    "perplexity": query_perplexity,
}


def query_engine(engine_slug: str, prompt_text: str) -> str:
    """
    Main entry point. Dispatches to the correct adapter with retry logic.
    Raises if engine_slug is unknown.
    """
    adapter = ADAPTERS.get(engine_slug)
    if not adapter:
        raise ValueError(f"Unknown engine slug: {engine_slug}")
    return with_retry(adapter, prompt_text)
