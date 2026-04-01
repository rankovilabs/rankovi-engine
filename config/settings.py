"""
config/settings.py
Loads environment variables. In Cloud Run these come from Secret Manager.
Locally they come from a .env file.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Database
DB_HOST     = os.environ.get("DB_HOST", "localhost")
DB_PORT     = int(os.environ.get("DB_PORT", 5432))
DB_NAME     = os.environ.get("DB_NAME", "rankovi")
DB_USER     = os.environ.get("DB_USER", "rankovi")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")

# ── LLM API Keys
OPENAI_API_KEY      = os.environ.get("OPENAI_API_KEY", "")
ANTHROPIC_API_KEY   = os.environ.get("ANTHROPIC_API_KEY", "")
GEMINI_API_KEY      = os.environ.get("GEMINI_API_KEY", "")
PERPLEXITY_API_KEY  = os.environ.get("PERPLEXITY_API_KEY", "")

# ── Research API Keys
DATAFORSEO_LOGIN    = os.environ.get("DATAFORSEO_LOGIN", "")
DATAFORSEO_PASSWORD = os.environ.get("DATAFORSEO_PASSWORD", "")

# ── Engine config
ENGINE_CONFIG = {
    "chatgpt": {
        "api_key_env": OPENAI_API_KEY,
        "model":       "gpt-4o",
        "timeout":     30,
    },
    "claude": {
        "api_key_env": ANTHROPIC_API_KEY,
        "model":       "claude-sonnet-4-6",
        "timeout":     30,
    },
    "gemini": {
        "api_key_env": GEMINI_API_KEY,
        "model":       "gemini-2.5-flash",
        "timeout":     30,
    },
    "perplexity": {
        "api_key_env": PERPLEXITY_API_KEY,
        "model":       "sonar",
        "timeout":     30,
    },
}

# ── Runner behavior
MAX_RETRIES         = 3
RETRY_DELAY_SECONDS = 5
CONCURRENT_ENGINES  = False

# ── Multi-pass configuration
# Number of times each prompt is fired per engine per run
# Results are averaged to produce a statistically stable mention rate
PASSES_PER_PROMPT = int(os.environ.get("PASSES_PER_PROMPT", 3))
