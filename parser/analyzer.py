"""
parser/analyzer.py
Parses a raw LLM response and extracts:
  - brand_mentioned (bool)
  - mention_count (int)
  - mention_rate (float)
  - passes_run (int)
  - passes_mentioned (int)
  - position (first | second | passing | none)
  - sentiment (positive | neutral | negative)
  - competitor_mentioned (bool)
  - citations (list of domain dicts)

Design note: all detection is case-insensitive string matching.
No ML models — fast, deterministic, easy to debug.
"""
import re
import tldextract


# ── Helpers ───────────────────────────────────────────────────────────────────

_CORROBORATION_TERMS = [
    "print", "blueprint", "wide format", "large format", "shipping",
    "azulprints", "azulprints.com", "architectural", "construction",
    "drawing", "plan", "cad", "technical",
]
_SHORT_ALIAS_THRESHOLD = 10


def _requires_corroboration(alias: str) -> bool:
    return len(alias) < _SHORT_ALIAS_THRESHOLD


def _has_corroboration(response_lower: str, match_start: int, match_end: int) -> bool:
    window_start = max(0, match_start - 300)
    window_end   = min(len(response_lower), match_end + 300)
    window = response_lower[window_start:window_end]
    return any(term in window for term in _CORROBORATION_TERMS)


# ── Brand mention detection ───────────────────────────────────────────────────

def detect_brand(response: str, aliases: list[str]) -> tuple[bool, int]:
    """
    Returns (mentioned: bool, count: int).
    Short aliases require corroborating context to avoid false positives.
    """
    response_lower = response.lower()
    total = 0
    sorted_aliases = sorted(aliases, key=len, reverse=True)

    for alias in sorted_aliases:
        pattern = re.compile(r'\b' + re.escape(alias.lower()) + r'\b')
        for match in pattern.finditer(response_lower):
            if _requires_corroboration(alias):
                if not _has_corroboration(response_lower, match.start(), match.end()):
                    continue
            total += 1

    return (total > 0, total)


# ── Position detection ────────────────────────────────────────────────────────

def detect_position(response: str, aliases: list[str]) -> str:
    response_lower = response.lower()
    length = len(response_lower)
    if length == 0:
        return "none"

    first_mention_pos = None
    sorted_aliases = sorted(aliases, key=len, reverse=True)

    for alias in sorted_aliases:
        pattern = re.compile(r'\b' + re.escape(alias.lower()) + r'\b')
        for match in pattern.finditer(response_lower):
            if _requires_corroboration(alias):
                if not _has_corroboration(response_lower, match.start(), match.end()):
                    continue
            if first_mention_pos is None or match.start() < first_mention_pos:
                first_mention_pos = match.start()
            break

    if first_mention_pos is None:
        return "none"

    relative_pos = first_mention_pos / length
    if relative_pos < 0.33:
        return "first"
    elif relative_pos < 0.66:
        return "second"
    else:
        return "passing"


# ── Sentiment detection ───────────────────────────────────────────────────────

POSITIVE_SIGNALS = [
    "recommend", "best", "top", "excellent", "great", "reliable",
    "fast", "trusted", "quality", "leading", "popular", "easy",
    "affordable", "professional", "highly rated", "well-reviewed",
]

NEGATIVE_SIGNALS = [
    "avoid", "poor", "slow", "unreliable", "expensive", "complaint",
    "problem", "issue", "bad", "disappointing", "negative review",
    "not recommended", "caution",
]

def detect_sentiment(response: str, aliases: list[str]) -> str:
    response_lower = response.lower()
    pos_score = 0
    neg_score = 0
    sorted_aliases = sorted(aliases, key=len, reverse=True)

    for alias in sorted_aliases:
        pattern = re.compile(r'\b' + re.escape(alias.lower()) + r'\b')
        for match in pattern.finditer(response_lower):
            if _requires_corroboration(alias):
                if not _has_corroboration(response_lower, match.start(), match.end()):
                    continue

            start  = max(0, match.start() - 150)
            end    = min(len(response_lower), match.end() + 150)
            window = response_lower[start:end]

            for signal in POSITIVE_SIGNALS:
                if signal in window:
                    pos_score += 1
            for signal in NEGATIVE_SIGNALS:
                if signal in window:
                    neg_score += 1

    if pos_score > neg_score:
        return "positive"
    elif neg_score > pos_score:
        return "negative"
    else:
        return "neutral"


# ── Competitor detection ──────────────────────────────────────────────────────

def detect_competitors(response: str, competitors: list[str]) -> bool:
    response_lower = response.lower()
    for comp in competitors:
        comp_lower = comp.lower()
        pattern = re.compile(r'\b' + re.escape(comp_lower) + r'\b')
        if pattern.search(response_lower):
            return True
        if '.' in comp_lower:
            plain = comp_lower.replace('www.', '')
            if plain in response_lower:
                return True
    return False


# ── Citation extraction ───────────────────────────────────────────────────────

# Common TLDs for bare domain detection
_COMMON_TLDS = r'(?:com|net|org|io|co|us|ai|app|gov|edu)'

def extract_citations(
    response: str,
    client_domain: str,
    competitors: list[str],
) -> list[dict]:
    """
    Extracts URLs and domains from the response.
    Handles:
      - Markdown links: [text](url)
      - Perplexity [CITATIONS] block
      - Bare https:// URLs
      - Bare domain mentions without protocol (e.g. fedex.com, plans4less.com)

    Returns list of {domain, url, is_client_domain, is_competitor}
    """
    urls = []

    # Markdown links
    urls += re.findall(r'\[.*?\]\((https?://[^\)]+)\)', response)

    # Perplexity citation block
    citation_block_match = re.search(r'\[CITATIONS\]\n(.*)', response, re.DOTALL)
    if citation_block_match:
        block = citation_block_match.group(1)
        urls += re.findall(r'https?://\S+', block)

    # Bare https:// URLs in text
    urls += re.findall(r'(?<!\()(https?://[^\s\)]+)', response)

    # Bare domain mentions without protocol (e.g. "visit fedex.com")
    bare_domains = re.findall(
        r'\b([a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.(?:[a-zA-Z0-9\-]+\.)?' +
        _COMMON_TLDS + r')\b',
        response
    )
    for bd in bare_domains:
        bd_lower = bd.lower()
        # Skip if already captured as part of a full URL
        already_captured = any(bd_lower in u.lower() for u in urls)
        if not already_captured:
            urls.append('https://' + bd_lower)

    # Deduplicate preserving order
    urls = list(dict.fromkeys(urls))

    # Build competitor registered-domain set for exact matching
    comp_domains = set()
    for comp in competitors:
        comp_lower = comp.lower()
        if '.' in comp_lower:
            extracted = tldextract.extract(comp_lower)
            rd = extracted.registered_domain
            if rd:
                comp_domains.add(rd.lower())
        else:
            # Name-style — strip spaces/hyphens for loose matching
            comp_domains.add(comp_lower.replace(' ', '').replace('-', ''))

    client_root = tldextract.extract(client_domain).registered_domain.lower()

    citations = []
    seen_domains = set()

    for url in urls:
        try:
            extracted = tldextract.extract(url)
            domain = extracted.registered_domain.lower()
            if not domain or domain in seen_domains:
                continue
            seen_domains.add(domain)

            is_client = (domain == client_root)

            # Exact registered-domain match for known competitor domains
            # plus loose name match for name-style entries
            is_comp = domain in comp_domains or any(
                c in domain for c in comp_domains if '.' not in c
            )

            citations.append({
                "domain":           domain,
                "url":              url,
                "is_client_domain": is_client,
                "is_competitor":    is_comp,
            })
        except Exception:
            continue

    return citations


# ── Main entry point ──────────────────────────────────────────────────────────

def analyze(
    response: str,
    brand_aliases: list[str],
    client_domain: str,
    competitors: list[str],
    passes_run: int = 1,
) -> dict:
    """
    Full analysis of a single LLM response.
    Returns a dict ready to be passed to db.insert_result + db.insert_citations.
    """
    mentioned, count = detect_brand(response, brand_aliases)
    passes_mentioned = 1 if mentioned else 0
    mention_rate     = round(passes_mentioned / passes_run, 4) if passes_run > 0 else 0.0

    return {
        "brand_mentioned":      mentioned,
        "mention_count":        count,
        "passes_run":           passes_run,
        "passes_mentioned":     passes_mentioned,
        "mention_rate":         mention_rate,
        "position":             detect_position(response, brand_aliases),
        "sentiment":            detect_sentiment(response, brand_aliases) if mentioned else "neutral",
        "competitor_mentioned": detect_competitors(response, competitors),
        "citations":            extract_citations(response, client_domain, competitors),
    }
