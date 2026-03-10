import hashlib
import re
from functools import lru_cache
from urllib.request import Request, urlopen
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "gclid", "fbclid", "igshid", "mc_cid", "mc_eid", "vero_id",
    "gaa_at", "gaa_n", "gaa_ts", "gaa_sig",
}

REDIRECT_PARAM_CANDIDATES = (
    "url", "q", "u", "dest", "destination", "redirect", "target", "continue",
)
GOOGLE_REDIRECT_HOSTS = {
    "google.com",
    "www.google.com",
    "news.google.com",
}
GOOGLE_REDIRECT_PATHS = {
    "/url",
    "/goto",
}


def _looks_like_http_url(value: str) -> bool:
    text = str(value or "").strip()
    return text.startswith("http://") or text.startswith("https://")


def _extract_redirect_target(url: str) -> str:
    try:
        parsed = urlparse(url or "")
    except Exception:
        return ""
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    for key in REDIRECT_PARAM_CANDIDATES:
        candidate = str(query.get(key) or "").strip()
        if _looks_like_http_url(candidate):
            return candidate
    return ""


@lru_cache(maxsize=4096)
def _resolve_google_redirect(url: str) -> str:
    direct = _extract_redirect_target(url)
    if direct:
        return direct
    try:
        parsed = urlparse(url or "")
        host = (parsed.hostname or "").lower()
        path = parsed.path or ""
    except Exception:
        return url
    if host not in GOOGLE_REDIRECT_HOSTS or path not in GOOGLE_REDIRECT_PATHS:
        return url
    try:
        req = Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; RiskDashboardBot/1.0)"},
        )
        with urlopen(req, timeout=5) as resp:
            final_url = str(resp.geturl() or "").strip()
        if _looks_like_http_url(final_url):
            return final_url
    except Exception:
        return url
    return url


def resolve_url(url: str) -> str:
    if not url:
        return ""
    text = str(url).strip()
    if not text:
        return ""
    direct = _extract_redirect_target(text)
    if direct:
        return direct
    try:
        parsed = urlparse(text)
        host = (parsed.hostname or "").lower()
        path = parsed.path or ""
    except Exception:
        return text
    if host in GOOGLE_REDIRECT_HOSTS and path in GOOGLE_REDIRECT_PATHS:
        return _resolve_google_redirect(text)
    return text


def normalize_url(url: str) -> str:
    if not url:
        return ""
    url = resolve_url(url).strip()
    if not url:
        return ""

    parsed = urlparse(url)

    scheme = (parsed.scheme or "http").lower()
    netloc = (parsed.netloc or "").lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]

    path = re.sub(r"//+", "/", parsed.path or "")

    # Normalize query by removing tracking params and sorting
    query_pairs = []
    for k, v in parse_qsl(parsed.query, keep_blank_values=True):
        if k in TRACKING_PARAMS or k.startswith("utm_") or k.startswith("gaa_"):
            continue
        query_pairs.append((k, v))
    query_pairs.sort()
    query = urlencode(query_pairs, doseq=True)

    fragment = ""  # drop fragments

    normalized = urlunparse((scheme, netloc, path, "", query, fragment))
    return normalized


def url_hash(url: str) -> str:
    normalized = normalize_url(url)
    if not normalized:
        return ""
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
