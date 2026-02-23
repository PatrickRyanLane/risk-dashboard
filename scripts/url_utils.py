import hashlib
import re
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "gclid", "fbclid", "igshid", "mc_cid", "mc_eid", "vero_id",
    "gaa_at", "gaa_n", "gaa_ts", "gaa_sig",
}


def normalize_url(url: str) -> str:
    if not url:
        return ""
    url = url.strip()
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
