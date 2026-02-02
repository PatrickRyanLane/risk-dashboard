import re
from urllib.parse import urlparse

ALWAYS_CONTROLLED_DOMAINS = {
    "facebook.com",
    "instagram.com",
    "play.google.com",
    "apps.apple.com",
}
CEO_UNCONTROLLED_DOMAINS = {
    "wikipedia.org",
    "youtube.com",
    "youtu.be",
    "tiktok.com",
}
CEO_CONTROLLED_PATH_KEYWORDS = {
    "/leadership/",
    "/about/",
    "/governance/",
    "/team/",
    "/investors/",
    "/board-of-directors",
    "/members/",
    "/member/",
}

FINANCE_TERMS = [
    r"\bearnings\b", r"\beps\b", r"\brevenue\b", r"\bguidance\b", r"\bforecast\b",
    r"\bprice target\b", r"\bupgrade\b", r"\bdowngrade\b", r"\bdividend\b",
    r"\bbuyback\b", r"\bshares?\b", r"\bstock\b", r"\bmarket cap\b",
    r"\bquarterly\b", r"\bfiscal\b", r"\bprofit\b", r"\bEBITDA\b",
    r"\b10-q\b", r"\b10-k\b", r"\bsec\b", r"\bipo\b"
]
FINANCE_TERMS_RE = re.compile("|".join(FINANCE_TERMS), flags=re.IGNORECASE)
FINANCE_SOURCES = {
    "yahoo.com", "marketwatch.com", "fool.com", "benzinga.com",
    "seekingalpha.com", "thefly.com", "barrons.com", "wsj.com",
    "investorplace.com", "nasdaq.com", "foolcdn.com"
}
TICKER_RE = re.compile(r"\b(?:NYSE|NASDAQ|AMEX):\s?[A-Z]{1,5}\b")


def hostname(url: str) -> str:
    try:
        host = (urlparse(url or "").hostname or "").lower()
        return host.replace("www.", "")
    except Exception:
        return ""


def _norm_token(s: str) -> str:
    return "".join(ch for ch in (s or "").lower() if ch.isalnum())


def _company_handle_tokens(company: str) -> set[str]:
    words = [w for w in re.split(r"\W+", company or "") if w]
    tokens = set()
    full = _norm_token(company)
    if full:
        tokens.add(full)
    if len(words) >= 2:
        tokens.add(_norm_token("".join(words[:2])))
    elif words:
        tokens.add(_norm_token(words[0]))
    return {t for t in tokens if len(t) >= 4}


def _person_handle_tokens(name: str) -> set[str]:
    words = [w for w in re.split(r"\W+", name or "") if w]
    tokens = set()
    full = _norm_token(name)
    if full:
        tokens.add(full)
    if len(words) >= 2:
        tokens.add(_norm_token("".join(words[:2])))
        tokens.add(_norm_token("".join(words[-2:])))
    if words:
        tokens.add(_norm_token(words[0]))
        tokens.add(_norm_token(words[-1]))
    return {t for t in tokens if len(t) >= 3}


def _is_brand_youtube_channel(company: str, url: str) -> bool:
    if not url or not company:
        return False
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower().replace("www.", "")
    if host not in {"youtube.com", "m.youtube.com"}:
        return False
    brand_token = _norm_token(company)
    if not brand_token:
        return False
    path = (parsed.path or "").strip("/")
    if not path:
        return False
    if path.lower().startswith("user/"):
        slug = path[5:]
    elif path.startswith("@"):
        slug = path[1:]
    else:
        slug = path.split("/", 1)[0]
    if not slug:
        return False
    slug_token = _norm_token(slug)
    return bool(slug_token) and brand_token in slug_token


def _is_linkedin_company_page(company: str, url: str) -> bool:
    if not url or not company:
        return False
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower().replace("www.", "")
    if host != "linkedin.com":
        return False
    path = (parsed.path or "").strip("/")
    if not path.lower().startswith("company/"):
        return False
    slug = path.split("/", 1)[1] if "/" in path else ""
    slug = slug.split("/", 1)[0] if slug else ""
    if not slug:
        return False
    brand_token = _norm_token(company)
    slug_token = _norm_token(slug)
    if brand_token and brand_token in slug_token:
        return True
    return _linkedin_slug_matches_company(company, slug)


def _linkedin_slug_matches_company(company: str, slug: str) -> bool:
    if not company or not slug:
        return False
    ignore = {
        "inc", "incorporated", "corporation", "corp", "company", "co",
        "llc", "ltd", "limited", "plc", "group", "holdings", "holding",
        "the", "and", "of", "services",
    }
    company_tokens = [
        _norm_token(t) for t in re.split(r"\W+", company.lower()) if t
    ]
    company_tokens = [
        t for t in company_tokens if t and t not in ignore and len(t) >= 4
    ]
    slug_tokens = [
        _norm_token(t) for t in re.split(r"[\W_]+", slug.lower()) if t
    ]
    slug_tokens = [t for t in slug_tokens if t and len(t) >= 3]
    if not company_tokens or not slug_tokens:
        return False
    return any(ct in st or st in ct for ct in company_tokens for st in slug_tokens)


def _is_linkedin_person_profile(name: str, url: str) -> bool:
    if not url or not name:
        return False
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower().replace("www.", "")
    if host != "linkedin.com":
        return False
    path = (parsed.path or "").strip("/")
    if not (path.lower().startswith("in/") or path.lower().startswith("pub/")):
        return False
    slug = path.split("/", 1)[1] if "/" in path else ""
    slug = slug.split("/", 1)[0] if slug else ""
    if not slug:
        return False
    slug_token = _norm_token(slug)
    if not slug_token:
        return False
    for token in _person_handle_tokens(name):
        if token and token in slug_token:
            return True
    return False


def _is_x_company_handle(company: str, url: str) -> bool:
    if not url or not company:
        return False
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower().replace("www.", "")
    if host not in {"x.com", "twitter.com"}:
        return False
    path = (parsed.path or "").strip("/")
    handle = path.split("/", 1)[0] if path else ""
    if not handle:
        return False
    handle_token = _norm_token(handle)
    if not handle_token:
        return False
    for token in _company_handle_tokens(company):
        if token and token in handle_token:
            return True
    return False


def _is_x_person_handle(name: str, url: str) -> bool:
    if not url or not name:
        return False
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower().replace("www.", "")
    if host not in {"x.com", "twitter.com"}:
        return False
    path = (parsed.path or "").strip("/")
    handle = path.split("/", 1)[0] if path else ""
    if not handle:
        return False
    handle_token = _norm_token(handle)
    if not handle_token:
        return False
    for token in _person_handle_tokens(name):
        if token and token in handle_token:
            return True
    return False


def parse_company_domains(websites: str) -> set[str]:
    if not websites:
        return set()
    domains = set()
    for url in websites.split("|"):
        url = (url or "").strip()
        if not url:
            continue
        if not url.startswith(("http://", "https://")):
            url = f"http://{url}"
        host = hostname(url)
        if host and "." in host:
            domains.add(host)
    return domains


def classify_control(
    company: str,
    url: str,
    company_domains: dict[str, set[str]],
    *,
    entity_type: str = "company",
    person_name: str | None = None,
) -> bool:
    host = hostname(url)
    if not host:
        return False
    try:
        path = (urlparse(url).path or "").lower()
    except Exception:
        path = ""
    if entity_type == "ceo":
        for bad in CEO_UNCONTROLLED_DOMAINS:
            if host == bad or host.endswith("." + bad):
                return False
        if person_name and _is_linkedin_person_profile(person_name, url):
            return True
        if person_name and _is_x_person_handle(person_name, url):
            return True
    if host == "facebook.com":
        if any(seg in path for seg in ("/posts/", "/photos/", "/videos/")):
            return False
        return True
    if host == "instagram.com":
        if any(seg in path for seg in ("/p/", "/reels/")):
            return False
        return True
    if host == "threads.net":
        if "/posts/" in path:
            return False
        return True
    if _is_brand_youtube_channel(company, url):
        return True
    if _is_linkedin_company_page(company, url):
        return True
    if "/status/" in path and host in {"x.com", "twitter.com"}:
        return False
    if _is_x_company_handle(company, url):
        return True
    for good in ALWAYS_CONTROLLED_DOMAINS:
        if host == good or host.endswith("." + good):
            return True
    matched_company_domain = False
    for rd in company_domains.get(company, set()):
        if host == rd or host.endswith("." + rd):
            matched_company_domain = True
            break
    if matched_company_domain:
        return True
    brand_token = _norm_token(company)
    parts = [_norm_token(part) for part in host.split(".") if part]
    if brand_token and brand_token in parts[:-1]:
        return True
    if entity_type == "ceo" and any(k in path for k in CEO_CONTROLLED_PATH_KEYWORDS):
        return matched_company_domain or (brand_token and brand_token in parts[:-1])
    return False


def is_financial_routine(title: str, snippet: str = "", url: str = "", source: str = "") -> bool:
    hay = f"{title} {snippet} {source}".strip()
    if FINANCE_TERMS_RE.search(hay):
        return True
    if TICKER_RE.search(title or ""):
        return True
    host = hostname(url)
    if host and any(host == d or host.endswith("." + d) for d in FINANCE_SOURCES):
        return True
    return False
