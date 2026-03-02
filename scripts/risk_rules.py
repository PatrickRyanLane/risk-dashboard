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
    "investorplace.com", "nasdaq.com", "foolcdn.com",
    "primaryignition.com", "tradingview.com", "marketscreener.com",
    "gurufocus.com"
}
# Broad source-level override; remove if this forces too many unrelated negatives.
FORCE_NEGATIVE_SOURCE_DOMAINS = {
    "bankinfosecurity.com",
}
TICKER_RE = re.compile(r"\b(?:NYSE|NASDAQ|AMEX):\s?[A-Z]{1,5}\b")
MATERIAL_RISK_TERMS = [
    r"\blawsuits?\b", r"\blegal action\b", r"\bclass action\b", r"\bsu(?:e|es|ed|ing)\b",
    r"\bsettle(?:ment|d|s)?\b", r"\bprobe\b", r"\binvestigat(?:e|es|ed|ion|ions)\b",
    r"\bsubpoena(?:s)?\b", r"\bsec (?:probe|investigation|charge|charges)\b", r"\bdoj\b",
    r"\bcharge(?:d|s)?\b", r"\bindict(?:ed|ment)?\b", r"\bfraud\b", r"\bscandal\b",
    r"\bbankrupt(?:cy|cies)?\b", r"\blayoffs?\b", r"\brecall(?:s|ed)?\b", r"\bdata breach(?:es)?\b",
    r"\bcyber(?:attack|attacks|breach|breaches)\b", r"\bwhistleblower(?:s)?\b",
    r"\bmisconduct\b", r"\bboycott(?:s|ed)?\b",
]
MATERIAL_RISK_TERMS_RE = re.compile("|".join(MATERIAL_RISK_TERMS), flags=re.IGNORECASE)

NAME_IGNORE_TOKENS = {
    "inc", "incorporated", "corporation", "corp", "company", "co",
    "llc", "ltd", "limited", "plc", "group", "holdings", "holding",
    "the", "and", "of", "services",
}
PUBLISHER_SUFFIX_TOKENS = {
    "news", "newsroom", "media", "press", "wire", "blog", "official"
}
SOCIAL_BYLINE_PLATFORM_PATTERNS = {
    "facebook.com": ("facebook",),
    "instagram.com": ("instagram",),
    "threads.net": ("threads",),
    "youtube.com": ("youtube", "youtu"),
    "youtu.be": ("youtube", "youtu"),
    "x.com": ("x", "twitter"),
    "twitter.com": ("twitter", "x"),
}

BRAND_NEUTRALIZE_TITLE_TERMS = [
    r"\bgrand\b",
    r"\bdiamond\b",
    r"\bsell\b",
    r"\bsells?\s+\d[\d,]*\s+shares?\b",
    r"\bsells?\s+shares?\b",
    r"\blow\b",
    r"\bdream\b",
    r"\bdarling\b",
    r"\bwells\b",
    r"\bbest\s+buy\b",
    r"\bkilled\b",
    r"\bmlm\b",
    r"\bmad\s+money\b",
    r"\brate\s+cut\b",
    r"\brates?\s+(drop|drops|dropped|fall|falls|fell)\b",
    r"\b(drop|drops|dropped|fall|falls|fell)\s+in\s+rates?\b",
    r"\bone\s+stop\s+shop\b",
    r"\bfuneral\b",
    r"\bcremation\b",
    r"\bcemetery\b",
    r"\blimited\b",
    r"\bno\s+organic\b",
    r"\brob\b",
    r"\blower\b",
    r"\benergy\b",
    r"\brebel\b",
    r"\bpay\b",
    r"\bcompensation\b",
    r"\bpopular\s+comment(s)?\b",
    r"\bshare(s|d|ing)?\b",
    r"\bcancer\s+society\b",
    r"\bamerican\s+cancer\s+society\b",
    r"\bthe\s+block\b",
    r"\bblock\s+by\s+jack\s+dorsey\b",
]
BRAND_NEUTRALIZE_TITLE_RE = re.compile("|".join(BRAND_NEUTRALIZE_TITLE_TERMS), flags=re.IGNORECASE)

BRAND_LEGAL_TROUBLE_TERMS = [
    r"\blawsuit(s)?\b", r"\bsued\b", r"\bsuing\b", r"\blegal\b",
    # Broad legal-context terms: keep for now; remove/narrow if false positives increase.
    r"\bsettlement(s)?\b", r"\bsettl(?:e|es|ed|ing)\b", r"\bfine(d)?\b", r"\bfine(?:d|s|ing)?\b", r"\bclass[- ]action\b",
    r"\bftc\b", r"\bsec\b", r"\bdoj\b", r"\bcfpb\b",
    r"\battorney\s+general\b",
    r"\bantitrust\b", r"\bban(s|ned)?\b",
    r"\btone[- ]deaf\b",
    r"\binvestigat(?:e|es|ed|ing|ion|ions)\b",
    r"\bdata leaks?\b", r"\bdata breach(es)?\b", r"\bsecurity breach(es)?\b", r"\bbreach(es)?\b",
    # Broad cyber terms: keep for recall; remove/narrow if they over-force negatives.
    r"\bhack(?:ed|s|ing)?\b", r"\bcyber[- ]?attack(?:s)?\b", r"\bexpos(?:e|es|ed|ing)\b",
    r"\bleak(?:ed|s|ing)?\b",
    r"\brecall(s|ed)?\b",
    r"\blayoff(s)?\b",
    r"\bboycott(?:ing|ed|s)?\b",
    r"\bexit(s|ed|ing)?\b", r"\bleav(e|es|ing|ers|ed)\b", r"\bdepart(s|ed|ing)?\b",
    r"\boust(er|ed|ing|s)?\b", r"\bstep\s+down\b", r"\bsteps\s+down\b",
    r"\bprobe(s|d)?\b", r"\binvestigation(s)?\b",
    r"\bcomplaint(s)?\b", r"\bunlawfully\b", r"\bdisclos(ed|e|ing)?\b",
    r"\btrial(s)?\b", r"\bguilty\b", r"\bconvicted\b",
    r"\bsanction(s|ed)?\b", r"\bpenalt(y|ies)\b",
    r"\bfraud\b", r"\bembezzl(e|ement)\b", r"\baccused\b", r"\bcommitted\b",
    r"\bdivorce\b", r"\bbankrupt(cy|cies)\b", r"\bapologizes\b", r"\bapology\b",
    r"\bepstein\b", r"\bghislaine\b", r"\bmaxwell\b",
    r"\bheadwinds\b", r"\bcontroversy\b", r"\bfallout\b",
    r"\bcancel(s|ed|ing|led|ling)?\b",
    r"\bresign(s|ed|ing|ation)?\b", r"\bquit(s|ting|ted)?\b",
    r"\bpressure\b", r"\bblast\b", r"\bno[- ]confidence\b",
    # Broad terms intentionally enabled for recall; remove or narrow if they over-force negatives.
    r"\btoxic\b", r"\bden(?:ied|ies|y)\b", r"\bdenounc(?:e|es|ed|ing)\b",
    r"\bcrisis\b", r"\bcrises\b", r"\barbitration\b",
]
BRAND_LEGAL_TROUBLE_RE = re.compile("|".join(BRAND_LEGAL_TROUBLE_TERMS), flags=re.IGNORECASE)

CEO_NEUTRALIZE_TITLE_TERMS = [
    r"\bflees\b",
    r"\bsavage\b",
    r"\brob\b",
    r"\bsells?\s+\d[\d,]*\s+shares?\b",
    r"\bsells?\s+shares?\b",
    r"\bnicholas\s+lower\b",
    r"\bmad\s+money\b",
    r"\brates?\s+(drop|drops|dropped|fall|falls|fell)\b",
    r"\b(drop|drops|dropped|fall|falls|fell)\s+in\s+rates?\b",
    r"\bno\s+organic\b",
    r"\brob\b",
    r"\blower\b",
    r"\benergy\b",
    r"\brebel\b",
    r"\bpay\b",
    r"\bcompensation\b",
    r"\bnet\s+worth\b",
    r"\bpopular\s+comment(s)?\b",
    r"\bshare(s|d|ing)?\b",
    r"\bcancer\s+society\b",
    r"\bamerican\s+cancer\s+society\b",
    r"\bthe\s+block\b",
    r"\bblock\s+by\s+jack\s+dorsey\b",
]
CEO_NEUTRALIZE_TITLE_RE = re.compile("|".join(CEO_NEUTRALIZE_TITLE_TERMS), flags=re.IGNORECASE)

CEO_ALWAYS_NEGATIVE_TERMS = [
    r"\bmandate\b",
    r"\bexit(s|ed|ing)?\b", r"\bleav(e|es|ing|ers|ed)\b", r"\bdepart(s|ed|ing)?\b",
    r"\boust(er|ed|ing|s)?\b", r"\bstep\s+down\b", r"\bsteps\s+down\b", r"\bremoved\b",
    r"\bstill\b", r"\bturnaround\b",
    r"\bface\b", r"\bcontroversy\b", r"\baccused\b", r"\bcommitted\b",
    r"\bapologizes\b", r"\bapology\b", r"\baware\b", r"\bepstein\b",
    r"\bloss\b", r"\bdivorce\b", r"\bbankrupt(cy|cies)\b",
    r"\bdata leaks?\b", r"\bdata breach(es)?\b", r"\bsecurity breach(es)?\b", r"\bbreach(es)?\b",
    r"\bunion\s+buster\b",
    r"\bfired\b", r"\bfiring\b", r"\bfires\b",
    r"(?<!t)\bax(e|ed|es)?\b", r"\bsack(ed|s)?\b", r"\boust(ed)?\b",
    r"\bplummeting\b",
    r"\bprobe(s|d)?\b", r"\binvestigation(s)?\b",
    r"\bcomplaint(s)?\b", r"\bunlawfully\b", r"\bdisclos(ed|e|ing)?\b",
    r"\btrial(s)?\b", r"\bguilty\b", r"\bconvicted\b",
    r"\bghislaine\b", r"\bmaxwell\b", r"\bfallout\b",
    r"\bcancel(s|ed|ing|led|ling)?\b",
    r"\bresign(s|ed|ing|ation)?\b", r"\bquit(s|ting|ted)?\b",
    r"\bpressure\b", r"\bblast\b", r"\bno[- ]confidence\b",
]
CEO_ALWAYS_NEGATIVE_RE = re.compile("|".join(CEO_ALWAYS_NEGATIVE_TERMS), flags=re.IGNORECASE)

NARRATIVE_RULE_VERSION = "v1"
NARRATIVE_CRISIS_TAGS = [
    "Workforce Reductions",
    "Accidents & Disasters",
    "Data Breaches",
    "Legal & Regulatory",
    "Unforced Errors (marketing, executive actions/comments)",
    "Labor Disputes",
    "CEO Departures (firings, resignations)",
    "Fraud",
    "Other",
]
NARRATIVE_NON_CRISIS_TAGS = [
    "Rebranding",
    "Mergers and acquisitions",
    "Planned Executive Turnover",
]

NARRATIVE_REBRANDING_RE = re.compile(
    r"\b(rebrand(?:ing|ed|s)?|brand refresh|new logo|renam(?:e|ed|ing)|new brand identity|brand overhaul)\b",
    flags=re.IGNORECASE,
)
NARRATIVE_MNA_RE = re.compile(
    r"\b(merger(?:s)?|acquisition(?:s)?|acquire(?:d|s|ing)?|buyout|takeover|merge(?:s|d|r|ing)?|spinoff|spin-off)\b",
    flags=re.IGNORECASE,
)
NARRATIVE_PLANNED_EXEC_RE = re.compile(
    r"\b(retire(?:s|d|ment|ing)?|succession plan(?:ning)?|planned succession|planned transition|"
    r"step(?:ping)? down|to step down|will step down|named successor|successor)\b",
    flags=re.IGNORECASE,
)
NARRATIVE_PLANNED_EXEC_EXCLUDE_RE = re.compile(
    r"\b(fired|firing|ousted|forced out|amid|scandal|probe|investigat(?:e|es|ed|ing|ion)|"
    r"lawsuit|indict(?:ed|ment)?|charged|fraud|misconduct)\b",
    flags=re.IGNORECASE,
)
NARRATIVE_WORKFORCE_RE = re.compile(
    r"\b(layoffs?|job cuts?|workforce reduction(?:s)?|workforce cuts?|headcount reduction(?:s)?|"
    r"staff reduction(?:s)?|restructuring plan)\b",
    flags=re.IGNORECASE,
)
NARRATIVE_ACCIDENT_RE = re.compile(
    r"\b(accident(?:s)?|explosion(?:s)?|fire(?:s)?|disaster(?:s)?|fatal(?:ity|ities)|"
    r"injur(?:y|ies)|crash(?:es|ed)?|derailment|collapse(?:d|s)?|plant incident)\b",
    flags=re.IGNORECASE,
)
NARRATIVE_DATA_BREACH_RE = re.compile(
    r"\b(data breach(?:es)?|cyber(?:attack|attacks)|ransomware|hack(?:ed|s|ing)?|"
    r"security breach(?:es)?|data leak(?:s|ed|ing)?|expos(?:e|ed|ure|ing))\b",
    flags=re.IGNORECASE,
)
NARRATIVE_LEGAL_RE = re.compile(
    r"\b(attorney general|lawsuit(?:s)?|legal action|regulator(?:y)?|regulatory|"
    r"investigat(?:e|es|ed|ing|ion)|probe(?:s|d)?|settle(?:ment|s|d|ing)?|fine(?:d|s|ing)?|"
    r"charged|indict(?:ed|ment)?|sec\b|doj\b|ftc\b|cfpb\b)\b",
    flags=re.IGNORECASE,
)
NARRATIVE_UNFORCED_RE = re.compile(
    r"\b(backlash|boycott(?:s|ed|ing)?|tone[- ]deaf|ad campaign|advertising campaign|"
    r"public apology|apolog(?:y|ies|ize|ized|izing)|controversial comment(?:s)?|"
    r"executive comment(?:s)?|social media post)\b",
    flags=re.IGNORECASE,
)
NARRATIVE_LABOR_RE = re.compile(
    r"\b(strike(?:s|d|ing)?|walkout(?:s)?|labor dispute(?:s)?|union dispute(?:s)?|"
    r"picket(?:ing)?|collective bargaining|contract talks?)\b",
    flags=re.IGNORECASE,
)
NARRATIVE_CEO_DEPART_RE = re.compile(
    r"\b(ceo\s+(?:resign(?:s|ed|ing|ation)?|step(?:s|ped)? down|depart(?:s|ed|ure)|"
    r"fired|ouste?d|removed)|chief executive\s+(?:resign(?:s|ed|ing|ation)?|step(?:s|ped)? down|"
    r"fired|ouste?d|removed)|resign(?:s|ed|ing|ation)? as ceo|ouste?d ceo|fired ceo)\b",
    flags=re.IGNORECASE,
)
NARRATIVE_FRAUD_RE = re.compile(
    r"\b(fraud|embezzl(?:e|ed|ing|ement)|briber(?:y|ies)|corruption|ponzi|accounting fraud|"
    r"falsif(?:y|ied|ication)|misappropriation)\b",
    flags=re.IGNORECASE,
)


def strip_neutral_terms_brand(headline: str) -> str:
    if not headline:
        return ""
    cleaned = BRAND_NEUTRALIZE_TITLE_RE.sub(" ", headline)
    return " ".join(cleaned.split())


def should_neutralize_brand_title(title: str) -> bool:
    return bool(BRAND_NEUTRALIZE_TITLE_RE.search(title or ""))


def _is_force_negative_source(url: str = "", source: str = "") -> bool:
    host = hostname(url)
    if host and any(host == d or host.endswith("." + d) for d in FORCE_NEGATIVE_SOURCE_DOMAINS):
        return True
    source_l = (source or "").lower()
    if "bank info security" in source_l or "bankinfosecurity" in source_l:
        return True
    return False


def title_mentions_legal_trouble(title: str, snippet: str = "", url: str = "", source: str = "") -> bool:
    if _is_force_negative_source(url=url, source=source):
        return True
    hay = f"{title} {snippet}".strip()
    return bool(BRAND_LEGAL_TROUBLE_RE.search(hay))


def strip_neutral_terms_ceo(title: str) -> str:
    s = str(title or "")
    s = CEO_NEUTRALIZE_TITLE_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def should_neutralize_ceo_title(title: str) -> bool:
    return bool(CEO_NEUTRALIZE_TITLE_RE.search(str(title or "")))


def should_force_negative_ceo(title: str, snippet: str = "", url: str = "", source: str = "") -> bool:
    if title_mentions_legal_trouble(title, snippet, url=url, source=source):
        return True
    hay = f"{title} {snippet}".strip()
    return bool(CEO_ALWAYS_NEGATIVE_RE.search(hay))


def hostname(url: str) -> str:
    try:
        host = (urlparse(url or "").hostname or "").lower()
        return host.replace("www.", "")
    except Exception:
        return ""


def _norm_token(s: str) -> str:
    return "".join(ch for ch in (s or "").lower() if ch.isalnum())


def _name_tokens(value: str, *, min_len: int = 4) -> list[str]:
    tokens = []
    for raw in re.split(r"[\W_]+", value or ""):
        token = _norm_token(raw)
        if not token:
            continue
        if token in NAME_IGNORE_TOKENS:
            continue
        if len(token) < min_len:
            continue
        tokens.append(token)
    return tokens


def _publisher_matches_company(company: str, publisher: str) -> bool:
    if not company or not publisher:
        return False
    brand_token = _norm_token(company)
    publisher_token = _norm_token(publisher)
    if brand_token and brand_token == publisher_token:
        return True

    company_tokens = _name_tokens(company)
    publisher_tokens = set(_name_tokens(publisher, min_len=3))
    if len(company_tokens) >= 2 and set(company_tokens).issubset(publisher_tokens):
        return True

    if len(company_tokens) == 1 and brand_token:
        if publisher_token == brand_token:
            return True
        if publisher_token.startswith(brand_token):
            suffix = publisher_token[len(brand_token):]
            if suffix and suffix in PUBLISHER_SUFFIX_TOKENS:
                return True
    return False


def _host_matches_social(base_host: str, host: str) -> bool:
    return host == base_host or host.endswith("." + base_host)


def _social_platform_tokens_for_host(host: str) -> tuple[str, ...]:
    for base_host, tokens in SOCIAL_BYLINE_PLATFORM_PATTERNS.items():
        if _host_matches_social(base_host, host):
            return tokens
    return ()


def _extract_social_byline_author(snippet: str, host: str) -> str:
    if not snippet or not host:
        return ""
    platform_tokens = _social_platform_tokens_for_host(host)
    if not platform_tokens:
        return ""
    platforms = "|".join(re.escape(token) for token in platform_tokens)
    pattern = re.compile(
        rf"\bby\s+(.{{2,120}}?)\s+on\s+(?:{platforms})\b",
        flags=re.IGNORECASE,
    )
    matches = list(pattern.finditer(snippet))
    if not matches:
        return ""
    author = matches[-1].group(1).strip(" \"'.,:;()[]{}")
    return " ".join(author.split())


def _social_byline_matches_company(company: str, snippet: str, host: str) -> bool:
    if not company or not snippet or not host:
        return False
    author = _extract_social_byline_author(snippet, host)
    if not author:
        return False
    return _publisher_matches_company(company, author)


def _instagram_handle_from_path(path: str) -> str:
    if not path:
        return ""
    segments = [seg for seg in path.strip("/").split("/") if seg]
    if not segments:
        return ""
    first = segments[0].lstrip("@").lower()
    reserved = {
        "p", "reel", "reels", "stories", "explore", "accounts", "tv",
        "about", "privacy", "legal", "developer",
    }
    if first in reserved:
        return ""
    return first


def _is_instagram_company_handle(company: str, url: str) -> bool:
    if not company or not url:
        return False
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower().replace("www.", "")
    if not _host_matches_social("instagram.com", host):
        return False
    handle_token = _norm_token(_instagram_handle_from_path(parsed.path or ""))
    if not handle_token:
        return False
    for token in _company_handle_tokens(company):
        if token and (token in handle_token or handle_token in token):
            return True
    return False


def _is_instagram_person_handle(name: str, url: str) -> bool:
    if not name or not url:
        return False
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower().replace("www.", "")
    if not _host_matches_social("instagram.com", host):
        return False
    handle_token = _norm_token(_instagram_handle_from_path(parsed.path or ""))
    if not handle_token:
        return False
    for token in _person_handle_tokens(name):
        if token and (token in handle_token or handle_token in token):
            return True
    return False


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


def _is_linkedin_company_post(company: str, url: str) -> bool:
    if not url or not company:
        return False
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower().replace("www.", "")
    if host != "linkedin.com":
        return False
    path = (parsed.path or "").strip("/")
    if not path.lower().startswith("posts/"):
        return False
    slug = path.split("/", 1)[1] if "/" in path else ""
    slug = slug.split("/", 1)[0] if slug else ""
    if not slug:
        return False
    handle = slug.split("_", 1)[0] if "_" in slug else slug
    brand_token = _norm_token(company)
    handle_token = _norm_token(handle)
    if brand_token and brand_token in handle_token:
        return True
    return _linkedin_slug_matches_company(company, handle)

def _linkedin_slug_matches_company(company: str, slug: str) -> bool:
    if not company or not slug:
        return False
    company_tokens = [
        _norm_token(t) for t in re.split(r"\W+", company.lower()) if t
    ]
    company_tokens = [
        t for t in company_tokens if t and t not in NAME_IGNORE_TOKENS and len(t) >= 4
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
    publisher: str | None = None,
    snippet: str | None = None,
) -> bool:
    if _publisher_matches_company(company, publisher or ""):
        return True
    host = hostname(url)
    if not host:
        return False
    try:
        path = (urlparse(url).path or "").lower()
    except Exception:
        path = ""
    social_byline_controlled = _social_byline_matches_company(company, snippet or "", host)
    is_facebook = _host_matches_social("facebook.com", host)
    is_instagram = _host_matches_social("instagram.com", host)
    is_threads = _host_matches_social("threads.net", host)
    is_x = _host_matches_social("x.com", host) or _host_matches_social("twitter.com", host)

    if entity_type == "ceo":
        for bad in CEO_UNCONTROLLED_DOMAINS:
            if host == bad or host.endswith("." + bad):
                if social_byline_controlled:
                    return True
                return False
        if person_name and _is_linkedin_person_profile(person_name, url):
            return True
        if person_name and _is_x_person_handle(person_name, url):
            return True
    if is_facebook:
        if any(seg in path for seg in ("/posts/", "/photos/", "/videos/")):
            return social_byline_controlled
        return True
    if is_instagram:
        if "/reels/" in path or "/reel/" in path:
            # Reels are only controlled when snippet attribution matches the brand.
            if social_byline_controlled:
                return True
            if _is_instagram_company_handle(company, url):
                return True
            if entity_type == "ceo" and person_name and _is_instagram_person_handle(person_name, url):
                return True
            return False
        if "/p/" in path:
            return social_byline_controlled
        return True
    if is_threads:
        if "/posts/" in path:
            return social_byline_controlled
        return True
    if _is_brand_youtube_channel(company, url):
        return True
    if social_byline_controlled and (
        _host_matches_social("youtube.com", host)
        or _host_matches_social("youtu.be", host)
        or is_x
    ):
        return True
    if _is_linkedin_company_post(company, url):
        return True
    if _is_linkedin_company_page(company, url):
        return True
    if "/status/" in path and is_x:
        if social_byline_controlled:
            return True
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


def has_material_risk_terms(title: str, snippet: str = "", source: str = "") -> bool:
    hay = f"{title} {snippet} {source}".strip()
    return bool(MATERIAL_RISK_TERMS_RE.search(hay))


def should_neutralize_finance_routine(
    sentiment: str | None,
    title: str,
    snippet: str = "",
    url: str = "",
    source: str = "",
    finance_routine: bool | None = None,
) -> bool:
    if sentiment not in {"positive", "negative"}:
        return False
    is_routine = finance_routine if finance_routine is not None else is_financial_routine(title, snippet, url, source)
    if not is_routine:
        return False
    if has_material_risk_terms(title, snippet, source):
        return False
    return True


def _dedupe_preserve(items: list[str]) -> list[str]:
    out: list[str] = []
    seen = set()
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def classify_narrative_tags(
    title: str,
    snippet: str = "",
    *,
    url: str = "",
    source: str = "",
    sentiment: str | None = None,
    finance_routine: bool | None = None,
) -> dict:
    """
    Rule-based narrative taxonomy for negative Top Stories items.
    Returns:
      {
        "primary_tag": str|None,
        "primary_group": "crisis"|"non_crisis"|None,
        "tags": list[str],
        "is_crisis": bool|None,
        "rule_version": str
      }
    """
    sentiment_l = (sentiment or "").strip().lower()
    if sentiment_l and sentiment_l != "negative":
        return {
            "primary_tag": None,
            "primary_group": None,
            "tags": [],
            "is_crisis": None,
            "rule_version": NARRATIVE_RULE_VERSION,
        }
    if finance_routine is True:
        return {
            "primary_tag": None,
            "primary_group": None,
            "tags": [],
            "is_crisis": None,
            "rule_version": NARRATIVE_RULE_VERSION,
        }

    hay = " ".join([title or "", snippet or "", source or "", url or ""]).strip()
    if not hay:
        return {
            "primary_tag": None,
            "primary_group": None,
            "tags": [],
            "is_crisis": None,
            "rule_version": NARRATIVE_RULE_VERSION,
        }

    crisis_tags: list[str] = []
    non_crisis_tags: list[str] = []

    if NARRATIVE_REBRANDING_RE.search(hay):
        non_crisis_tags.append("Rebranding")
    if NARRATIVE_MNA_RE.search(hay):
        non_crisis_tags.append("Mergers and acquisitions")
    if NARRATIVE_PLANNED_EXEC_RE.search(hay) and not NARRATIVE_PLANNED_EXEC_EXCLUDE_RE.search(hay):
        non_crisis_tags.append("Planned Executive Turnover")

    if NARRATIVE_WORKFORCE_RE.search(hay):
        crisis_tags.append("Workforce Reductions")
    if NARRATIVE_ACCIDENT_RE.search(hay):
        crisis_tags.append("Accidents & Disasters")
    if NARRATIVE_DATA_BREACH_RE.search(hay):
        crisis_tags.append("Data Breaches")
    if NARRATIVE_LEGAL_RE.search(hay):
        crisis_tags.append("Legal & Regulatory")
    if NARRATIVE_UNFORCED_RE.search(hay):
        crisis_tags.append("Unforced Errors (marketing, executive actions/comments)")
    if NARRATIVE_LABOR_RE.search(hay):
        crisis_tags.append("Labor Disputes")
    if NARRATIVE_CEO_DEPART_RE.search(hay):
        crisis_tags.append("CEO Departures (firings, resignations)")
    if NARRATIVE_FRAUD_RE.search(hay):
        crisis_tags.append("Fraud")

    crisis_tags = _dedupe_preserve(crisis_tags)
    non_crisis_tags = _dedupe_preserve(non_crisis_tags)

    if crisis_tags:
        # Crisis takes precedence if both crisis and non-crisis cues appear.
        tags = _dedupe_preserve(crisis_tags + non_crisis_tags)
        return {
            "primary_tag": crisis_tags[0],
            "primary_group": "crisis",
            "tags": tags,
            "is_crisis": True,
            "rule_version": NARRATIVE_RULE_VERSION,
        }
    if non_crisis_tags:
        return {
            "primary_tag": non_crisis_tags[0],
            "primary_group": "non_crisis",
            "tags": non_crisis_tags,
            "is_crisis": False,
            "rule_version": NARRATIVE_RULE_VERSION,
        }

    # Fallback category for negative, non-financial items with no specific match.
    return {
        "primary_tag": "Other",
        "primary_group": "crisis",
        "tags": ["Other"],
        "is_crisis": True,
        "rule_version": NARRATIVE_RULE_VERSION,
    }
