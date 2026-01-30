#!/usr/bin/env python3
"""
Aggregate SERP feature presence from raw Parquet (SerpAPI JSON) into Postgres.

Inputs:
  - Parquet files with columns: query, raw_json
  - raw_json contains SerpAPI response under $.data

Writes to:
  serp_feature_daily (date, entity_type, entity_id, entity_name, feature_type, total_count)
"""
from __future__ import annotations

import argparse
from urllib.parse import urlparse
import json
import os
import re
import hashlib
from datetime import datetime
from typing import Dict, List, Tuple, Set

import duckdb
import psycopg2
from psycopg2.extras import execute_values
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


FEATURES = [
    "aio",
    "paa",
    "videos",
    "social",
    "perspectives",
    "top_stories",
    # "knowledge_graph",
]

FINANCE_TERMS = [
    r"\bearnings\b", r"\beps\b", r"\brevenue\b", r"\bguidance\b", r"\bforecast\b",
    r"\bprice target\b", r"\bupgrade\b", r"\bdowngrade\b", r"\bdividend\b",
    r"\bbuyback\b", r"\bshares?\b", r"\bstock\b", r"\bmarket cap\b",
    r"\bquarterly\b", r"\bfiscal\b", r"\bprofit\b", r"\bebitda\b",
    r"\b10-q\b", r"\b10-k\b", r"\bsec\b", r"\bipo\b"
]
FINANCE_TERMS_RE = re.compile("|".join(FINANCE_TERMS), flags=re.IGNORECASE)
FINANCE_SOURCES = {
    "yahoo.com", "marketwatch.com", "fool.com", "benzinga.com",
    "seekingalpha.com", "thefly.com", "barrons.com", "wsj.com",
    "investorplace.com", "nasdaq.com", "foolcdn.com"
}

ALWAYS_CONTROLLED_DOMAINS: Set[str] = {
    "facebook.com",
    "instagram.com",
    "play.google.com",
    "apps.apple.com",
}

UNCONTROLLED_DOMAINS: Set[str] = {
    "wikipedia.org",
    "youtube.com",
    "youtu.be",
    "tiktok.com",
}

CONTROLLED_PATH_KEYWORDS = {
    "/leadership/", "/about/", "/governance/", "/team/", "/investors/",
    "/board-of-directors", "/members/", "/member/"
}


def get_conn():
    dsn = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not dsn:
        raise SystemExit("Set DATABASE_URL or SUPABASE_DB_URL")
    return psycopg2.connect(dsn)


def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = str(name).strip()
    suffixes = [
        " Inc.", " Inc", " Corporation", " Corp.", " Corp",
        " Company", " Co.", " Co", " LLC", " L.L.C.",
        " Ltd.", " Ltd", " Limited", " PLC", " plc",
        ".com", ".net", ".org"
    ]
    for suffix in sorted(suffixes, key=len, reverse=True):
        if name.lower().endswith(suffix.lower()):
            name = name[:-len(suffix)].strip()
            break
    return "".join(ch for ch in name.lower() if ch.isalnum() or ch.isspace()).strip()

def _norm_token(name: str) -> str:
    return "".join(ch for ch in normalize_name(name) if ch.isalnum())

def _is_brand_youtube_channel(company: str, url: str) -> bool:
    host = _hostname(url)
    if host not in {"youtube.com", "youtu.be"}:
        return False
    token = _norm_token(company)
    if not token:
        return False
    try:
        from urllib.parse import urlparse
        path = (urlparse(url).path or "").lower()
    except Exception:
        path = ""
    return token in path.replace("-", "").replace("_", "")

def _hostname(url: str) -> str:
    try:
        from urllib.parse import urlparse
        host = (urlparse(url).hostname or "").lower()
        return host.replace("www.", "")
    except Exception:
        return ""


def _norm_token(s: str) -> str:
    return "".join(ch for ch in (s or "").lower() if ch.isalnum())


def _company_handle_tokens(company: str) -> Set[str]:
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
    return bool(brand_token) and brand_token in slug_token


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

def _is_financial_routine(title: str, snippet: str = "", url: str = "", source: str = "") -> bool:
    hay = f"{title} {snippet}".strip()
    if FINANCE_TERMS_RE.search(hay):
        return True
    host = _hostname(url)
    if host and any(host == d or host.endswith("." + d) for d in FINANCE_SOURCES):
        return True
    if source and any(source.lower().endswith(d) for d in FINANCE_SOURCES):
        return True
    return False

def _vader_label(analyzer: SentimentIntensityAnalyzer, text: str) -> Tuple[str, float]:
    scores = analyzer.polarity_scores(text or "")
    compound = scores.get("compound", 0.0)
    if compound >= 0.15:
        return "positive", compound
    if compound <= -0.10:
        return "negative", compound
    return "neutral", compound

def load_company_map(cur) -> Dict[str, Tuple[str, str]]:
    cur.execute("select id, name from companies")
    out = {}
    for cid, name in cur.fetchall():
        out[normalize_name(name)] = (str(cid), name)
    return out


def load_ceo_map(cur) -> Dict[Tuple[str, str], str]:
    cur.execute("""
        select ceo.id, ceo.name, c.id, c.name
        from ceos ceo
        join companies c on c.id = ceo.company_id
    """)
    out = {}
    for ceo_id, ceo_name, company_id, company_name in cur.fetchall():
        out[(normalize_name(ceo_name), str(company_id))] = str(ceo_id)
    return out


def match_company(query: str, company_map: Dict[str, Tuple[str, str]]):
    key = normalize_name(query)
    if not key:
        return None
    if key in company_map:
        return company_map.get(key)
    padded = f" {key} "
    candidates = sorted(company_map.items(), key=lambda kv: len(kv[0]), reverse=True)
    for norm_name, value in candidates:
        if f" {norm_name} " in padded:
            return value
        if key.startswith(f"{norm_name} ") or key.endswith(f" {norm_name}"):
            return value
    return None


def match_ceo(query: str, company_map: Dict[str, Tuple[str, str]], ceo_map: Dict[Tuple[str, str], str]):
    q = query.strip()
    q_lower = q.lower()
    # Match company by longest suffix
    candidates = sorted(company_map.items(), key=lambda kv: len(kv[0]), reverse=True)
    for norm_name, (company_id, company_name) in candidates:
        if q_lower.endswith(norm_name.lower()) or q_lower.endswith(company_name.lower()):
            ceo_part = q[: -len(company_name)].strip()
            ceo_key = normalize_name(ceo_part)
            ceo_id = ceo_map.get((ceo_key, company_id))
            return company_id, company_name, ceo_id, ceo_part
    return None, None, None, q


def load_company_domains(cur) -> Dict[str, Set[str]]:
    cur.execute("select name, websites from companies")
    company_domains: Dict[str, Set[str]] = {}
    for name, websites in cur.fetchall():
        company = str(name or "").strip()
        if not company:
            continue
        if not websites:
            continue
        urls = str(websites).split("|")
        for url in urls:
            url = str(url or "").strip()
            if not url or url.lower() == "nan":
                continue
            if not url.startswith(("http://", "https://")):
                url = f"http://{url}"
            host = _hostname(url)
            if host and "." in host:
                company_domains.setdefault(company, set()).add(host)
    return company_domains


def classify_control(company: str, url: str, company_domains: Dict[str, Set[str]]) -> bool:
    host = _hostname(url)
    if not host:
        return False

    path = ""
    try:
        from urllib.parse import urlparse
        path = (urlparse(url).path or "").lower()
    except Exception:
        path = ""

    if host == "facebook.com":
        return "/posts/" not in path
    if host == "instagram.com":
        return "/p/" not in path
    if _is_linkedin_company_page(company, url):
        return True
    if _is_x_company_handle(company, url):
        return True

    if host in UNCONTROLLED_DOMAINS or any(host.endswith("." + d) for d in UNCONTROLLED_DOMAINS):
        return _is_brand_youtube_channel(company, url)

    if _is_brand_youtube_channel(company, url):
        return True

    for good in ALWAYS_CONTROLLED_DOMAINS:
        if host == good or host.endswith("." + good):
            return True

    if any(k in path for k in CONTROLLED_PATH_KEYWORDS):
        return True

    company_specific_domains = company_domains.get(company, set())
    for rd in company_specific_domains:
        if host == rd or host.endswith("." + rd):
            return True

    brand_token = _norm_token(company)
    if brand_token:
        host_parts = host.split(".")
        normalized_parts = [_norm_token(part) for part in host_parts if part]
        if brand_token in normalized_parts[:-1]:
            return True

    return False


def parquet_url(date_str: str, entity_type: str) -> str:
    suffix = "brand" if entity_type == "brand" else "ceo"
    return f"https://tk-public-data.s3.us-east-1.amazonaws.com/serp_files/{date_str}-{suffix}-raw-queries.parquet"


def load_feature_counts(path_or_url: str):
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    sql = f"""
        select
          query,
          case when json_extract(raw_json, '$.data.ai_overview') is null then 0 else 1 end as aio_count,
          coalesce(json_array_length(json_extract(raw_json, '$.data.related_questions')), 0) as paa_count,
          coalesce(json_array_length(json_extract(raw_json, '$.data.inline_videos')), 0) +
          coalesce(json_array_length(json_extract(raw_json, '$.data.short_videos')), 0) as videos_count,
          coalesce(json_array_length(json_extract(raw_json, '$.data.latest_posts')), 0) as social_count,
          coalesce(json_array_length(json_extract(raw_json, '$.data.perspectives')), 0) as perspectives_count,
          coalesce(json_array_length(json_extract(raw_json, '$.data.top_stories')), 0) as top_stories_count,
          case when json_extract(raw_json, '$.data.knowledge_graph') is null then 0 else 1 end as knowledge_graph_count
        from read_parquet('{path_or_url}')
    """
    return con.execute(sql).fetch_df()

def load_aio_citation_sentiment(path_or_url: str):
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    df = con.execute(f"select query, raw_json from read_parquet('{path_or_url}')").fetch_df()
    analyzer = SentimentIntensityAnalyzer()
    out = {}
    for _, row in df.iterrows():
        query = str(row.get("query") or "").strip()
        if not query:
            continue
        raw = row.get("raw_json")
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        aio = obj.get("data", {}).get("ai_overview")
        if not aio or not isinstance(aio, dict):
            continue
        references = aio.get("references") or []
        pos = neu = neg = 0
        for ref in references:
            if not isinstance(ref, dict):
                continue
            title = str(ref.get("title") or "")
            snippet = str(ref.get("snippet") or "")
            link = str(ref.get("link") or "")
            source = str(ref.get("source") or "")
            if _is_financial_routine(title, snippet, link, source):
                label = "neutral"
            else:
                label, _ = _vader_label(analyzer, f"{title} {snippet}".strip())
            if label == "positive":
                pos += 1
            elif label == "negative":
                neg += 1
            else:
                neu += 1
        if pos or neu or neg:
            out[query] = (pos, neu, neg, pos + neu + neg)
    return out

def load_paa_sentiment(path_or_url: str):
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    df = con.execute(f"select query, raw_json from read_parquet('{path_or_url}')").fetch_df()
    analyzer = SentimentIntensityAnalyzer()
    out = {}
    for _, row in df.iterrows():
        query = str(row.get("query") or "").strip()
        if not query:
            continue
        raw = row.get("raw_json")
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        items = obj.get("data", {}).get("related_questions") or []
        pos = neu = neg = 0
        for item in items:
            if not isinstance(item, dict):
                continue
            text = str(item.get("question") or "")
            blocks = item.get("text_blocks") or []
            for b in blocks:
                if isinstance(b, dict):
                    text = f"{text} {b.get('snippet') or ''}".strip()
            if not text:
                continue
            label, _ = _vader_label(analyzer, text)
            if label == "positive":
                pos += 1
            elif label == "negative":
                neg += 1
            else:
                neu += 1
        if pos or neu or neg:
            out[query] = (pos, neu, neg, pos + neu + neg)
    return out

def load_videos_sentiment(path_or_url: str):
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    df = con.execute(f"select query, raw_json from read_parquet('{path_or_url}')").fetch_df()
    analyzer = SentimentIntensityAnalyzer()
    out = {}
    for _, row in df.iterrows():
        query = str(row.get("query") or "").strip()
        if not query:
            continue
        raw = row.get("raw_json")
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        data = obj.get("data", {})
        items = []
        items.extend(data.get("inline_videos") or [])
        items.extend(data.get("short_videos") or [])
        pos = neu = neg = 0
        for item in items:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "")
            snippet = str(item.get("snippet") or "")
            text = f"{title} {snippet}".strip()
            if not text:
                continue
            label, _ = _vader_label(analyzer, text)
            if label == "positive":
                pos += 1
            elif label == "negative":
                neg += 1
            else:
                neu += 1
        if pos or neu or neg:
            out[query] = (pos, neu, neg, pos + neu + neg)
    return out

def load_perspectives_sentiment(path_or_url: str):
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    df = con.execute(f"select query, raw_json from read_parquet('{path_or_url}')").fetch_df()
    analyzer = SentimentIntensityAnalyzer()
    out = {}
    for _, row in df.iterrows():
        query = str(row.get("query") or "").strip()
        if not query:
            continue
        raw = row.get("raw_json")
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        items = obj.get("data", {}).get("perspectives") or []
        pos = neu = neg = 0
        for item in items:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "")
            snippet = str(item.get("snippet") or "")
            text = f"{title} {snippet}".strip()
            if not text:
                continue
            label, _ = _vader_label(analyzer, text)
            if label == "positive":
                pos += 1
            elif label == "negative":
                neg += 1
            else:
                neu += 1
        if pos or neu or neg:
            out[query] = (pos, neu, neg, pos + neu + neg)
    return out


def load_top_stories_sentiment(path_or_url: str):
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    df = con.execute(f"select query, raw_json from read_parquet('{path_or_url}')").fetch_df()
    analyzer = SentimentIntensityAnalyzer()
    out = {}
    for _, row in df.iterrows():
        query = str(row.get("query") or "").strip()
        if not query:
            continue
        raw = row.get("raw_json")
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        items = obj.get("data", {}).get("top_stories") or []
        pos = neu = neg = 0
        for item in items:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "")
            snippet = str(item.get("snippet") or "")
            link = str(item.get("link") or "")
            source = str(item.get("source") or "")
            label, _ = _sentiment_for_item(analyzer, title, snippet, link, source)
            if label == "positive":
                pos += 1
            elif label == "negative":
                neg += 1
            else:
                neu += 1
        if pos or neu or neg:
            out[query] = (pos, neu, neg, pos + neu + neg)
    return out


def _item_hash(url: str, title: str, snippet: str, feature_type: str, position: int) -> str:
    base = url or f"{feature_type}|{position}|{title}|{snippet}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()


def _sentiment_for_item(analyzer: SentimentIntensityAnalyzer, title: str, snippet: str, url: str, source: str):
    finance_routine = _is_financial_routine(title, snippet, url, source)
    if finance_routine:
        return "neutral", True
    label, _ = _vader_label(analyzer, f"{title} {snippet}".strip())
    return label, False


def load_feature_items(path_or_url: str, date_str: str, entity_type: str,
                       company_map, ceo_map, company_domains: Dict[str, Set[str]]):
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    df = con.execute(f"select query, raw_json from read_parquet('{path_or_url}')").fetch_df()
    analyzer = SentimentIntensityAnalyzer()
    rows = []

    for _, row in df.iterrows():
        query = str(row.get("query") or "").strip()
        if not query:
            continue
        raw = row.get("raw_json")
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue

        if entity_type == "brand":
            match = match_company(query, company_map)
            if not match:
                continue
            entity_id, entity_name = match
            company_name = entity_name
        else:
            company_id, company_name, ceo_id, ceo_name = match_ceo(query, company_map, ceo_map)
            if not ceo_id:
                continue
            entity_id = ceo_id
            entity_name = ceo_name or query

        data = obj.get("data", {}) or {}

        items = []
        aio = data.get("ai_overview") or {}
        refs = aio.get("references") or []
        for idx, ref in enumerate(refs):
            if not isinstance(ref, dict):
                continue
            items.append({
                "feature_type": "aio_citations",
                "item_type": "aio_reference",
                "title": str(ref.get("title") or ""),
                "snippet": str(ref.get("snippet") or ""),
                "url": str(ref.get("link") or ""),
                "source": str(ref.get("source") or ""),
                "position": idx + 1,
            })

        paa = data.get("related_questions") or []
        for idx, item in enumerate(paa):
            if not isinstance(item, dict):
                continue
            items.append({
                "feature_type": "paa_items",
                "item_type": "paa_question",
                "title": str(item.get("question") or item.get("title") or ""),
                "snippet": str(item.get("snippet") or item.get("answer") or ""),
                "url": str(item.get("link") or item.get("source") or ""),
                "source": str(item.get("source") or ""),
                "position": idx + 1,
            })

        videos = (data.get("inline_videos") or []) + (data.get("short_videos") or [])
        for idx, item in enumerate(videos):
            if not isinstance(item, dict):
                continue
            items.append({
                "feature_type": "videos_items",
                "item_type": "video",
                "title": str(item.get("title") or ""),
                "snippet": str(item.get("snippet") or ""),
                "url": str(item.get("link") or item.get("url") or ""),
                "source": str(item.get("source") or ""),
                "position": idx + 1,
            })

        perspectives = data.get("perspectives") or []
        for idx, item in enumerate(perspectives):
            if not isinstance(item, dict):
                continue
            items.append({
                "feature_type": "perspectives_items",
                "item_type": "perspective",
                "title": str(item.get("title") or ""),
                "snippet": str(item.get("snippet") or ""),
                "url": str(item.get("link") or ""),
                "source": str(item.get("source") or ""),
                "position": idx + 1,
            })

        top_stories = data.get("top_stories") or []
        for idx, item in enumerate(top_stories):
            if not isinstance(item, dict):
                continue
            items.append({
                "feature_type": "top_stories_items",
                "item_type": "top_story",
                "title": str(item.get("title") or ""),
                "snippet": str(item.get("snippet") or ""),
                "url": str(item.get("link") or ""),
                "source": str(item.get("source") or ""),
                "position": idx + 1,
            })

        for item in items:
            title = item.get("title", "")
            snippet = item.get("snippet", "")
            url = item.get("url", "")
            source = item.get("source", "")
            position = int(item.get("position") or 0) or None
            feature = item.get("feature_type", "")
            item_type = item.get("item_type", "")
            domain = _hostname(url) if url else ""

            sentiment_label, finance_routine = _sentiment_for_item(
                analyzer, title, snippet, url, source
            )
            control_class = None
            if url and company_name:
                control_class = "controlled" if classify_control(company_name, url, company_domains) else "uncontrolled"

            url_hash = _item_hash(url, title, snippet, feature, position or 0)
            rows.append((
                date_str,
                entity_type,
                entity_id,
                entity_name,
                feature,
                item_type,
                title,
                snippet,
                url,
                domain,
                position,
                url_hash,
                sentiment_label,
                control_class,
                finance_routine,
                source,
            ))

    return rows


def upsert_feature_items(cur, rows, source: str):
    if not rows:
        return 0
    deduped = {}
    for row in rows:
        key = (row[0], row[1], row[3], row[4], row[11])
        deduped[key] = row
    rows = list(deduped.values())
    sql = """
        insert into serp_feature_items
          (date, entity_type, entity_id, entity_name, feature_type, item_type,
           title, snippet, url, domain, position, url_hash,
           sentiment_label, control_class, finance_routine, source)
        values %s
        on conflict (date, entity_type, entity_name, feature_type, url_hash) do update set
          title = excluded.title,
          snippet = excluded.snippet,
          url = excluded.url,
          domain = excluded.domain,
          position = excluded.position,
          sentiment_label = excluded.sentiment_label,
          control_class = excluded.control_class,
          finance_routine = excluded.finance_routine,
          source = excluded.source,
          updated_at = now()
    """
    execute_values(cur, sql, rows, page_size=1000)
    return len(rows)
def build_feature_rows(df, date_str: str, entity_type: str, company_map, ceo_map,
                       aio_sentiment, paa_sentiment, videos_sentiment, perspectives_sentiment, top_stories_sentiment):
    rows = []
    for _, r in df.iterrows():
        query = str(r.get("query") or "").strip()
        if not query:
            continue
        if entity_type == "brand":
            match = match_company(query, company_map)
            if match:
                entity_id, entity_name = match
            else:
                entity_id, entity_name = None, query
        else:
            company_id, company_name, ceo_id, ceo_name = match_ceo(query, company_map, ceo_map)
            entity_id = ceo_id
            entity_name = ceo_name or query
        counts = {
            "aio": int(r.get("aio_count", 0) or 0),
            "paa": int(r.get("paa_count", 0) or 0),
            "videos": int(r.get("videos_count", 0) or 0),
            "social": int(r.get("social_count", 0) or 0),
            "perspectives": int(r.get("perspectives_count", 0) or 0),
            "top_stories": int(r.get("top_stories_count", 0) or 0),
            # "knowledge_graph": int(r.get("knowledge_graph_count", 0) or 0),
        }
        for feature, count in counts.items():
            if count <= 0:
                continue
            rows.append((date_str, entity_type, entity_id, entity_name, feature, count, 0, 0, 0))

        if query in aio_sentiment:
            pos, neu, neg, total = aio_sentiment[query]
            rows.append((date_str, entity_type, entity_id, entity_name, "aio_citations", total, pos, neu, neg))
        if query in paa_sentiment:
            pos, neu, neg, total = paa_sentiment[query]
            rows.append((date_str, entity_type, entity_id, entity_name, "paa_items", total, pos, neu, neg))
        if query in videos_sentiment:
            pos, neu, neg, total = videos_sentiment[query]
            rows.append((date_str, entity_type, entity_id, entity_name, "videos_items", total, pos, neu, neg))
        if query in perspectives_sentiment:
            pos, neu, neg, total = perspectives_sentiment[query]
            rows.append((date_str, entity_type, entity_id, entity_name, "perspectives_items", total, pos, neu, neg))
        if query in top_stories_sentiment:
            pos, neu, neg, total = top_stories_sentiment[query]
            rows.append((date_str, entity_type, entity_id, entity_name, "top_stories_items", total, pos, neu, neg))
    return rows


def upsert_feature_rows(cur, rows, source: str):
    if not rows:
        return 0
    sql = """
        insert into serp_feature_daily
          (date, entity_type, entity_id, entity_name, feature_type, total_count,
           positive_count, neutral_count, negative_count, source)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (date, entity_type, entity_name, feature_type) do update set
          total_count = excluded.total_count,
          positive_count = excluded.positive_count,
          neutral_count = excluded.neutral_count,
          negative_count = excluded.negative_count,
          source = excluded.source,
          updated_at = now()
    """
    cur.executemany(
        sql,
        [(d, et, eid, ename, feat, cnt, pos, neu, neg, source)
         for d, et, eid, ename, feat, cnt, pos, neu, neg in rows]
    )
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--entity-type", choices=["brand", "ceo"], required=True)
    parser.add_argument("--source-url", default="", help="Override parquet URL")
    args = parser.parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        raise SystemExit("Invalid --date, expected YYYY-MM-DD")

    url = args.source_url or parquet_url(args.date, args.entity_type)
    print(f"Reading parquet: {url}")

    with get_conn() as conn:
        with conn.cursor() as cur:
            company_map = load_company_map(cur)
            ceo_map = load_ceo_map(cur)
            company_domains = load_company_domains(cur)

        df = load_feature_counts(url)
        aio_sentiment = load_aio_citation_sentiment(url)
        paa_sentiment = load_paa_sentiment(url)
        videos_sentiment = load_videos_sentiment(url)
        perspectives_sentiment = load_perspectives_sentiment(url)
        top_stories_sentiment = load_top_stories_sentiment(url)
        rows = build_feature_rows(
            df, args.date, args.entity_type, company_map, ceo_map,
            aio_sentiment, paa_sentiment, videos_sentiment, perspectives_sentiment, top_stories_sentiment
        )
        item_rows = load_feature_items(
            url, args.date, args.entity_type, company_map, ceo_map, company_domains
        )

        with conn.cursor() as cur:
            inserted = upsert_feature_rows(cur, rows, "aio_parquet")
            inserted_items = upsert_feature_items(cur, item_rows, "aio_parquet")
        conn.commit()

    print(f"Rows upserted: {inserted}")
    print(f"Item rows upserted: {inserted_items}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
