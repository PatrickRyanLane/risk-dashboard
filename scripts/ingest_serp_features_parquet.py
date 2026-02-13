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
import time
from urllib.parse import urlparse
import json
import os
import re
import hashlib
from datetime import datetime
from typing import Dict, List, Tuple, Set

import duckdb
import psycopg2
import requests
from psycopg2.extras import execute_values
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from risk_rules import classify_control, is_financial_routine, parse_company_domains, title_mentions_legal_trouble

FEATURES = [
    "aio",
    "paa",
    "videos",
    "social",
    "perspectives",
    "top_stories",
    # "knowledge_graph",
]

DOWNLOAD_TIMEOUT = int(os.getenv("PARQUET_DOWNLOAD_TIMEOUT", "60"))



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

def _hostname(url: str) -> str:
    try:
        from urllib.parse import urlparse
        host = (urlparse(url).hostname or "").lower()
        return host.replace("www.", "")
    except Exception:
        return ""


def _vader_label(analyzer: SentimentIntensityAnalyzer, text: str) -> Tuple[str, float]:
    scores = analyzer.polarity_scores(text or "")
    compound = scores.get("compound", 0.0)
    if compound >= 0.15:
        return "positive", compound
    if compound <= -0.10:
        return "negative", compound
    return "neutral", compound


def _step(label: str, fn):
    print(f"[STEP] {label}...")
    start = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - start
    try:
        size = len(result)
        print(f"[STEP] {label} done in {elapsed:.2f}s (rows={size})")
    except Exception:
        print(f"[STEP] {label} done in {elapsed:.2f}s")
    return result


def _log_db_locks(cur):
    try:
        cur.execute("""
            select
              a.pid,
              now() - a.query_start as wait,
              a.state,
              a.wait_event_type,
              a.wait_event,
              a.query
            from pg_stat_activity a
            where cardinality(pg_blocking_pids(a.pid)) > 0
            order by wait desc
            limit 10
        """)
        rows = cur.fetchall()
        if rows:
            print("[STEP] lock_check: blocking detected")
            for pid, wait, state, wait_type, wait_event, query in rows:
                print(f"[LOCK] pid={pid} wait={wait} state={state} {wait_type}:{wait_event}")
                print(f"[LOCK] query={query[:300]}")
        else:
            print("[STEP] lock_check: no blocking queries")
    except Exception as exc:
        print(f"[STEP] lock_check: failed ({exc})")


def _chunked(rows, size: int):
    for i in range(0, len(rows), size):
        yield i, rows[i:i + size]


def _cache_parquet(url: str, date_str: str, entity_type: str) -> str:
    if not url.lower().startswith("http"):
        return url
    cache_path = f"/tmp/serp-features-{date_str}-{entity_type}.parquet"
    if os.path.exists(cache_path) and os.path.getsize(cache_path) > 0:
        print(f"Using cached parquet: {cache_path}")
        return cache_path
    print(f"Downloading parquet to {cache_path}...")
    with requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT) as resp:
        resp.raise_for_status()
        with open(cache_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    fh.write(chunk)
    print(f"Downloaded parquet bytes: {os.path.getsize(cache_path)}")
    return cache_path

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
        domains = parse_company_domains(str(websites))
        if domains:
            company_domains.setdefault(company, set()).update(domains)
    return company_domains


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
            if is_financial_routine(title, snippet=snippet, url=link, source=source):
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
    if title_mentions_legal_trouble(title, snippet):
        return "negative", False
    finance_routine = is_financial_routine(title, snippet=snippet, url=url, source=source)
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
                control_class = "controlled" if classify_control(
                    company_name,
                    url,
                    company_domains,
                    entity_type="ceo" if entity_type == "ceo" else "company",
                    person_name=entity_name if entity_type == "ceo" else None,
                ) else "uncontrolled"
            if control_class == "controlled":
                sentiment_label = "positive"

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
    total = len(rows)
    if total:
        chunk_size = 5000
        for idx, chunk in _chunked(rows, chunk_size):
            execute_values(cur, sql, chunk, page_size=1000)
            print(f"[STEP] upsert_feature_items {idx + len(chunk)}/{total}")
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
    deduped = {}
    for row in rows:
        key = (row[0], row[1], row[3], row[4])
        deduped[key] = row
    rows = list(deduped.values())
    sql = """
        insert into serp_feature_daily
          (date, entity_type, entity_id, entity_name, feature_type, total_count,
           positive_count, neutral_count, negative_count, source)
        values %s
        on conflict (date, entity_type, entity_name, feature_type) do update set
          total_count = excluded.total_count,
          positive_count = excluded.positive_count,
          neutral_count = excluded.neutral_count,
          negative_count = excluded.negative_count,
          source = excluded.source,
          updated_at = now()
    """
    values = [
        (d, et, eid, ename, feat, cnt, pos, neu, neg, source)
        for d, et, eid, ename, feat, cnt, pos, neu, neg in rows
    ]
    total = len(values)
    chunk_size = 5000
    for idx, chunk in _chunked(values, chunk_size):
        execute_values(cur, sql, chunk, page_size=1000)
        print(f"[STEP] upsert_feature_rows {idx + len(chunk)}/{total}")
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
    local_path = _cache_parquet(url, args.date, args.entity_type)
    if local_path != url:
        print(f"Using local parquet: {local_path}")

    with get_conn() as conn:
        with conn.cursor() as cur:
            company_map = load_company_map(cur)
            ceo_map = load_ceo_map(cur)
            company_domains = load_company_domains(cur)

        df = _step("load_feature_counts", lambda: load_feature_counts(local_path))
        aio_sentiment = _step("load_aio_citation_sentiment", lambda: load_aio_citation_sentiment(local_path))
        paa_sentiment = _step("load_paa_sentiment", lambda: load_paa_sentiment(local_path))
        videos_sentiment = _step("load_videos_sentiment", lambda: load_videos_sentiment(local_path))
        perspectives_sentiment = _step("load_perspectives_sentiment", lambda: load_perspectives_sentiment(local_path))
        top_stories_sentiment = _step("load_top_stories_sentiment", lambda: load_top_stories_sentiment(local_path))
        rows = _step(
            "build_feature_rows",
            lambda: build_feature_rows(
                df, args.date, args.entity_type, company_map, ceo_map,
                aio_sentiment, paa_sentiment, videos_sentiment, perspectives_sentiment, top_stories_sentiment
            )
        )
        item_rows = _step(
            "load_feature_items",
            lambda: load_feature_items(
                local_path, args.date, args.entity_type, company_map, ceo_map, company_domains
            )
        )

        with conn.cursor() as cur:
            _step("lock_check", lambda: _log_db_locks(cur))
            inserted = _step("upsert_feature_rows", lambda: upsert_feature_rows(cur, rows, "aio_parquet"))
            inserted_items = _step("upsert_feature_items", lambda: upsert_feature_items(cur, item_rows, "aio_parquet"))
        conn.commit()

    print(f"Rows upserted: {inserted}")
    print(f"Item rows upserted: {inserted_items}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
