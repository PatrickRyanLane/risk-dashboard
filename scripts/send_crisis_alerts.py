#!/usr/bin/env python3
"""
Reads negative summary data from the DB and sends Slack alerts to Salesforce account owners.
Tracks alert history in GCS to prevent spamming.
Implements Dynamic Thresholding, Type-Specific Alerts, DAILY VOLUME CAPS, and JITTER.
"""

import os
import argparse
import requests
import urllib.parse
import re
import time
import hashlib
import random
from difflib import get_close_matches
from datetime import datetime, timedelta
from simple_salesforce import Salesforce
import psycopg2
import pandas as pd
from llm_utils import build_summary_prompt, call_llm_text

# --- CONFIG ---
DRY_RUN = True  # <--- SET TO True FOR TESTING
SLACK_BOT_TOKEN = os.getenv('SLACK_BOT_TOKEN')
SF_USERNAME = os.getenv('SF_USERNAME')
SF_PASSWORD = os.getenv('SF_PASSWORD')
SF_TOKEN = os.getenv('SF_SECURITY_TOKEN')
SLACK_CHANNEL = "#crisis-alerts-test" 
FALLBACK_SLACK_ID = "UT1EC3ENR" 
LLM_API_KEY = os.getenv("LLM_API_KEY", "")
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openai")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")
LLM_SUMMARY_MAX_CALLS = int(os.getenv("LLM_SUMMARY_MAX_CALLS", "20"))
LLM_CACHE_TABLE = "llm_summary_cache"
NEGATIVE_HISTORY_DAYS = int(os.getenv("NEGATIVE_HISTORY_DAYS", "180"))
RISK_LABEL_WEIGHT = float(os.getenv("RISK_LABEL_WEIGHT", "3"))
ALERT_BRANDS = os.getenv("ALERT_BRANDS", "1") == "1"
ALERT_CEOS = os.getenv("ALERT_CEOS", "1") == "1"

# SERP gating (negative + uncontrolled: same URL must be negative AND uncontrolled)
SERP_GATE_ENABLED = os.getenv("SERP_GATE_ENABLED", "1") == "1"
SERP_GATE_MIN = int(os.getenv("SERP_GATE_MIN", "2"))
SERP_GATE_DAYS = int(os.getenv("SERP_GATE_DAYS", "2"))
SERP_GATE_DEBUG = os.getenv("SERP_GATE_DEBUG", "1") == "1"
SERP_TOP_STORIES_REQUIRED = os.getenv("SERP_TOP_STORIES_REQUIRED", "1") == "1"
SERP_TOP_STORIES_NEG_MIN = int(os.getenv("SERP_TOP_STORIES_NEG_MIN", "3"))

# Configurable Floors
MIN_NEGATIVE_ARTICLES = 13
PERCENTILE_CUTOFF = 0.97
ALERT_COOLDOWN_HOURS = 168

# --- FLOOD PROTECTION ---
MAX_ALERTS_PER_DAY = 10  # Strict limit: Max 20 alerts per 24-hour rolling window

# Manual color mapping for your VIPs
OWNER_COLORS = {
    "Shannon Buell": "#a589e8", 
    "Ken Schiefer": "#ff8261", 
    "Kenneth Schiefer": "#ff8261", 
    "Mac Cummings":  "#6fb210", 
    "Maclaren Cummings":  "#6fb210", 
    "Brittney Lee":  "#58dbed", 
    "Chris Loman":   "#00586d", 
    "Fall Back":     "#ffc32e",  
}

def get_owner_color(owner_name):
    """Returns a specific color for VIPs, or the Fall Back yellow for EVERYONE else."""
    if not owner_name:
        return OWNER_COLORS["Fall Back"]
    for vip, color in OWNER_COLORS.items():
        if vip.lower() in owner_name.lower():
            return color
    return OWNER_COLORS["Fall Back"]

def normalize_name(name):
    if not name: return ""
    name = str(name).strip()
    suffixes = [
        ' Inc.', ' Inc', ' Corporation', ' Corp.', ' Corp', 
        ' Company', ' Co.', ' Co', ' LLC', ' L.L.C.', ' Ltd.', ' Ltd', 
        ' PLC', ' plc', ' Group', ' Holdings', ' .com'
    ]
    for suffix in sorted(suffixes, key=len, reverse=True):
        if name.lower().endswith(suffix.lower()):
            name = name[:-len(suffix)].strip()
            break
    name = re.sub(r'[^\w\s]', '', name)
    return name


def get_db_conn():
    dsn = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not dsn:
        return None
    return psycopg2.connect(dsn)


def ensure_alert_tables(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                create table if not exists alert_history (
                    alert_key text primary key,
                    sent_at timestamptz not null
                )
                """
            )
            cur.execute(
                f"""
                create table if not exists {LLM_CACHE_TABLE} (
                    llm_key text primary key,
                    summary text not null,
                    created_at timestamptz not null default now()
                )
                """
            )


def load_alert_history_db(conn):
    with conn.cursor() as cur:
        cur.execute("select alert_key, sent_at from alert_history")
        return {k: v.isoformat() for k, v in cur.fetchall()}


def load_llm_cache_db(conn, date_str: str):
    like = f"%|{date_str}"
    with conn.cursor() as cur:
        cur.execute(
            f"select llm_key, summary from {LLM_CACHE_TABLE} where llm_key like %s",
            (like,),
        )
        return {k: v for k, v in cur.fetchall()}


def upsert_alert_history_db(conn, history: dict):
    rows = [(k, v) for k, v in history.items()]
    if not rows:
        return
    sql = """
        insert into alert_history (alert_key, sent_at)
        values %s
        on conflict (alert_key) do update set
          sent_at = excluded.sent_at
    """
    with conn:
        with conn.cursor() as cur:
            from psycopg2.extras import execute_values
            execute_values(cur, sql, rows, page_size=1000)


def upsert_llm_cache_db(conn, cache: dict):
    rows = [(k, v, datetime.utcnow().isoformat()) for k, v in cache.items()]
    if not rows:
        return
    sql = f"""
        insert into {LLM_CACHE_TABLE} (llm_key, summary, created_at)
        values %s
        on conflict (llm_key) do update set
          summary = excluded.summary,
          created_at = excluded.created_at
    """
    with conn:
        with conn.cursor() as cur:
            from psycopg2.extras import execute_values
            execute_values(cur, sql, rows, page_size=1000)

def load_negative_summary_db(history_days: int):
    conn = get_db_conn()
    if conn is None:
        return None
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select date, company, ceo, article_type,
                           negative_count, crisis_risk_count, top_headlines
                    from negative_articles_summary_mv
                    where date >= (current_date - (%s || ' days')::interval)
                    order by date desc, company
                    """,
                    (history_days,),
                )
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
        return pd.DataFrame(rows, columns=cols)
    except Exception as exc:
        print(f"⚠️ DB summary load failed, falling back to CSV: {exc}")
        return None
    finally:
        conn.close()

def load_serp_counts_db(days: int):
    conn = get_db_conn()
    if conn is None:
        return {}, {}, {}, {}
    brand_uncontrolled = {}
    ceo_uncontrolled = {}
    brand_negative = {}
    ceo_negative = {}
    sql = """
        with brand_rows as (
            select c.name as company,
                   sum(case when coalesce(ov.override_sentiment_label, r.llm_sentiment_label, r.sentiment_label) = 'negative' then 1 else 0 end) as neg_count,
                   sum(case when coalesce(ov.override_sentiment_label, r.llm_sentiment_label, r.sentiment_label) = 'negative'
                             and coalesce(ov.override_control_class, r.llm_control_class, r.control_class) = 'uncontrolled'
                            then 1 else 0 end) as neg_uncontrolled
            from serp_runs sr
            join companies c on c.id = sr.company_id
            join serp_results r on r.serp_run_id = sr.id
            left join serp_result_overrides ov on ov.serp_result_id = r.id
            where sr.entity_type = 'company'
              and sr.run_at::date >= (current_date - (%s || ' days')::interval)
            group by c.name
        ),
        ceo_rows as (
            select c.name as company, ceo.name as ceo,
                   sum(case when coalesce(ov.override_sentiment_label, r.llm_sentiment_label, r.sentiment_label) = 'negative' then 1 else 0 end) as neg_count,
                   sum(case when coalesce(ov.override_sentiment_label, r.llm_sentiment_label, r.sentiment_label) = 'negative'
                             and coalesce(ov.override_control_class, r.llm_control_class, r.control_class) = 'uncontrolled'
                            then 1 else 0 end) as neg_uncontrolled
            from serp_runs sr
            join ceos ceo on ceo.id = sr.ceo_id
            join companies c on c.id = ceo.company_id
            join serp_results r on r.serp_run_id = sr.id
            left join serp_result_overrides ov on ov.serp_result_id = r.id
            where sr.entity_type = 'ceo'
              and sr.run_at::date >= (current_date - (%s || ' days')::interval)
            group by c.name, ceo.name
        )
        select 'brand'::text as kind, company, null::text as ceo, neg_count, neg_uncontrolled from brand_rows
        union all
        select 'ceo'::text as kind, company, ceo, neg_count, neg_uncontrolled from ceo_rows
    """
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, (max(1, days), max(1, days)))
                for kind, company, ceo, neg_count, neg_uncontrolled in cur.fetchall():
                    if kind == "brand":
                        if company:
                            brand_negative[company] = int(neg_count or 0)
                            brand_uncontrolled[company] = int(neg_uncontrolled or 0)
                    else:
                        key = (company, ceo)
                        ceo_negative[key] = int(neg_count or 0)
                        ceo_uncontrolled[key] = int(neg_uncontrolled or 0)
    except Exception as exc:
        print(f"⚠️ DB SERP load failed, falling back to CSV: {exc}")
        return {}, {}, {}, {}
    finally:
        conn.close()
    return brand_uncontrolled, ceo_uncontrolled, brand_negative, ceo_negative


def load_top_stories_counts_db(days: int):
    conn = get_db_conn()
    if conn is None:
        return {}, {}
    brand_counts = {}
    ceo_counts = {}
    sql = """
        select entity_type, entity_name,
               sum(total_count) as total_count,
               sum(negative_count) as negative_count
        from serp_feature_daily
        where feature_type = 'top_stories_items'
          and date >= (current_date - (%s || ' days')::interval)
        group by entity_type, entity_name
    """
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, (max(1, days),))
                for entity_type, entity_name, total_count, negative_count in cur.fetchall():
                    key = (entity_name or "").strip()
                    if not key:
                        continue
                    total = int(total_count or 0)
                    neg = int(negative_count or 0)
                    if entity_type in {"brand", "company"}:
                        brand_counts[key] = (total, neg)
                    elif entity_type == "ceo":
                        ceo_counts[key] = (total, neg)
    except Exception as exc:
        print(f"⚠️ DB top stories load failed: {exc}")
        return {}, {}
    finally:
        conn.close()
    return brand_counts, ceo_counts

def load_top_stories_items_db(days: int):
    conn = get_db_conn()
    if conn is None:
        return {}, {}
    brand_items = {}
    ceo_items = {}
    sql = """
        select sfi.date, sfi.entity_type, sfi.entity_name,
               sfi.title, sfi.url,
               coalesce(ov.override_sentiment_label, sfi.llm_sentiment_label, sfi.sentiment_label) as sentiment
        from serp_feature_items sfi
        left join serp_feature_item_overrides ov on ov.serp_feature_item_id = sfi.id
        where sfi.feature_type = 'top_stories_items'
          and sfi.date >= (current_date - (%s || ' days')::interval)
          and coalesce(ov.override_sentiment_label, sfi.llm_sentiment_label, sfi.sentiment_label) = 'negative'
        order by sfi.date, sfi.entity_name, sfi.position
    """
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, (max(1, days),))
                for dval, entity_type, entity_name, title, url, sentiment in cur.fetchall():
                    key_name = (entity_name or "").strip()
                    if not key_name:
                        continue
                    entry = {"title": title or "", "url": url or ""}
                    key = (dval.isoformat(), key_name)
                    if entity_type in {"brand", "company"}:
                        brand_items.setdefault(key, []).append(entry)
                    elif entity_type == "ceo":
                        ceo_items.setdefault(key, []).append(entry)
    except Exception as exc:
        print(f"⚠️ DB top stories items load failed: {exc}")
        return {}, {}
    finally:
        conn.close()
    return brand_items, ceo_items

def get_salesforce_owner(brand_name):
    if not brand_name: return None, None
    try:
        sf = Salesforce(username=SF_USERNAME, password=SF_PASSWORD, security_token=SF_TOKEN)
        
        # 1. Exact Match
        safe_name = brand_name.replace("'", "\\'")
        query = f"SELECT Name, Owner.Email, Owner.Name FROM Account WHERE Name = '{safe_name}' LIMIT 1"
        result = sf.query(query)
        if result['totalSize'] > 0:
            return result['records'][0]['Owner']['Email'], result['records'][0]['Owner']['Name']

        # 2. Token/Prefix Match
        core_name = normalize_name(brand_name)
        if len(core_name) < 2: return None, None
        
        safe_core = core_name.replace("'", "\\'")
        fuzzy_query = f"SELECT Name, Owner.Email, Owner.Name FROM Account WHERE Name LIKE '%{safe_core}%' LIMIT 10"
        fuzzy_result = sf.query(fuzzy_query)
        if fuzzy_result['totalSize'] == 0: return None, None
            
        candidates = fuzzy_result['records']
        core_lower = core_name.lower()
        
        for rec in candidates:
            sf_lower = rec['Name'].lower()
            if sf_lower == core_lower or sf_lower.startswith(core_lower + " "):
                return rec['Owner']['Email'], rec['Owner']['Name']
            if f" {core_lower} " in f" {sf_lower} ":
                return rec['Owner']['Email'], rec['Owner']['Name']

        # 3. Fuzzy Fallback
        candidate_names = [r['Name'] for r in candidates]
        best_matches = get_close_matches(brand_name, candidate_names, n=1, cutoff=0.6)
        if best_matches:
            match_rec = next(r for r in candidates if r['Name'] == best_matches[0])
            return match_rec['Owner']['Email'], match_rec['Owner']['Name']

        return None, None
    except Exception as e:
        print(f"⚠️ Salesforce lookup failed for {brand_name}: {e}")
        return None, None

def get_slack_user_id(email):
    if not email: return None
    try:
        url = "https://slack.com/api/users.lookupByEmail"
        headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}
        resp = requests.get(url, headers=headers, params={"email": email})
        data = resp.json()
        if data.get("ok"): return data['user']['id']
    except Exception: pass
    return None

def send_slack_alert(brand, ceo_name, article_type, count, p97_val, headlines, top_stories, owner_slack_id, owner_name, summary_text="", risk_score=None, channel=None):
    """Sends a Block Kit alert."""
    
    if article_type == 'ceo' and ceo_name and ceo_name != 'nan':
        alert_title = f"🧑🏻‍💼 CEO Crisis: {ceo_name}"
        sub_context = f"Company: {brand}"
        safe_filter = urllib.parse.quote(ceo_name)
        dashboard_url = f"https://news-sentiment-dashboard-yelv2pxzuq-uc.a.run.app/?tab=ceos&company={safe_filter}"
    else:
        alert_title = f"🏢 Brand Crisis: {brand}"
        if ceo_name and ceo_name.lower() != 'nan':
             sub_context = f"CEO: {ceo_name}"
        else:
             sub_context = "Category: Corporate Brand"
        safe_filter = urllib.parse.quote(brand)
        dashboard_url = f"https://news-sentiment-dashboard-yelv2pxzuq-uc.a.run.app/?tab=brands&company={safe_filter}"

    if owner_slack_id:
        mention_text = f"<@{owner_slack_id}>"
    elif owner_name:
        mention_text = f"{owner_name} (Email lookup failed)"
    else:
        mention_text = f"<@{FALLBACK_SLACK_ID}> (Salesforce Missing)"

    headline_text = ""
    if top_stories:
        for item in top_stories[:3]:
            title = (item.get("title") or "").strip().strip('"')
            url = (item.get("url") or "").strip()
            if title and url:
                headline_text += f"• <{url}|{title}>\n"
            elif title:
                headline_text += f"• {title}\n"
        if len(top_stories) > 3:
            headline_text += f"_...and {len(top_stories) - 3} more_"
    elif headlines:
        raw_heads = str(headlines).split('|')
        for hl in raw_heads[:3]:
            clean_hl = hl.strip().strip('"')
            headline_text += f"• {clean_hl}\n"
        if len(raw_heads) > 3:
            headline_text += f"_...and {len(raw_heads) - 3} more_"

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": alert_title,
                "emoji": True
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Attention:* {mention_text}\n*Context:* {sub_context} (View on <{dashboard_url}|Risk Dashboard>)"
            }
        },
        { "type": "divider" },
                *([{
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Summary:*\n{summary_text}"
            }
        }] if summary_text else []),
        { "type": "divider" },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Today's Negative Article Volume:*\n{count} Articles"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Normal Negative Article Baseline (P97):*\n< {p97_val:.1f} Articles"
                }
            ]
        },
        *([{
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Risk Score:*\n{risk_score:.1f}"
                }
            ]
        }] if risk_score is not None else []),
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Top Stories:*\n{headline_text}" if headline_text else "*Top Stories:*\n_No negative top stories found_"
            }
        },
        # {
        #     "type": "context",
        #     "elements": [
        #         {
        #             "type": "mrkdwn",
        #             "text": f"View analysis on the <{dashboard_url}|Risk Dashboard>."
        #         }
        #     ]
        # }
    ]

    alert_color = get_owner_color(owner_name)
    payload = {
        "channel": channel or SLACK_CHANNEL,
        "attachments": [
            {
                "color": alert_color,
                "blocks": blocks
            }
        ]
    }

    # --- DRY RUN CHECK ---
    if DRY_RUN:
        print(f"👀 [DRY RUN] Would send Slack alert for: {brand} (Owner: {owner_name})")
        return # <--- Stop here, do not run requests.post

    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
        json=payload
    )
    try:
        data = resp.json()
    except Exception:
        data = {"ok": False, "error": f"http_{resp.status_code}"}
    if data.get("ok"):
        print(f"✅ Alert sent for {alert_title}")
    else:
        err = data.get("error", "unknown_error")
        print(f"⚠️ Slack send failed for {alert_title}: {err}")
        raise RuntimeError(f"Slack send failed: {err}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', default='risk-dashboard')
    args = parser.parse_args()

    conn = get_db_conn()
    if conn is None:
        print("DATABASE_URL not set. Exiting.")
        return
    ensure_alert_tables(conn)
    try:
        if LLM_API_KEY:
            print(f"🤖 LLM enabled: provider={LLM_PROVIDER}, model={LLM_MODEL}")
        else:
            print("🤖 LLM disabled: missing LLM_API_KEY")
        if SERP_GATE_ENABLED:
            print(f"🧭 SERP gate: enabled (min={SERP_GATE_MIN}, days={SERP_GATE_DAYS})")
        else:
            print("🧭 SERP gate: disabled")

        # 1. Load Data (DB only)
        df = load_negative_summary_db(NEGATIVE_HISTORY_DAYS)
        if df is None or df.empty:
            print("No DB negative summary data found. Exiting.")
            return
        print("🗄️ Using DB summary for alerts.")

        # 2. Load History
        history = load_alert_history_db(conn)

        # --- CALCULATE DAILY BUDGET ---
        current_time = datetime.now()
        one_day_ago = current_time - timedelta(hours=20) # ensures a fresh 20 alert budget every morning.

        recent_alerts_count = 0
        for timestamp_str in history.values():
            try:
                t_alert = datetime.fromisoformat(timestamp_str)
                if t_alert > one_day_ago:
                    recent_alerts_count += 1
            except ValueError:
                pass

        alerts_remaining_today = MAX_ALERTS_PER_DAY - recent_alerts_count

        print(f"📉 Daily Alert Budget: {MAX_ALERTS_PER_DAY} total.")
        print(f"🕒 Used in last 24h: {recent_alerts_count}")
        print(f"✅ Remaining capacity: {alerts_remaining_today}")

        if alerts_remaining_today <= 0:
            print("⛔ Daily alert limit reached. Exiting script to prevent flood.")
            return

        # --- SORT BY PRIORITY ---
        # Sort by blended signal (negatives + crisis_risk)
        print("📊 Sorting data by severity (negative count)...")
        if 'negative_count' in df.columns:
            if 'crisis_risk_count' not in df.columns:
                df['crisis_risk_count'] = 0
            df['risk_signal'] = df['negative_count'].fillna(0) + (df['crisis_risk_count'].fillna(0) * RISK_LABEL_WEIGHT)
            df.sort_values(by='risk_signal', ascending=False, inplace=True)

        # --- CALCULATE THRESHOLDS & DATA MATURITY ---
        brand_stats = df[df['article_type'] == 'brand'].groupby('company')['negative_count'].agg(['count', lambda x: x.quantile(PERCENTILE_CUTOFF)]).to_dict('index')
        ceo_stats = df[df['article_type'] == 'ceo'].groupby('company')['negative_count'].agg(['count', lambda x: x.quantile(PERCENTILE_CUTOFF)]).to_dict('index')

        MIN_HISTORY_POINTS = 14
        HARD_FLOOR_NEW_CO = 15

        updates_made = False
        llm_cache = load_llm_cache_db(conn, current_time.date().isoformat())
        llm_calls = 0

        serp_brand_counts = {}
        serp_ceo_counts = {}
        serp_brand_neg_counts = {}
        serp_ceo_neg_counts = {}
        top_stories_brand = {}
        top_stories_ceo = {}
        top_stories_brand_items = {}
        top_stories_ceo_items = {}
        if SERP_GATE_ENABLED:
            b_unctrl, c_unctrl, b_neg, c_neg = load_serp_counts_db(SERP_GATE_DAYS)
            serp_brand_counts = b_unctrl
            serp_ceo_counts = c_unctrl
            top_stories_brand, top_stories_ceo = load_top_stories_counts_db(SERP_GATE_DAYS)
            top_stories_brand_items, top_stories_ceo_items = load_top_stories_items_db(SERP_GATE_DAYS)

        for _, row in df.iterrows():
            # FLOOD PROTECTION CHECK
            if alerts_remaining_today <= 0:
                print("🛑 Daily limit hit mid-run. Stopping alerts for today.")
                break

            brand = row['company']
            count = row['negative_count']
            headlines = row['top_headlines']

            article_type = str(row.get('article_type', 'brand')).lower().strip()
            ceo_name = str(row.get('ceo', '')).strip()
            if article_type == "brand" and not ALERT_BRANDS:
                continue
            if article_type == "ceo" and not ALERT_CEOS:
                continue

            # A. STRICT DATE FILTER (Last 48 Hours Only)
            date_str = str(row['date'])
            try:
                row_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            except Exception:
                continue

            server_now = datetime.now().date()
            if row_date != server_now and row_date != server_now - timedelta(days=1):
                continue

            # B. DYNAMIC THRESHOLD CHECK (disabled)
            # stats_lookup = ceo_stats if article_type == 'ceo' else brand_stats
            # company_stats = stats_lookup.get(brand, {'count': 0, '<lambda_0>': 0})
            # history_points = company_stats['count']
            # p97 = company_stats['<lambda_0>']
            # if history_points >= MIN_HISTORY_POINTS:
            #     if count < MIN_NEGATIVE_ARTICLES: continue
            #     if count < p97: continue
            #     threshold_msg = f"P97 ({p97:.1f})"
            #     baseline_val = p97
            # else:
            #     if count < HARD_FLOOR_NEW_CO: continue
            #     threshold_msg = f"Hard Floor ({HARD_FLOOR_NEW_CO})"
            #     baseline_val = HARD_FLOOR_NEW_CO
            # if SERP_GATE_DEBUG:
            #     print(f"   [Gate] {brand} ({article_type}) neg_articles={count} baseline={threshold_msg}")

            threshold_msg = "SERP + Top Stories"
            baseline_val = 0

            # B2. SERP CONFIRMATION GATE
            serp_count = 0
            if SERP_GATE_ENABLED:
                if article_type == 'ceo':
                    serp_count = serp_ceo_counts.get((brand, ceo_name), 0)
                    top_total, top_neg = top_stories_ceo.get(ceo_name, (0, 0))
                else:
                    serp_count = serp_brand_counts.get(brand, 0)
                    top_total, top_neg = top_stories_brand.get(brand, (0, 0))
                if SERP_GATE_DEBUG:
                    print(f"   [Gate] {brand} ({article_type}) serp_uncontrolled={serp_count} top_total={top_total} top_neg={top_neg}")
                if SERP_TOP_STORIES_REQUIRED and top_total <= 0:
                    if SERP_GATE_DEBUG:
                        print(f"   [Gate] Skipping {brand} ({article_type}) - no Top Stories")
                    continue
                if top_neg < SERP_TOP_STORIES_NEG_MIN:
                    if SERP_GATE_DEBUG:
                        print(f"   [Gate] Skipping {brand} ({article_type}) - Top Stories neg={top_neg}")
                    continue
                if serp_count < SERP_GATE_MIN:
                    if SERP_GATE_DEBUG:
                        print(f"   [Gate] Skipping {brand} ({article_type}) - SERP neg+uncontrolled={serp_count}")
                    continue
            elif article_type == 'ceo':
                serp_count = serp_ceo_counts.get((brand, ceo_name), 0)
            else:
                serp_count = serp_brand_counts.get(brand, 0)

            # C. COOLDOWN CHECK
            history_key = f"{brand}_{article_type}"
            last_alert = history.get(history_key)

            if not last_alert and article_type == 'brand':
                last_alert = history.get(brand)

            if last_alert:
                last_date = datetime.fromisoformat(last_alert)
                if current_time - last_date < timedelta(hours=ALERT_COOLDOWN_HOURS):
                    # Silent skip
                    continue

            # --- TRIGGER ALERT ---
            crisis_count = int(row.get('crisis_risk_count', 0) or 0)
            print(f"🚀 Alert: {history_key} | Vol: {count} | Threshold: {threshold_msg}")

            owner_email, owner_name = get_salesforce_owner(brand)
            slack_id = get_slack_user_id(owner_email)

            summary_text = ""
            llm_key = f"{brand}|{ceo_name}|{article_type}|{date_str}"
            if LLM_API_KEY and llm_calls < LLM_SUMMARY_MAX_CALLS:
                if llm_key in llm_cache:
                    summary_text = llm_cache.get(llm_key, "")
                else:
                    if article_type == "ceo":
                        top_items = top_stories_ceo_items.get((date_str, ceo_name), [])
                    else:
                        top_items = top_stories_brand_items.get((date_str, brand), [])
                    top_titles = [i.get("title", "").strip().strip('"') for i in top_items if i.get("title")]
                    if not top_titles:
                        raw_heads = str(headlines).split('|')
                        top_titles = [h.strip().strip('"') for h in raw_heads if h.strip()]
                    prompt = build_summary_prompt(
                        article_type,
                        ceo_name if article_type == "ceo" else brand,
                        top_titles[:5]
                    )
                    summary_text = call_llm_text(prompt, LLM_API_KEY, LLM_MODEL)
                    llm_cache[llm_key] = summary_text
                    llm_calls += 1

            if article_type == "ceo":
                top_items = top_stories_ceo_items.get((date_str, ceo_name), [])
            else:
                top_items = top_stories_brand_items.get((date_str, brand), [])

            send_slack_alert(
                brand, ceo_name, article_type, count, baseline_val,
                headlines, top_items, slack_id, owner_name, summary_text, None
            )

            # --- JITTER IMPLEMENTATION ---
            jitter_seconds = random.randint(0, 6 * 3600)
            effective_timestamp = current_time + timedelta(seconds=jitter_seconds)

            if DRY_RUN:
                print(f"   [Test] Jitter applied: {jitter_seconds/3600:.1f} hours.")
                print(f"   [Test] Next unlock time would be: {effective_timestamp}")
            else:
                history[history_key] = effective_timestamp.isoformat()

            updates_made = True

            # Decrement Budget
            alerts_remaining_today -= 1

            # --- DRY RUN SLEEP ---
            # Don't sleep for 2 seconds in testing, it's annoying.
            if not DRY_RUN:
                time.sleep(2)

        # --- SAVE CHECK ---
        if updates_made:
            if DRY_RUN:
                print("🚫 [DRY RUN] Skipping DB updates. No changes made.")
            else:
                upsert_alert_history_db(conn, history)
                upsert_llm_cache_db(conn, llm_cache)
                print("💾 Alert history + LLM cache updated.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
