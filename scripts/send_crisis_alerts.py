#!/usr/bin/env python3
"""
Reads aggregated negative articles and sends Slack alerts to Salesforce account owners.
Tracks alert history in GCS to prevent spamming.
Implements Dynamic Thresholding, Type-Specific Alerts, DAILY VOLUME CAPS, and JITTER.
"""

import os
import json
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
from storage_utils import CloudStorageManager
from llm_utils import build_summary_prompt, call_llm_text

# --- CONFIG ---
DRY_RUN = True  # <--- SET TO TRUE FOR TESTING
SLACK_BOT_TOKEN = os.getenv('SLACK_BOT_TOKEN')
SF_USERNAME = os.getenv('SF_USERNAME')
SF_PASSWORD = os.getenv('SF_PASSWORD')
SF_TOKEN = os.getenv('SF_SECURITY_TOKEN')
SLACK_CHANNEL = "#crisis-alerts" 
FALLBACK_SLACK_ID = "UT1EC3ENR" 
LLM_API_KEY = os.getenv("LLM_API_KEY", "")
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openai")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")
LLM_SUMMARY_MAX_CALLS = int(os.getenv("LLM_SUMMARY_MAX_CALLS", "20"))
LLM_CACHE_DIR = "data/llm_cache"
NEGATIVE_HISTORY_DAYS = int(os.getenv("NEGATIVE_HISTORY_DAYS", "180"))
RISK_LABEL_WEIGHT = float(os.getenv("RISK_LABEL_WEIGHT", "3"))

# SERP gating (negative + uncontrolled)
SERP_GATE_ENABLED = os.getenv("SERP_GATE_ENABLED", "1") == "1"
SERP_GATE_MIN = int(os.getenv("SERP_GATE_MIN", "2"))
SERP_GATE_DAYS = int(os.getenv("SERP_GATE_DAYS", "2"))
SERP_GATE_DEBUG = os.getenv("SERP_GATE_DEBUG", "1") == "1"

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


def parse_bool(value):
    return str(value).strip().lower() in {"true", "1", "yes", "y", "controlled"}

def get_db_conn():
    dsn = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not dsn:
        return None
    return psycopg2.connect(dsn)

def load_negative_summary_db(history_days: int):
    conn = get_db_conn()
    if conn is None:
        return None
    sql = """
        with company_rows as (
            select m.scored_at::date as date,
                   c.name as company,
                   null::text as ceo,
                   'brand' as article_type,
                   count(*) filter (
                       where coalesce(ov.override_sentiment_label, m.llm_sentiment_label, m.sentiment_label) = 'negative'
                   ) as negative_count,
                   count(*) filter (where m.llm_risk_label = 'crisis_risk') as crisis_risk_count,
                   array_to_string(
                       (array_agg(a.title order by a.title) filter (
                           where coalesce(ov.override_sentiment_label, m.llm_sentiment_label, m.sentiment_label) = 'negative'
                       ))[1:3],
                       ' | '
                   ) as top_headlines
            from company_article_mentions m
            join companies c on c.id = m.company_id
            join articles a on a.id = m.article_id
            left join company_article_overrides ov
              on ov.company_id = m.company_id and ov.article_id = m.article_id
            where m.scored_at >= (current_date - (%s || ' days')::interval)
            group by m.scored_at::date, c.name
        ),
        ceo_rows as (
            select m.scored_at::date as date,
                   c.name as company,
                   ceo.name as ceo,
                   'ceo' as article_type,
                   count(*) filter (
                       where coalesce(ov.override_sentiment_label, m.llm_sentiment_label, m.sentiment_label) = 'negative'
                   ) as negative_count,
                   count(*) filter (where m.llm_risk_label = 'crisis_risk') as crisis_risk_count,
                   array_to_string(
                       (array_agg(a.title order by a.title) filter (
                           where coalesce(ov.override_sentiment_label, m.llm_sentiment_label, m.sentiment_label) = 'negative'
                       ))[1:3],
                       ' | '
                   ) as top_headlines
            from ceo_article_mentions m
            join ceos ceo on ceo.id = m.ceo_id
            join companies c on c.id = ceo.company_id
            join articles a on a.id = m.article_id
            left join ceo_article_overrides ov
              on ov.ceo_id = m.ceo_id and ov.article_id = m.article_id
            where m.scored_at >= (current_date - (%s || ' days')::interval)
            group by m.scored_at::date, c.name, ceo.name
        )
        select * from company_rows
        union all
        select * from ceo_rows
        order by date desc, company
    """
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, (history_days, history_days))
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
        return pd.DataFrame(rows, columns=cols)
    except Exception as exc:
        print(f"⚠️ DB summary load failed, falling back to CSV: {exc}")
        return None
    finally:
        conn.close()

def load_serp_counts(storage, date_str):
    brand_counts = {}
    ceo_counts = {}

    brand_path = f"data/processed_serps/{date_str}-brand-serps-modal.csv"
    ceo_path = f"data/processed_serps/{date_str}-ceo-serps-modal.csv"

    if storage.file_exists(brand_path):
        df = storage.read_csv(brand_path)
        if not df.empty:
            df["sentiment"] = df.get("sentiment", "").astype(str).str.lower()
            df["controlled"] = df.get("controlled", False).apply(parse_bool)
            mask = (df["sentiment"] == "negative") & (~df["controlled"])
            brand_counts = df[mask].groupby("company").size().to_dict()

    if storage.file_exists(ceo_path):
        df = storage.read_csv(ceo_path)
        if not df.empty:
            df["sentiment"] = df.get("sentiment", "").astype(str).str.lower()
            df["controlled"] = df.get("controlled", False).apply(parse_bool)
            mask = (df["sentiment"] == "negative") & (~df["controlled"])
            ceo_counts = df[mask].groupby(["company", "ceo"]).size().to_dict()

    return brand_counts, ceo_counts

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

def send_slack_alert(brand, ceo_name, article_type, count, p97_val, headlines, owner_slack_id, owner_name, summary_text="", risk_score=None):
    """Sends a Block Kit alert."""
    
    if article_type == 'ceo' and ceo_name and ceo_name != 'nan':
        alert_title = f"🚨 CEO Crisis: {ceo_name}"
        sub_context = f"Company: {brand}"
        safe_filter = urllib.parse.quote(ceo_name)
        dashboard_url = f"https://news-sentiment-dashboard-yelv2pxzuq-uc.a.run.app/?tab=ceos&company={safe_filter}"
    else:
        alert_title = f"🚨 Brand Crisis: {brand}"
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
    if headlines:
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
                "text": f"*Top Headlines:*\n{headline_text}"
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
        "channel": SLACK_CHANNEL,
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

    requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
        json=payload
    )
    print(f"✅ Alert sent for {alert_title}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', default='risk-dashboard')
    args = parser.parse_args()

    storage = CloudStorageManager(args.bucket)
    if LLM_API_KEY:
        print(f"🤖 LLM enabled: provider={LLM_PROVIDER}, model={LLM_MODEL}")
    else:
        print("🤖 LLM disabled: missing LLM_API_KEY")
    if SERP_GATE_ENABLED:
        print(f"🧭 SERP gate: enabled (min={SERP_GATE_MIN}, days={SERP_GATE_DAYS})")
    else:
        print("🧭 SERP gate: disabled")
    
    # 1. Load Data
    summary_path = "data/daily_counts/negative-articles-summary.csv"
    df = load_negative_summary_db(NEGATIVE_HISTORY_DAYS)
    if df is None or df.empty:
        if not storage.file_exists(summary_path):
            print("No negative summary file found. Exiting.")
            return
        print("📄 Using CSV summary fallback (GCS).")
        df = storage.read_csv(summary_path)
    else:
        print("🗄️ Using DB summary for alerts.")
    
    # 2. Load History
    history_path = "data/alert_history.json"
    history = {}
    if storage.file_exists(history_path):
        try:
            history = json.loads(storage.read_text(history_path))
        except:
            print("Could not read history, starting fresh.")

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
    llm_cache_path = f"{LLM_CACHE_DIR}/{current_time.date()}-crisis-summary.json"
    if storage.file_exists(llm_cache_path):
        try:
            llm_cache = json.loads(storage.read_text(llm_cache_path))
        except Exception:
            llm_cache = {}
    else:
        llm_cache = {}
    llm_calls = 0

    serp_brand_counts = {}
    serp_ceo_counts = {}
    if SERP_GATE_ENABLED:
        for delta in range(SERP_GATE_DAYS):
            dstr = (current_time.date() - timedelta(days=delta)).strftime('%Y-%m-%d')
            b_counts, c_counts = load_serp_counts(storage, dstr)
            for company, count in b_counts.items():
                serp_brand_counts[company] = serp_brand_counts.get(company, 0) + int(count)
            for key, count in c_counts.items():
                serp_ceo_counts[key] = serp_ceo_counts.get(key, 0) + int(count)
    
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

        # A. STRICT DATE FILTER (Last 48 Hours Only)
        date_str = str(row['date']) 
        try:
            row_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        except:
            continue
            
        server_now = datetime.now().date()
        if row_date != server_now and row_date != server_now - timedelta(days=1):
            continue

        # B. DYNAMIC THRESHOLD CHECK
        stats_lookup = ceo_stats if article_type == 'ceo' else brand_stats
        company_stats = stats_lookup.get(brand, {'count': 0, '<lambda_0>': 0})
        
        history_points = company_stats['count']
        p97 = company_stats['<lambda_0>']
        
        if history_points >= MIN_HISTORY_POINTS:
            if count < MIN_NEGATIVE_ARTICLES: continue
            if count < p97: continue
            threshold_msg = f"P97 ({p97:.1f})"
            baseline_val = p97
        else:
            if count < HARD_FLOOR_NEW_CO: continue
            threshold_msg = f"Hard Floor ({HARD_FLOOR_NEW_CO})"
            baseline_val = HARD_FLOOR_NEW_CO

        # B2. SERP CONFIRMATION GATE
        serp_count = 0
        if SERP_GATE_ENABLED:
            if article_type == 'ceo':
                serp_count = serp_ceo_counts.get((brand, ceo_name), 0)
            else:
                serp_count = serp_brand_counts.get(brand, 0)
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
        news_excess = max(0, count - baseline_val)
        risk_score = float(news_excess + (serp_count * 2) + (crisis_count * RISK_LABEL_WEIGHT))
        print(f"🚀 Alert: {history_key} | Vol: {count} | Threshold: {threshold_msg} | Risk Score: {risk_score:.1f}")
        
        owner_email, owner_name = get_salesforce_owner(brand)
        slack_id = get_slack_user_id(owner_email)
        
        summary_text = ""
        llm_key = f"{brand}|{ceo_name}|{article_type}|{date_str}"
        if LLM_API_KEY and llm_calls < LLM_SUMMARY_MAX_CALLS:
            if llm_key in llm_cache:
                summary_text = llm_cache.get(llm_key, "")
            else:
                raw_heads = str(headlines).split('|')
                clean_heads = [h.strip().strip('"') for h in raw_heads if h.strip()]
                prompt = build_summary_prompt(article_type, ceo_name if article_type == "ceo" else brand, clean_heads[:5])
                summary_text = call_llm_text(prompt, LLM_API_KEY, LLM_MODEL)
                llm_cache[llm_key] = summary_text
                llm_calls += 1

        send_slack_alert(
            brand, ceo_name, article_type, count, baseline_val, 
            headlines, slack_id, owner_name, summary_text, risk_score
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
            print("🚫 [DRY RUN] Skipping save to 'alert_history.json'. No changes made.")
        else:
            storage.write_text(json.dumps(history, indent=2), history_path)
            storage.write_text(json.dumps(llm_cache, indent=2), llm_cache_path)
            print("💾 Alert history updated.")

if __name__ == "__main__":
    main()
