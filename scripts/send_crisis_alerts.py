#!/usr/bin/env python3
"""
Reads aggregated negative articles and sends Slack alerts to Salesforce account owners.
Tracks alert history in GCS to prevent spamming.
Implements Dynamic Thresholding & Type-Specific Alerts (Brand vs CEO).
"""

import os
import json
import argparse
import requests
import urllib.parse
import re
import time
from difflib import get_close_matches
from datetime import datetime, timedelta
from simple_salesforce import Salesforce
from storage_utils import CloudStorageManager

# --- CONFIG ---
SLACK_BOT_TOKEN = os.getenv('SLACK_BOT_TOKEN')
SF_USERNAME = os.getenv('SF_USERNAME')
SF_PASSWORD = os.getenv('SF_PASSWORD')
SF_TOKEN = os.getenv('SF_SECURITY_TOKEN')
SLACK_CHANNEL = "#crisis-alerts" 
FALLBACK_SLACK_ID = "UT1EC3ENR" 

# Configurable Floors
MIN_NEGATIVE_ARTICLES = 13
PERCENTILE_CUTOFF = 0.95
ALERT_COOLDOWN_HOURS = 168  

def normalize_name(name):
    """Strips legal suffixes to find the 'core' brand name."""
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

def get_salesforce_owner(brand_name):
    """Finds account owner using Exact -> Token -> Fuzzy matching."""
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

def send_slack_alert(brand, ceo_name, article_type, count, p95_val, headlines, owner_slack_id, owner_name):
    """
    Sends a Block Kit alert. 
    Customizes title/link based on whether it's a CEO or Brand crisis.
    """
    
    # 1. Customize Content based on Type
    if article_type == 'ceo' and ceo_name and ceo_name != 'nan':
        # CEO CRISIS MODE
        alert_title = f"🚨 CEO Crisis: {ceo_name}"
        sub_context = f"Company: {brand}"
        # Point to the CEO tab
        safe_filter = urllib.parse.quote(ceo_name)
        dashboard_url = f"https://news-sentiment-dashboard-yelv2pxzuq-uc.a.run.app/?tab=ceos&company={safe_filter}"
    else:
        # BRAND CRISIS MODE
        alert_title = f"🚨 Brand Crisis: {brand}"
        sub_context = "Category: Corporate Brand"
        # Point to the Brand tab
        safe_filter = urllib.parse.quote(brand)
        dashboard_url = f"https://news-sentiment-dashboard-yelv2pxzuq-uc.a.run.app/?tab=brands&company={safe_filter}"

    # 2. Determine Recipient
    if owner_slack_id:
        mention_text = f"<@{owner_slack_id}>"
    elif owner_name:
        mention_text = f"{owner_name} (Email lookup failed)"
    else:
        mention_text = f"<@{FALLBACK_SLACK_ID}> (Salesforce Missing)"

    # 3. Format Headlines
    headline_text = ""
    if headlines:
        raw_heads = str(headlines).split('|')
        for hl in raw_heads[:3]: 
            clean_hl = hl.strip().strip('"')
            headline_text += f"• {clean_hl}\n"
        if len(raw_heads) > 3:
            headline_text += f"_...and {len(raw_heads) - 3} more_"

    # 4. Construct Blocks
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
                "text": f"*Attention:* {mention_text}\n*Context:* {sub_context}"
            }
        },
        { "type": "divider" },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Today's Volume:*\n{count} Articles"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Normal Baseline (P95):*\n< {p95_val:.1f} Articles"
                }
            ]
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Top Headlines:*\n{headline_text}"
            }
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"View analysis on the <{dashboard_url}|Risk Dashboard>."
                }
            ]
        }
    ]

    requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
        json={"channel": SLACK_CHANNEL, "text": alert_title, "blocks": blocks}
    )
    print(f"✅ Alert sent for {alert_title}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', default='risk-dashboard')
    args = parser.parse_args()

    storage = CloudStorageManager(args.bucket)
    
    # 1. Load Data
    summary_path = "data/daily_counts/negative-articles-summary.csv"
    if not storage.file_exists(summary_path):
        print("No negative summary file found. Exiting.")
        return
    
    df = storage.read_csv(summary_path)
    
    # 2. Load History
    history_path = "data/alert_history.json"
    history = {}
    if storage.file_exists(history_path):
        try:
            history = json.loads(storage.read_text(history_path))
        except:
            print("Could not read history, starting fresh.")

    # --- CALCULATE THRESHOLDS & DATA MATURITY ---
    print("📊 Calculating thresholds...")
    
    # We calculate counts to know if we have enough history to trust the P95
    # If a company appears < 10 times, P95 is statistically noisy.
    brand_stats = df[df['article_type'] == 'brand'].groupby('company')['negative_count'].agg(['count', lambda x: x.quantile(PERCENTILE_CUTOFF)]).to_dict('index')
    ceo_stats = df[df['article_type'] == 'ceo'].groupby('company')['negative_count'].agg(['count', lambda x: x.quantile(PERCENTILE_CUTOFF)]).to_dict('index')

    # Config for "New" Companies (Low History)
    MIN_HISTORY_POINTS = 14   # Need 14 days of data to trust P95
    HARD_FLOOR_NEW_CO = 20    # If < 14 days history, require 15 articles to alert (Stricter)

    current_time = datetime.now()
    updates_made = False
    
    for _, row in df.iterrows():
        brand = row['company']
        count = row['negative_count']
        headlines = row['top_headlines']
        
        article_type = str(row.get('article_type', 'brand')).lower().strip()
        ceo_name = str(row.get('ceo', '')).strip()

        # A. STRICT DATE FILTER (Last 48 Hours Only)
        date_str = str(row['date']) 
        row_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        
        # NOTE: Using server time. If simulating 2026 data on 2025 server, 
        # you must hardcode this like: server_now = datetime(2026, 1, 13).date()
        server_now = datetime.now().date()
        
        if row_date != server_now and row_date != server_now - timedelta(days=1):
            continue

        # B. DYNAMIC THRESHOLD CHECK
        # Retrieve stats based on type
        stats_lookup = ceo_stats if article_type == 'ceo' else brand_stats
        company_stats = stats_lookup.get(brand, {'count': 0, '<lambda_0>': 0})
        
        history_points = company_stats['count']
        p95 = company_stats['<lambda_0>']

        # LOGIC:
        # 1. If we have lots of history (>= 14 days), use the P95 and the normal floor (5).
        # 2. If history is thin (< 14 days), ignore P95 and use the STRICT Hard Floor (15).
        
        if history_points >= MIN_HISTORY_POINTS:
            # Mature Data: Use P95 + Standard Floor
            if count < MIN_NEGATIVE_ARTICLES: continue
            if count < p95: continue
            threshold_msg = f"P95 ({p95:.1f})"
        else:
            # Immature Data: Use Strict Floor Only
            if count < HARD_FLOOR_NEW_CO: continue
            threshold_msg = f"Hard Floor ({HARD_FLOOR_NEW_CO})"

        # C. COOLDOWN CHECK
        history_key = f"{brand}_{article_type}"
        last_alert = history.get(history_key)
        
        # Migration check for old keys
        if not last_alert and article_type == 'brand':
            last_alert = history.get(brand)

        if last_alert:
            last_date = datetime.fromisoformat(last_alert)
            if current_time - last_date < timedelta(hours=ALERT_COOLDOWN_HOURS):
                print(f"Skipping {history_key} (Cooling down)")
                continue

        # --- TRIGGER ALERT ---
        print(f"🚀 Alert: {history_key} | Vol: {count} | Threshold: {threshold_msg} | Hist: {history_points} days")
        
        owner_email, owner_name = get_salesforce_owner(brand)
        slack_id = get_slack_user_id(owner_email)
        
        send_slack_alert(
            brand, ceo_name, article_type, count, p95 if history_points >= MIN_HISTORY_POINTS else HARD_FLOOR_NEW_CO, 
            headlines, slack_id, owner_name
        )
        
        history[history_key] = current_time.isoformat()
        updates_made = True
        time.sleep(2) 

    if updates_made:
        storage.write_text(json.dumps(history, indent=2), history_path)
        print("💾 Alert history updated.")

if __name__ == "__main__":
    main()
