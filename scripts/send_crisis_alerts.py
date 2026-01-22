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
from storage_utils import CloudStorageManager

# --- CONFIG ---
DRY_RUN = False  # <--- SET TO TRUE FOR TESTING
SLACK_BOT_TOKEN = os.getenv('SLACK_BOT_TOKEN')
SF_USERNAME = os.getenv('SF_USERNAME')
SF_PASSWORD = os.getenv('SF_PASSWORD')
SF_TOKEN = os.getenv('SF_SECURITY_TOKEN')
SLACK_CHANNEL = "#crisis-alerts" 
FALLBACK_SLACK_ID = "UT1EC3ENR" 

# Configurable Floors
MIN_NEGATIVE_ARTICLES = 13
PERCENTILE_CUTOFF = 0.97
ALERT_COOLDOWN_HOURS = 168

# --- FLOOD PROTECTION ---
MAX_ALERTS_PER_DAY = 5  # Strict limit: Max 20 alerts per 24-hour rolling window

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

def send_slack_alert(brand, ceo_name, article_type, count, p97_val, headlines, owner_slack_id, owner_name):
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
                "text": f"*Attention:* {mention_text}\n*Context:* {sub_context}"
            }
        },
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

    alert_color = get_owner_color(owner_name)
    payload = {
        "channel": SLACK_CHANNEL,
        "text": alert_title,
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
    # Sort by 'negative_count' descending so we prioritize the BIGGEST crises first
    print("📊 Sorting data by severity (negative count)...")
    if 'negative_count' in df.columns:
        df.sort_values(by='negative_count', ascending=False, inplace=True)

    # --- CALCULATE THRESHOLDS & DATA MATURITY ---
    brand_stats = df[df['article_type'] == 'brand'].groupby('company')['negative_count'].agg(['count', lambda x: x.quantile(PERCENTILE_CUTOFF)]).to_dict('index')
    ceo_stats = df[df['article_type'] == 'ceo'].groupby('company')['negative_count'].agg(['count', lambda x: x.quantile(PERCENTILE_CUTOFF)]).to_dict('index')

    MIN_HISTORY_POINTS = 14   
    HARD_FLOOR_NEW_CO = 15    

    updates_made = False
    
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
        else:
            if count < HARD_FLOOR_NEW_CO: continue
            threshold_msg = f"Hard Floor ({HARD_FLOOR_NEW_CO})"

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
        print(f"🚀 Alert: {history_key} | Vol: {count} | Threshold: {threshold_msg}")
        
        owner_email, owner_name = get_salesforce_owner(brand)
        slack_id = get_slack_user_id(owner_email)
        
        send_slack_alert(
            brand, ceo_name, article_type, count, p97 if history_points >= MIN_HISTORY_POINTS else HARD_FLOOR_NEW_CO, 
            headlines, slack_id, owner_name
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
            print("💾 Alert history updated.")

if __name__ == "__main__":
    main()
