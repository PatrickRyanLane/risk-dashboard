#!/usr/bin/env python3
"""
Reads aggregated negative articles and sends Slack alerts to Salesforce account owners.
Tracks alert history in GCS to prevent spamming.
Implements Dynamic Thresholding: Alerts only on 80th percentile spikes with min volume of 3.
"""

import os
import json
import argparse
import requests
import pandas as pd
from datetime import datetime, timedelta
from simple_salesforce import Salesforce
from storage_utils import CloudStorageManager
import re
from difflib import get_close_matches

# --- CONFIG ---
SLACK_BOT_TOKEN = os.getenv('SLACK_BOT_TOKEN')
SF_USERNAME = os.getenv('SF_USERNAME')
SF_PASSWORD = os.getenv('SF_PASSWORD')
SF_TOKEN = os.getenv('SF_SECURITY_TOKEN')
SLACK_CHANNEL = "#crisis-alerts" 
FALLBACK_SLACK_ID = "UT1EC3ENR" 

# Configurable Floors
MIN_NEGATIVE_ARTICLES = 3  # The absolute floor (user requirement: ">= 3 total articles")
PERCENTILE_CUTOFF = 0.80   # The relative threshold (80th percentile)
ALERT_COOLDOWN_HOURS = 168  

def normalize_name(name):
    """
    Strips legal suffixes to find the 'core' brand name.
    Example: "Apple Inc." -> "Apple"
    """
    if not name: return ""
    name = str(name).strip()
    
    # Common suffixes to remove (case insensitive)
    suffixes = [
        ' Inc.', ' Inc', ' Corporation', ' Corp.', ' Corp', 
        ' Company', ' Co.', ' Co', ' LLC', ' L.L.C.', ' Ltd.', ' Ltd', 
        ' PLC', ' plc', ' Group', ' Holdings', ' .com'
    ]
    
    # Sort by length (desc) so we catch "L.L.C." before "L.C."
    for suffix in sorted(suffixes, key=len, reverse=True):
        if name.lower().endswith(suffix.lower()):
            name = name[:-len(suffix)].strip()
            break
            
    # Remove special chars (keep spaces/alphanumeric)
    name = re.sub(r'[^\w\s]', '', name)
    return name

def get_salesforce_owner(brand_name):
    """
    Finds account owner with a 2-step lookup (Exact -> Fuzzy).
    """
    if not brand_name: return None, None
    
    try:
        sf = Salesforce(username=SF_USERNAME, password=SF_PASSWORD, security_token=SF_TOKEN)
        
        # ---------------------------------------------------------
        # ATTEMPT 1: Exact Match (The "Perfect" Match)
        # ---------------------------------------------------------
        # Escape single quotes in brand names (e.g., "McDonald's")
        safe_name = brand_name.replace("'", "\\'")
        query = f"SELECT Name, Owner.Email, Owner.Name FROM Account WHERE Name = '{safe_name}' LIMIT 1"
        result = sf.query(query)
        
        if result['totalSize'] > 0:
            owner = result['records'][0]['Owner']
            print(f"   ✅ Found exact match in Salesforce: {result['records'][0]['Name']}")
            return owner['Email'], owner['Name']

        # ---------------------------------------------------------
        # ATTEMPT 2: Fuzzy/Core Match (The "Smart" Match)
        # ---------------------------------------------------------
        core_name = normalize_name(brand_name)
        if len(core_name) < 3: 
            # Too short to fuzzy match safely (e.g. "GE") -> Give up
            print(f"   ⚠️ Exact match failed and name '{core_name}' too short for fuzzy search.")
            return None, None
            
        print(f"   🔄 Exact match failed. Trying fuzzy search for '{core_name}'...")
        
        # Search for any account *containing* the core name
        # We fetch up to 5 candidates to pick the best one
        safe_core = core_name.replace("'", "\\'")
        fuzzy_query = f"SELECT Name, Owner.Email, Owner.Name FROM Account WHERE Name LIKE '%{safe_core}%' LIMIT 5"
        fuzzy_result = sf.query(fuzzy_query)
        
        if fuzzy_result['totalSize'] == 0:
            return None, None
            
        # Python Logic: Find the closest string match among the candidates
        # This prevents searching for "Apple" and accidentally matching "Pineapple Corp"
        candidate_names = [rec['Name'] for rec in fuzzy_result['records']]
        best_matches = get_close_matches(brand_name, candidate_names, n=1, cutoff=0.6)
        
        if best_matches:
            best_name = best_matches[0]
            # Find the record that corresponds to this name
            match_rec = next(r for r in fuzzy_result['records'] if r['Name'] == best_name)
            owner = match_rec['Owner']
            print(f"   ✅ Fuzzy match success: '{brand_name}' matched to '{best_name}'")
            return owner['Email'], owner['Name']

        return None, None

    except Exception as e:
        print(f"⚠️ Salesforce lookup failed for {brand_name}: {e}")
        return None, None

def get_slack_user_id(email):
    """Exchanges email for Slack User ID to allow tagging."""
    if not email: return None
    try:
        url = "https://slack.com/api/users.lookupByEmail"
        headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}
        resp = requests.get(url, headers=headers, params={"email": email})
        data = resp.json()
        if data.get("ok"):
            return data['user']['id']
    except Exception as e:
        print(f"⚠️ Slack lookup failed: {e}")
    return None

def send_slack_alert(brand, count, p80, headlines, owner_slack_id, owner_name):
    """
    Sends a structured Block Kit message to Slack.
    """
    
    # 1. Determine who to tag
    if owner_slack_id:
        mention_text = f"<@{owner_slack_id}>"
    elif owner_name:
        mention_text = f"{owner_name} (Email lookup failed)"
    else:
        mention_text = f"<@{FALLBACK_SLACK_ID}> (Salesforce Missing)"

    # 2. Format Headlines (Bullet points)
    # Truncate to avoid making the message too long
    headline_text = ""
    if headlines:
        raw_heads = str(headlines).split('|')
        for hl in raw_heads[:3]: 
            # Clean up potential double quotes or extra whitespace
            clean_hl = hl.strip().strip('"')
            headline_text += f"• {clean_hl}\n"
        
        if len(raw_heads) > 3:
            headline_text += f"_...and {len(raw_heads) - 3} more_"

    # 3. Construct the "Block Kit" Payload
    # This creates a visually rich card instead of just text
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🚨 Crisis Alert: {brand}",
                "emoji": True
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Attention:* {mention_text}\n*Status:* :chart_with_upwards_trend: Unusual Negative Surge Detected"
            }
        },
        {
            "type": "divider"
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Today's Volume:*\n{count} Articles"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Normal Baseline (P80):*\n< {p80:.1f} Articles"
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
                    "text": "View the full <https://your-dashboard-url.com|Risk Dashboard> for analysis."
                }
            ]
        }
    ]

    # 4. Send Payload (Using 'blocks' instead of just 'text')
    try:
        requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
            json={
                "channel": SLACK_CHANNEL,
                "text": f"Crisis Alert for {brand}", # Fallback for notifications
                "blocks": blocks
            }
        )
        print(f"✅ Alert sent for {brand}")
    except Exception as e:
        print(f"⚠️ Failed to send Slack alert for {brand}: {e}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', default='risk-dashboard')
    args = parser.parse_args()

    storage = CloudStorageManager(args.bucket)
    
    # 1. Load Data (This file contains 90 days of history)
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

    # --- [NEW] CALCULATE DYNAMIC THRESHOLDS ---
    # Group by company and calculate the 80th percentile of 'negative_count'
    # This creates a dictionary: {'Nike': 5.2, 'Apple': 12.0, ...}
    print("📊 Calculating 80th percentile thresholds based on 90-day history...")
    percentiles = df.groupby('company')['negative_count'].quantile(PERCENTILE_CUTOFF).to_dict()

    # 3. Process Today's Alerts
    current_time = datetime.now()
    updates_made = False
    
    for _, row in df.iterrows():
        brand = row['company']
        count = row['negative_count']
        headlines = row['top_headlines']
        
        # A. DATE FILTER: Only look at "Today" (or last 24h)
        date_str = str(row['date']) 
        row_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        if row_date < datetime.now().date() - timedelta(days=1):
            continue

        # B. DYNAMIC THRESHOLD CHECK
        # Get this brand's specific 80th percentile (default to 0 if new brand)
        p80 = percentiles.get(brand, 0)

        # THE FORMULA: 
        # 1. Must be >= 3 total negative articles (Floor)
        # 2. Must be > The brand's 80th percentile (Relative Spike)
        if count < MIN_NEGATIVE_ARTICLES:
            continue
            
        if count < p80:
            # It's negative, but "normal" for this brand
            continue

        # C. COOLDOWN CHECK
        last_alert = history.get(brand)
        if last_alert:
            last_date = datetime.fromisoformat(last_alert)
            if current_time - last_date < timedelta(hours=ALERT_COOLDOWN_HOURS):
                print(f"Skipping {brand} (Cooling down since {last_date})")
                continue

        # --- TRIGGER ALERT ---
        print(f"🚀 Triggering alert for {brand} (Count: {count} >= P80: {p80:.1f})...")
        
        owner_email, owner_name = get_salesforce_owner(brand)

        # --- 🧪 TEST OVERRIDE ---
        # owner_email = "plane@terakeet.com" 
        # owner_name = "Pat Lane"
        # ------------------------
        
        slack_id = get_slack_user_id(owner_email)
        
        # Pass p80 to the alert function so we can show it in the message
        send_slack_alert(brand, count, p80, headlines, slack_id, owner_name)
        
        history[brand] = current_time.isoformat()
        updates_made = True

    # 4. Save State
    if updates_made:
        storage.write_text(json.dumps(history, indent=2), history_path)
        print("💾 Alert history updated.")

if __name__ == "__main__":
    main()
