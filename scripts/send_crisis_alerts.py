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
ALERT_COOLDOWN_HOURS = 24  

def get_salesforce_owner(brand_name):
    """Finds the account owner email for a brand in Salesforce."""
    try:
        sf = Salesforce(username=SF_USERNAME, password=SF_PASSWORD, security_token=SF_TOKEN)
        query = f"SELECT Owner.Email, Owner.Name FROM Account WHERE Name = '{brand_name}' LIMIT 1"
        result = sf.query(query)
        
        if result['totalSize'] > 0:
            owner = result['records'][0]['Owner']
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

def send_slack_alert(brand, count, p80_val, headlines, owner_slack_id, owner_name):
    """Sends the formatted alert with context about the spike."""
    
    if owner_slack_id:
        mention = f"<@{owner_slack_id}>"
    elif owner_name:
        mention = f"{owner_name} (Email lookup failed)"
    else:
        mention = f"<@{FALLBACK_SLACK_ID}> (Salesforce Missing)"

    headline_text = ""
    if headlines:
        for hl in str(headlines).split('|')[:3]: 
            headline_text += f"• {hl}\n"

    message = {
        "channel": SLACK_CHANNEL,
        "text": (
            f"🚨 **CRISIS ALERT: {brand}**\n"
            f"**Attn:** {mention}\n\n"
            f"**Issue:** Negative news surge detected.\n"
            f"**Volume:** {count} negative articles (Normal 80% range: < {p80_val:.1f})\n\n"
            f"**Top Headlines:**\n{headline_text}\n"
            f"_Check the dashboard for full details._"
        )
    }

    requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
        json=message
    )
    print(f"✅ Alert sent for {brand} to {mention}")

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
        
        # owner_email, owner_name = get_salesforce_owner(brand)

        # --- 🧪 TEST OVERRIDE ---
        owner_email = "plane@terakeet.com" 
        owner_name = "Pat Lane"
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