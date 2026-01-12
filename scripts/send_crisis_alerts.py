#!/usr/bin/env python3
"""
Reads aggregated negative articles and sends Slack alerts to Salesforce account owners.
Tracks alert history in GCS to prevent spamming.
"""

import os
import json
import argparse
import requests
from datetime import datetime, timedelta
from simple_salesforce import Salesforce
from storage_utils import CloudStorageManager

# --- CONFIG ---
SLACK_BOT_TOKEN = os.getenv('SLACK_BOT_TOKEN')
SF_USERNAME = os.getenv('SF_USERNAME')
SF_PASSWORD = os.getenv('SF_PASSWORD')
SF_TOKEN = os.getenv('SF_SECURITY_TOKEN')
SLACK_CHANNEL = "#crisis-alerts" # Fallback/Public channel
FALLBACK_SLACK_ID = "UT1EC3ENR" # Put your ID or a specific manager's ID here

# Thresholds
NEGATIVE_COUNT_THRESHOLD = 3  # Only alert if > 3 negative articles
ALERT_COOLDOWN_HOURS = 24     # Don't alert the same brand again for 24 hours

def get_salesforce_owner(brand_name):
    """Finds the account owner email for a brand in Salesforce."""
    try:
        sf = Salesforce(username=SF_USERNAME, password=SF_PASSWORD, security_token=SF_TOKEN)
        # Note: Adjust 'Name' if you use a specific field for Brand Name
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

def send_slack_alert(brand, count, headlines, owner_slack_id, owner_name):
    """Sends the formatted alert."""
    
    # Logic: If we found a Slack ID, tag them. If not, just name them.
    if owner_slack_id:
        mention = f"<@{owner_slack_id}>"
    elif owner_name:
        mention = f"{owner_name} (Email lookup failed)"
    else:
    # If Salesforce didn't return anyone, tag the fallback person
        mention = f"<@{FALLBACK_SLACK_ID}> (Salesforce Missing)"
    # Format top headlines
    headline_text = ""
    if headlines:
        # Headlines in your CSV are |-separated
        for hl in str(headlines).split('|')[:3]: 
            headline_text += f"• {hl}\n"

    message = {
        "channel": SLACK_CHANNEL,
        "text": (
            f"🚨 **CRISIS ALERT: {brand}**\n"
            f"**Attn:** {mention}\n\n"
            f"**Status:** {count} negative articles detected today.\n"
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
    
    # 1. Load Data
    summary_path = "data/daily_counts/negative-articles-summary.csv"
    if not storage.file_exists(summary_path):
        print("No negative summary file found. Exiting.")
        return
    
    df = storage.read_csv(summary_path)
    
    # 2. Load Alert History (State)
    history_path = "data/alert_history.json"
    history = {}
    if storage.file_exists(history_path):
        try:
            history = json.loads(storage.read_text(history_path))
        except:
            print("Could not read history, starting fresh.")

    # 3. Process
    current_time = datetime.now()
    updates_made = False

    # Group by company to sum up CEO + Brand negative counts if needed
    # Your CSV has 'negative_count' per row. Let's filter high severity.
    
    for _, row in df.iterrows():
        brand = row['company']
        count = row['negative_count']
        headlines = row['top_headlines']
        
        if count < NEGATIVE_COUNT_THRESHOLD:
            continue

        # Check cooldown
        last_alert = history.get(brand)
        if last_alert:
            last_date = datetime.fromisoformat(last_alert)
            if current_time - last_date < timedelta(hours=ALERT_COOLDOWN_HOURS):
                print(f"Skipping {brand} (Cooling down since {last_date})")
                continue

        # --- TRIGGER ALERT ---
        print(f"🚀 Triggering alert for {brand}...")
        
        # A. Salesforce
        owner_email, owner_name = get_salesforce_owner(brand)

        # --- 🧪 TEST OVERRIDE ---
    # Uncomment this line to force ALL alerts to you for testing
    owner_email = "plane@terakeet.com" 
    owner_name = "Pat Lane"
    # ------------------------
        
        # B. Slack
        slack_id = get_slack_user_id(owner_email)
        
        # C. Send
        send_slack_alert(brand, count, headlines, slack_id, owner_name)
        
        # D. Update History
        history[brand] = current_time.isoformat()
        updates_made = True

    # 4. Save State
    if updates_made:
        storage.write_text(json.dumps(history, indent=2), history_path)
        print("💾 Alert history updated.")

if __name__ == "__main__":
    main()