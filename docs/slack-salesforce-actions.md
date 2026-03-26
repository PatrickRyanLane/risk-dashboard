# Slack -> Salesforce Outreach Buttons (MVP)

This wiring adds Salesforce action buttons to crisis Slack alerts:
- Button label: `Create SF Lead`
- Action ID: `create_sf_outreach_lead`
- Behavior: creates a Salesforce `Lead` for human review, prefilling the matched outreach contact last name when available
- Button label: `Create SF Outreach Draft`
- Action ID: `create_sf_outreach_draft`
- Behavior: creates a Salesforce `Task` for human review (no auto-send)

## What was added

- Alert button payload in `scripts/send_crisis_alerts.py`
  - Used by both `send_crisis_alerts.py` and `send_targeted_alerts.py`
- Interactivity webhook in `scripts/slack_salesforce_actions.py`
  - `POST /slack/interactions`
  - Signature verification (Slack signing secret)
  - Idempotent click tracking in Postgres table `slack_action_history`
  - Salesforce Task and Lead creation

## Required env vars

- `SLACK_SIGNING_SECRET`
- `DATABASE_URL` (or `SUPABASE_DB_URL`)
- `SF_USERNAME`
- `SF_PASSWORD`
- `SF_SECURITY_TOKEN`

Optional:
- `SLACK_ENABLE_ACTION_BUTTON` (default `1`)
- `SLACK_ACTION_VALUE_SIGNING_SECRET` (HMAC-signs button payload to prevent tampering)
- `OUTREACH_TASK_SUBJECT` (default `Crisis Outreach Draft Review`)
- `OUTREACH_TASK_STATUS` (default `Open`)
- `OUTREACH_TASK_PRIORITY` (default `High`)
- `OUTREACH_TASK_OWNER_ID` (fallback owner if account owner not found)
- `OUTREACH_TASK_DUE_DAYS` (default `1`)
- `OUTREACH_LEAD_OWNER_ID` (fallback owner if account owner not found)
- `OUTREACH_LEAD_STATUS` (default `Cold Outreach`)
- `OUTREACH_LEAD_LAST_NAME_FALLBACK` (default `Communications` when no ideal contact last name is found)
- `SLACK_ALLOWED_TEAM_IDS` (comma-separated Slack workspace IDs)
- `SLACK_ALLOWED_APP_IDS` (comma-separated Slack app IDs)
- `SLACK_ALLOWED_CHANNEL_IDS` (comma-separated channel IDs)
- `SLACK_ALLOWED_USER_IDS` (comma-separated user IDs allowed to click)
- `SLACK_EXPECTED_BOT_ID` (only accept actions from this bot message source)
- `EXPECTED_ACTION_BLOCK_ID` (default `crisis_alert_actions_v1`)
- `MAX_SLACK_BODY_BYTES` (default `200000`)

## Slack app setup

1. In your Slack app, enable **Interactivity & Shortcuts**.
2. Set Request URL to your deployed webhook:
   - `https://<your-service>/slack/interactions`
3. Ensure the bot token used by alert sender can post messages (`chat.postMessage`).

## Run locally

```bash
python scripts/slack_salesforce_actions.py
```

Health check:

```bash
curl http://localhost:8080/health
```

## Deploy script

A ready deploy script is included:

```bash
./scripts/deploy_slack_actions.sh
```

Override config as needed:

```bash
PROJECT_ID=gen-lang-client-0154760958 \
REGION=us-west1 \
SLACK_ALLOWED_TEAM_IDS=TXXXX \
SLACK_ALLOWED_APP_IDS=AXXXX \
SLACK_ALLOWED_CHANNEL_IDS=CXXXX \
./scripts/deploy_slack_actions.sh
```

## Notes

- Clicks are idempotent by action/message/user tuple hash.
- Requests are rejected unless Slack signature + timestamp checks pass.
- Additional origin allowlists are available via `SLACK_ALLOWED_*` env vars.
- The webhook currently responds synchronously (simple MVP).
- Next step for scale: enqueue click payload and process async.
