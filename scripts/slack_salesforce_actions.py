#!/usr/bin/env python3
"""
Slack interactivity webhook for crisis-alert action buttons.

MVP behavior:
- Verify Slack request signatures.
- Handle `create_sf_outreach_draft` button clicks.
- Create a Salesforce Task for human review (no auto-send).
- Record idempotent processing in Postgres.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from datetime import datetime, timedelta, timezone

import psycopg2
from flask import Flask, jsonify, request
from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceMalformedRequest

app = Flask(__name__)


def _csv_env_set(name: str) -> set[str]:
    raw = os.getenv(name, "")
    if not raw:
        return set()
    return {item.strip() for item in raw.split(",") if item.strip()}


SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET", "")
DB_DSN = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL", "")
SLACK_ACTION_VALUE_SIGNING_SECRET = os.getenv("SLACK_ACTION_VALUE_SIGNING_SECRET", "").strip()
SLACK_ALLOWED_TEAM_IDS = _csv_env_set("SLACK_ALLOWED_TEAM_IDS")
SLACK_ALLOWED_APP_IDS = _csv_env_set("SLACK_ALLOWED_APP_IDS")
SLACK_ALLOWED_CHANNEL_IDS = _csv_env_set("SLACK_ALLOWED_CHANNEL_IDS")
SLACK_ALLOWED_USER_IDS = _csv_env_set("SLACK_ALLOWED_USER_IDS")
SLACK_EXPECTED_BOT_ID = os.getenv("SLACK_EXPECTED_BOT_ID", "").strip()
MAX_SLACK_BODY_BYTES = max(10_000, int(os.getenv("MAX_SLACK_BODY_BYTES", "200000")))
EXPECTED_ACTION_BLOCK_ID = os.getenv("EXPECTED_ACTION_BLOCK_ID", "crisis_alert_actions_v1")

SF_USERNAME = os.getenv("SF_USERNAME", "")
SF_PASSWORD = os.getenv("SF_PASSWORD", "")
SF_SECURITY_TOKEN = os.getenv("SF_SECURITY_TOKEN", "")

OUTREACH_TASK_SUBJECT = os.getenv("OUTREACH_TASK_SUBJECT", "Crisis Outreach Draft Review")
OUTREACH_TASK_STATUS = os.getenv("OUTREACH_TASK_STATUS", "Not Started")
OUTREACH_TASK_PRIORITY = os.getenv("OUTREACH_TASK_PRIORITY", "High")
OUTREACH_TASK_OWNER_ID = os.getenv("OUTREACH_TASK_OWNER_ID", "").strip()
OUTREACH_TASK_DUE_DAYS = max(0, int(os.getenv("OUTREACH_TASK_DUE_DAYS", "1")))


def _sf_escape(value: str) -> str:
    return (value or "").replace("\\", "\\\\").replace("'", "\\'")


def _verify_slack_signature() -> tuple[bool, str, bytes]:
    if not SLACK_SIGNING_SECRET:
        return False, "SLACK_SIGNING_SECRET not configured", b""

    timestamp = request.headers.get("X-Slack-Request-Timestamp", "").strip()
    signature = request.headers.get("X-Slack-Signature", "").strip()
    if not timestamp or not signature:
        return False, "missing Slack signature headers", b""
    if not timestamp.isdigit():
        return False, "invalid timestamp header", b""
    content_type = (request.headers.get("Content-Type") or "").lower()
    if "application/x-www-form-urlencoded" not in content_type:
        return False, "invalid content type", b""

    # Reject replayed requests older than 5 minutes.
    if abs(time.time() - int(timestamp)) > 60 * 5:
        return False, "stale Slack request timestamp", b""

    body_bytes = request.get_data(cache=True)
    if len(body_bytes) > MAX_SLACK_BODY_BYTES:
        return False, "request body too large", body_bytes

    body = body_bytes.decode("utf-8")

    base = f"v0:{timestamp}:{body}"
    digest = hmac.new(
        SLACK_SIGNING_SECRET.encode("utf-8"),
        base.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    computed = f"v0={digest}"
    if not hmac.compare_digest(computed, signature):
        return False, "signature mismatch", body_bytes
    return True, "", body_bytes


def _verify_payload_origin(payload: dict) -> tuple[bool, str]:
    team_id = ((payload.get("team") or {}).get("id") or "").strip()
    if SLACK_ALLOWED_TEAM_IDS and team_id not in SLACK_ALLOWED_TEAM_IDS:
        return False, "unauthorized workspace"
    app_id = (payload.get("api_app_id") or "").strip()
    if SLACK_ALLOWED_APP_IDS and app_id not in SLACK_ALLOWED_APP_IDS:
        return False, "unauthorized slack app"
    channel_id = ((payload.get("channel") or {}).get("id") or "").strip()
    if SLACK_ALLOWED_CHANNEL_IDS and channel_id not in SLACK_ALLOWED_CHANNEL_IDS:
        return False, "unauthorized channel"
    user_id = ((payload.get("user") or {}).get("id") or "").strip()
    if SLACK_ALLOWED_USER_IDS and user_id not in SLACK_ALLOWED_USER_IDS:
        return False, "user is not authorized for this action"
    if SLACK_EXPECTED_BOT_ID:
        msg_bot_id = ((payload.get("message") or {}).get("bot_id") or "").strip()
        if msg_bot_id and msg_bot_id != SLACK_EXPECTED_BOT_ID:
            return False, "unexpected message source"
    return True, ""


def _decode_action_context(action_value: str) -> tuple[dict, str]:
    try:
        parsed = json.loads(action_value or "{}")
    except Exception:
        return {}, "invalid action value json"
    if not isinstance(parsed, dict):
        return {}, "action value must be a json object"

    if "ctx" in parsed:
        ctx = parsed.get("ctx") or {}
        sig = (parsed.get("sig") or "").strip()
    else:
        # Backward compatibility for raw context payload.
        ctx = parsed
        sig = ""

    if not isinstance(ctx, dict):
        return {}, "invalid action context object"

    if SLACK_ACTION_VALUE_SIGNING_SECRET:
        if not sig:
            return {}, "missing action payload signature"
        compact_ctx = json.dumps(ctx, separators=(",", ":"))
        expected_sig = hmac.new(
            SLACK_ACTION_VALUE_SIGNING_SECRET.encode("utf-8"),
            compact_ctx.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(sig, expected_sig):
            return {}, "action payload signature mismatch"
    return ctx, ""


def _db_conn():
    if not DB_DSN:
        return None
    return psycopg2.connect(DB_DSN)


def _ensure_action_table(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                create table if not exists slack_action_history (
                    interaction_key text primary key,
                    action_id text not null,
                    company text,
                    ceo text,
                    article_type text,
                    alert_date date,
                    slack_user_id text,
                    slack_user_name text,
                    channel_id text,
                    message_ts text,
                    status text not null default 'received',
                    salesforce_task_id text,
                    error text,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
                """
            )


def _claim_interaction(
    conn,
    interaction_key: str,
    action_id: str,
    ctx: dict,
    slack_user_id: str,
    slack_user_name: str,
    channel_id: str,
    message_ts: str,
) -> bool:
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into slack_action_history (
                    interaction_key, action_id, company, ceo, article_type, alert_date,
                    slack_user_id, slack_user_name, channel_id, message_ts, status
                )
                values (%s, %s, %s, %s, %s, nullif(%s, '')::date, %s, %s, %s, %s, 'received')
                on conflict (interaction_key) do nothing
                """,
                (
                    interaction_key,
                    action_id,
                    (ctx.get("brand") or "").strip(),
                    (ctx.get("ceo") or "").strip(),
                    (ctx.get("article_type") or "brand").strip().lower(),
                    (ctx.get("alert_date") or "").strip(),
                    slack_user_id,
                    slack_user_name,
                    channel_id,
                    message_ts,
                ),
            )
            return cur.rowcount > 0


def _get_existing_result(conn, interaction_key: str) -> tuple[str, str, str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select status, coalesce(salesforce_task_id, ''), coalesce(error, '')
            from slack_action_history
            where interaction_key = %s
            """,
            (interaction_key,),
        )
        row = cur.fetchone()
        if not row:
            return "", "", ""
        return row[0] or "", row[1] or "", row[2] or ""


def _set_action_result(conn, interaction_key: str, status: str, task_id: str = "", error: str = ""):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update slack_action_history
                set status = %s,
                    salesforce_task_id = nullif(%s, ''),
                    error = nullif(%s, ''),
                    updated_at = now()
                where interaction_key = %s
                """,
                (status, task_id, error, interaction_key),
            )


def _sf_client() -> Salesforce:
    if not SF_USERNAME or not SF_PASSWORD or not SF_SECURITY_TOKEN:
        raise RuntimeError("Salesforce credentials missing (SF_USERNAME/SF_PASSWORD/SF_SECURITY_TOKEN)")
    return Salesforce(username=SF_USERNAME, password=SF_PASSWORD, security_token=SF_SECURITY_TOKEN)


def _find_account(sf: Salesforce, brand: str) -> tuple[str, str, str]:
    safe_brand = _sf_escape(brand)
    exact = sf.query(
        f"SELECT Id, Name, OwnerId FROM Account WHERE Name = '{safe_brand}' LIMIT 1"
    )
    if exact.get("totalSize", 0) > 0:
        rec = exact["records"][0]
        return rec.get("Id", ""), rec.get("Name", brand), rec.get("OwnerId", "")

    # Fallback fuzzy match.
    like = sf.query(
        f"SELECT Id, Name, OwnerId FROM Account WHERE Name LIKE '%{safe_brand}%' LIMIT 1"
    )
    if like.get("totalSize", 0) > 0:
        rec = like["records"][0]
        return rec.get("Id", ""), rec.get("Name", brand), rec.get("OwnerId", "")

    return "", brand, ""


def _find_comms_contact(sf: Salesforce, account_id: str) -> tuple[str, str, str, str]:
    if not account_id:
        return "", "", "", ""
    soql = (
        "SELECT Id, Name, Email, Title FROM Contact "
        f"WHERE AccountId = '{_sf_escape(account_id)}' AND ("
        "Title LIKE '%Communications%' OR "
        "Title LIKE '%Corporate Affairs%' OR "
        "Title LIKE '%Public Relations%' OR "
        "Title LIKE '%PR%' OR "
        "Title LIKE '%External Affairs%') "
        "ORDER BY LastModifiedDate DESC LIMIT 1"
    )
    result = sf.query(soql)
    if result.get("totalSize", 0) <= 0:
        return "", "", "", ""
    rec = result["records"][0]
    return (
        rec.get("Id", ""),
        rec.get("Name", ""),
        rec.get("Email", ""),
        rec.get("Title", ""),
    )


def _build_description(ctx: dict, contact: tuple[str, str, str, str], slack_user_name: str) -> str:
    brand = (ctx.get("brand") or "").strip()
    ceo = (ctx.get("ceo") or "").strip()
    article_type = (ctx.get("article_type") or "brand").strip().lower()
    alert_date = (ctx.get("alert_date") or "").strip()
    top_neg = int(ctx.get("top_stories_neg") or 0)
    dashboard_url = (ctx.get("dashboard_url") or "").strip()
    contact_name, contact_email, contact_title = contact[1], contact[2], contact[3]

    lines = [
        "Generated from Slack crisis alert button click.",
        "",
        f"Company: {brand}",
        f"Scope: {article_type}",
        f"CEO: {ceo or '(none)'}",
        f"Alert date: {alert_date or '(unknown)'}",
        f"Top Stories negative URLs: {top_neg}",
        f"Requested by (Slack): {slack_user_name}",
    ]
    if contact_name or contact_email:
        lines.extend(
            [
                "",
                "Suggested comms contact:",
                f"- Name: {contact_name or '(missing)'}",
                f"- Title: {contact_title or '(missing)'}",
                f"- Email: {contact_email or '(missing)'}",
            ]
        )
    if dashboard_url:
        lines.extend(["", f"Dashboard: {dashboard_url}"])
    lines.extend(
        [
            "",
            "Next step:",
            "- Draft outreach email and move to review queue (human send only).",
        ]
    )
    return "\n".join(lines)


def _create_salesforce_task(ctx: dict, slack_user_name: str) -> tuple[str, str]:
    brand = (ctx.get("brand") or "").strip()
    if not brand:
        raise RuntimeError("Missing company/brand in button payload")

    sf = _sf_client()
    account_id, account_name, account_owner_id = _find_account(sf, brand)
    contact_id, contact_name, contact_email, contact_title = _find_comms_contact(sf, account_id)

    due_date = (datetime.now(timezone.utc).date() + timedelta(days=OUTREACH_TASK_DUE_DAYS)).isoformat()
    description = _build_description(
        ctx,
        ("", contact_name, contact_email, contact_title),
        slack_user_name,
    )

    task_payload = {
        "Subject": OUTREACH_TASK_SUBJECT,
        "Status": OUTREACH_TASK_STATUS,
        "Priority": OUTREACH_TASK_PRIORITY,
        "ActivityDate": due_date,
        "Description": description[:32000],
    }
    if account_id:
        task_payload["WhatId"] = account_id
    if contact_id:
        task_payload["WhoId"] = contact_id

    owner_candidates = []
    if account_owner_id:
        owner_candidates.append(account_owner_id)
    if OUTREACH_TASK_OWNER_ID and OUTREACH_TASK_OWNER_ID not in owner_candidates:
        owner_candidates.append(OUTREACH_TASK_OWNER_ID)
    # Final fallback: no explicit OwnerId (integration user/default owner).
    owner_candidates.append("")

    result = None
    last_err = None
    owner_used = ""
    for candidate in owner_candidates:
        payload = dict(task_payload)
        if candidate:
            payload["OwnerId"] = candidate
        else:
            payload.pop("OwnerId", None)
        try:
            result = sf.Task.create(payload)
            owner_used = candidate
            break
        except SalesforceMalformedRequest as exc:
            last_err = exc
            if "INACTIVE_OWNER_OR_USER" in str(exc) and candidate:
                print(f"[WARN] Inactive task owner {candidate}; trying next fallback owner")
                continue
            raise

    if result is None and last_err is not None:
        raise last_err
    if not result.get("success"):
        raise RuntimeError(f"Salesforce task create failed: {result}")
    task_id = result.get("id", "")
    extra = f"account={account_name}" if account_name else "account=(none)"
    if contact_name:
        extra += f", contact={contact_name}"
    if owner_used:
        extra += f", owner={owner_used}"
    else:
        extra += ", owner=(integration/default)"
    return task_id, extra


def _interaction_key(payload: dict, action: dict) -> str:
    action_id = (action.get("action_id") or "").strip()
    action_ts = (action.get("action_ts") or "").strip()
    user_id = ((payload.get("user") or {}).get("id") or "").strip()
    msg_ts = ((payload.get("container") or {}).get("message_ts") or "").strip()
    channel_id = ((payload.get("channel") or {}).get("id") or "").strip()
    raw = "|".join([action_id, action_ts, user_id, msg_ts, channel_id])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "slack-salesforce-actions"})


@app.post("/slack/interactions")
def slack_interactions():
    ok, err, _raw_body = _verify_slack_signature()
    if not ok:
        return jsonify({"ok": False, "error": err}), 401

    payload_raw = request.form.get("payload", "").strip()
    if not payload_raw:
        return jsonify({"ok": False, "error": "missing payload"}), 400

    try:
        payload = json.loads(payload_raw)
    except Exception:
        return jsonify({"ok": False, "error": "invalid payload json"}), 400

    ok, err = _verify_payload_origin(payload)
    if not ok:
        return jsonify({"ok": False, "error": err}), 403

    actions = payload.get("actions") or []
    if not actions:
        return jsonify({"response_type": "ephemeral", "text": "No action received."}), 200

    action = actions[0]
    block_id = (action.get("block_id") or "").strip()
    if EXPECTED_ACTION_BLOCK_ID and block_id != EXPECTED_ACTION_BLOCK_ID:
        return jsonify({"ok": False, "error": "unexpected action block"}), 403
    action_id = (action.get("action_id") or "").strip()
    if action_id != "create_sf_outreach_draft":
        return jsonify({"response_type": "ephemeral", "text": f"Unsupported action: {action_id}"}), 200

    ctx, ctx_error = _decode_action_context(action.get("value") or "")
    if ctx_error:
        return jsonify({"ok": False, "error": ctx_error}), 403

    user_obj = payload.get("user") or {}
    slack_user_id = (user_obj.get("id") or "").strip()
    slack_user_name = (user_obj.get("username") or user_obj.get("name") or slack_user_id).strip()
    channel_id = ((payload.get("channel") or {}).get("id") or "").strip()
    message_ts = ((payload.get("container") or {}).get("message_ts") or "").strip()
    interaction_key = _interaction_key(payload, action)

    conn = _db_conn()
    if conn is None:
        return jsonify(
            {
                "response_type": "ephemeral",
                "text": "DATABASE_URL is not configured for action tracking.",
            }
        ), 500

    try:
        _ensure_action_table(conn)
        claimed = _claim_interaction(
            conn,
            interaction_key=interaction_key,
            action_id=action_id,
            ctx=ctx,
            slack_user_id=slack_user_id,
            slack_user_name=slack_user_name,
            channel_id=channel_id,
            message_ts=message_ts,
        )
        if not claimed:
            status, task_id, prior_error = _get_existing_result(conn, interaction_key)
            if status == "success":
                msg = f"Already processed. Salesforce task: {task_id or '(created)'}"
            elif status == "failed":
                msg = f"This click already failed earlier: {prior_error or 'unknown error'}"
            else:
                msg = "This click is already being processed."
            return jsonify({"response_type": "ephemeral", "text": msg}), 200

        try:
            task_id, extra = _create_salesforce_task(ctx, slack_user_name)
            _set_action_result(conn, interaction_key, "success", task_id=task_id)
            return jsonify(
                {
                    "response_type": "ephemeral",
                    "text": f"Created Salesforce review task `{task_id}` ({extra}).",
                }
            ), 200
        except Exception as exc:
            error_ref = interaction_key[:12]
            print(f"[ERROR] slack_interaction_failed ref={error_ref} error={exc}")
            _set_action_result(conn, interaction_key, "failed", error=str(exc)[:1000])
            return jsonify(
                {
                    "response_type": "ephemeral",
                    "text": f"Could not create Salesforce task. Ref: `{error_ref}`",
                }
            ), 200
    finally:
        conn.close()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=False)
