#!/usr/bin/env python3
"""
Slack interactivity webhook for crisis-alert action buttons.

MVP behavior:
- Verify Slack request signatures.
- Handle task and lead action button clicks from crisis alerts.
- Create a Salesforce Task or Lead for human review (no auto-send).
- Record idempotent processing in Postgres.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone

import psycopg2
import requests
from flask import Flask, jsonify, request
try:
    from google.cloud import tasks_v2
except Exception:
    tasks_v2 = None
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
OUTREACH_TASK_ACTION_ID = "create_sf_outreach_draft"
OUTREACH_LEAD_ACTION_ID = "create_sf_outreach_lead"
CLOUD_TASKS_PROJECT = (
    os.getenv("CLOUD_TASKS_PROJECT")
    or os.getenv("GOOGLE_CLOUD_PROJECT")
    or os.getenv("GCP_PROJECT")
    or ""
).strip()
CLOUD_TASKS_LOCATION = os.getenv("CLOUD_TASKS_LOCATION", "").strip()
CLOUD_TASKS_QUEUE = os.getenv("CLOUD_TASKS_QUEUE", "").strip()

SF_USERNAME = os.getenv("SF_USERNAME", "")
SF_PASSWORD = os.getenv("SF_PASSWORD", "")
SF_SECURITY_TOKEN = os.getenv("SF_SECURITY_TOKEN", "")

OUTREACH_TASK_SUBJECT = os.getenv("OUTREACH_TASK_SUBJECT", "Crisis Outreach Draft Review")
OUTREACH_TASK_STATUS = os.getenv("OUTREACH_TASK_STATUS", "Open")
OUTREACH_TASK_PRIORITY = os.getenv("OUTREACH_TASK_PRIORITY", "High")
OUTREACH_TASK_OWNER_ID = os.getenv("OUTREACH_TASK_OWNER_ID", "").strip()
OUTREACH_TASK_DUE_DAYS = max(0, int(os.getenv("OUTREACH_TASK_DUE_DAYS", "1")))
OUTREACH_LEAD_OWNER_ID = os.getenv("OUTREACH_LEAD_OWNER_ID", "").strip()
OUTREACH_LEAD_STATUS = os.getenv("OUTREACH_LEAD_STATUS", "Cold Outreach").strip()
OUTREACH_LEAD_LAST_NAME_FALLBACK = (
    os.getenv("OUTREACH_LEAD_LAST_NAME_FALLBACK", "Communications").strip() or "Communications"
)
SF_ACCOUNT_MATCH_MIN_SCORE = int(os.getenv("SF_ACCOUNT_MATCH_MIN_SCORE", "78"))

_COMPANY_SUFFIXES = {
    "inc",
    "incorporated",
    "corp",
    "corporation",
    "co",
    "company",
    "llc",
    "ltd",
    "limited",
    "plc",
    "holdings",
    "holding",
    "group",
}
_ACTION_TABLE_LOCK = threading.Lock()
_ACTION_TABLE_READY = False


def _empty_contact() -> dict[str, str]:
    return {
        "id": "",
        "name": "",
        "first_name": "",
        "last_name": "",
        "email": "",
        "title": "",
    }


def _sf_escape(value: str) -> str:
    return (value or "").replace("\\", "\\\\").replace("'", "\\'")


def _normalize_company_name(name: str) -> str:
    raw = (name or "").lower().replace("&", " and ")
    tokens = re.sub(r"[^a-z0-9]+", " ", raw).split()
    filtered = [tok for tok in tokens if tok and tok not in _COMPANY_SUFFIXES]
    return " ".join(filtered).strip()


def _score_account_match(brand_norm: str, brand_tokens: list[str], candidate_name: str) -> int:
    cand_norm = _normalize_company_name(candidate_name)
    if not brand_norm or not cand_norm:
        return 0
    if cand_norm == brand_norm:
        return 100

    cand_tokens = cand_norm.split()
    if not cand_tokens:
        return 0

    brand_set = set(brand_tokens)
    cand_set = set(cand_tokens)
    overlap = len(brand_set & cand_set)
    if overlap == 0:
        return 0

    recall = overlap / max(1, len(brand_set))
    precision = overlap / max(1, len(cand_set))
    score = int(100 * (0.65 * recall + 0.35 * precision))

    # Penalize extra unmatched tokens to avoid false positives like
    # "H&R Block" for target "Block".
    extra = max(0, len(cand_set) - len(brand_set))
    score -= extra * 8

    # Reward prefix consistency.
    if cand_norm.startswith(brand_norm + " ") or brand_norm.startswith(cand_norm + " "):
        score += 5
    if brand_tokens and cand_tokens and brand_tokens[0] != cand_tokens[0]:
        score -= 10

    return max(0, score)


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


def _task_signature(body_bytes: bytes) -> str:
    if not SLACK_ACTION_VALUE_SIGNING_SECRET:
        return ""
    return hmac.new(
        SLACK_ACTION_VALUE_SIGNING_SECRET.encode("utf-8"),
        body_bytes,
        hashlib.sha256,
    ).hexdigest()


def _verify_task_signature(body_bytes: bytes, signature: str) -> tuple[bool, str]:
    if not SLACK_ACTION_VALUE_SIGNING_SECRET:
        return True, ""
    if not signature:
        return False, "missing task signature"
    expected = _task_signature(body_bytes)
    if not hmac.compare_digest(expected, signature.strip()):
        return False, "task signature mismatch"
    return True, ""


def _cloud_tasks_enabled() -> bool:
    return bool(tasks_v2 and CLOUD_TASKS_PROJECT and CLOUD_TASKS_LOCATION and CLOUD_TASKS_QUEUE)


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
            cur.execute(
                """
                alter table slack_action_history
                add column if not exists salesforce_record_id text
                """
            )
            cur.execute(
                """
                alter table slack_action_history
                add column if not exists salesforce_record_type text
                """
            )


def _ensure_action_table_once(conn):
    global _ACTION_TABLE_READY
    if _ACTION_TABLE_READY:
        return
    with _ACTION_TABLE_LOCK:
        if _ACTION_TABLE_READY:
            return
        _ensure_action_table(conn)
        _ACTION_TABLE_READY = True


def _cloud_tasks_queue_path() -> str:
    if not _cloud_tasks_enabled():
        return ""
    client = tasks_v2.CloudTasksClient()
    return client.queue_path(CLOUD_TASKS_PROJECT, CLOUD_TASKS_LOCATION, CLOUD_TASKS_QUEUE)


def _enqueue_action_task(base_url: str, payload: dict):
    if not _cloud_tasks_enabled():
        raise RuntimeError("Cloud Tasks is not configured")

    client = tasks_v2.CloudTasksClient()
    queue_path = _cloud_tasks_queue_path()
    body_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    signature = _task_signature(body_bytes)
    if signature:
        headers["X-Action-Task-Signature"] = signature

    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": f"{base_url.rstrip('/')}/internal/process-action",
            "headers": headers,
            "body": body_bytes,
        }
    }
    client.create_task(parent=queue_path, task=task)


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


def _get_existing_result(conn, interaction_key: str) -> tuple[str, str, str, str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                status,
                coalesce(
                    salesforce_record_type,
                    case when nullif(salesforce_task_id, '') is not null then 'task' else '' end,
                    ''
                ),
                coalesce(salesforce_record_id, salesforce_task_id, ''),
                coalesce(error, '')
            from slack_action_history
            where interaction_key = %s
            """,
            (interaction_key,),
        )
        row = cur.fetchone()
        if not row:
            return "", "", "", ""
        return row[0] or "", row[1] or "", row[2] or "", row[3] or ""


def _set_action_result(
    conn,
    interaction_key: str,
    status: str,
    record_type: str = "",
    record_id: str = "",
    error: str = "",
):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update slack_action_history
                set status = %s,
                    salesforce_record_type = nullif(%s, ''),
                    salesforce_record_id = nullif(%s, ''),
                    salesforce_task_id = case
                        when %s = 'task' then nullif(%s, '')
                        else salesforce_task_id
                    end,
                    error = nullif(%s, ''),
                    updated_at = now()
                where interaction_key = %s
                """,
                (status, record_type, record_id, record_type, record_id, error, interaction_key),
            )


def _post_slack_followup(response_url: str, text: str):
    if not response_url:
        return
    try:
        requests.post(
            response_url,
            json={
                "response_type": "ephemeral",
                "replace_original": False,
                "text": text,
            },
            timeout=5,
        )
    except Exception as exc:
        print(f"[WARN] Failed posting Slack follow-up: {exc}")


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

    brand_norm = _normalize_company_name(brand)
    brand_tokens = brand_norm.split()
    if not brand_tokens:
        return "", brand, ""

    # Fallback: fetch a candidate set, then score locally to avoid wrong matches.
    candidate_records = []
    like_full = sf.query(
        f"SELECT Id, Name, OwnerId FROM Account WHERE Name LIKE '%{safe_brand}%' LIMIT 50"
    )
    candidate_records.extend(like_full.get("records") or [])

    first_token = brand_tokens[0]
    if first_token and first_token not in {"the", "and"}:
        like_token = sf.query(
            f"SELECT Id, Name, OwnerId FROM Account WHERE Name LIKE '%{_sf_escape(first_token)}%' LIMIT 50"
        )
        for rec in like_token.get("records") or []:
            rid = rec.get("Id")
            if rid and all(rid != r.get("Id") for r in candidate_records):
                candidate_records.append(rec)

    if candidate_records:
        scored = []
        for rec in candidate_records:
            name = rec.get("Name", "") or ""
            score = _score_account_match(brand_norm, brand_tokens, name)
            scored.append((score, rec))
        scored.sort(key=lambda x: x[0], reverse=True)
        best_score, best_rec = scored[0]
        second_score = scored[1][0] if len(scored) > 1 else -1

        if best_score < SF_ACCOUNT_MATCH_MIN_SCORE:
            print(f"[WARN] No safe Salesforce account match for '{brand}' (best score={best_score})")
            return "", brand, ""
        if second_score >= SF_ACCOUNT_MATCH_MIN_SCORE and (best_score - second_score) < 4:
            print(
                f"[WARN] Ambiguous Salesforce account match for '{brand}' "
                f"(scores {best_score} vs {second_score}); leaving unlinked."
            )
            return "", brand, ""
        return (
            best_rec.get("Id", ""),
            best_rec.get("Name", brand),
            best_rec.get("OwnerId", ""),
        )

    return "", brand, ""


def _contact_title_score(title: str) -> int:
    title_norm = re.sub(r"[^a-z0-9]+", " ", (title or "").lower()).strip()
    if not title_norm:
        return -1

    score = 0

    # Prefer the most relevant outward-facing crisis contacts first.
    if re.search(r"\b(communications?|comms|corporate affairs?|public affairs?|public relations?|pr|external affairs?|media relations?|cco)\b", title_norm):
        score += 120
    if re.search(r"\b(marketing|brand|reputation|cmo)\b", title_norm):
        score += 80
    if re.search(r"\b(chief executive officer|ceo|president|founder|co founder|cofounder)\b", title_norm):
        score += 55

    if re.search(r"\bchief\b", title_norm):
        score += 35
    elif re.search(r"\b(executive vice president|senior vice president|evp|svp|vice president|vp|head|president)\b", title_norm):
        score += 24
    elif re.search(r"\bdirector\b", title_norm):
        score += 14

    if re.search(r"\b(manager|specialist|coordinator|assistant|associate|intern)\b", title_norm):
        score -= 35
    if re.search(r"\b(legal|finance|accounting|human resources|hr|operations|sales|customer support)\b", title_norm):
        score -= 25

    return score


def _find_comms_contact(sf: Salesforce, account_id: str) -> dict[str, str]:
    if not account_id:
        return _empty_contact()
    soql = (
        "SELECT Id, Name, FirstName, LastName, Email, Title, LastModifiedDate FROM Contact "
        f"WHERE AccountId = '{_sf_escape(account_id)}' AND Title != null "
        "ORDER BY LastModifiedDate DESC LIMIT 100"
    )
    result = sf.query(soql)
    if result.get("totalSize", 0) <= 0:
        return _empty_contact()

    candidates = []
    for rec in result.get("records") or []:
        title = rec.get("Title", "") or ""
        score = _contact_title_score(title)
        if score < 60:
            continue
        has_email = 1 if (rec.get("Email") or "").strip() else 0
        candidates.append((score, has_email, rec.get("LastModifiedDate", "") or "", rec))

    if not candidates:
        return _empty_contact()

    candidates.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
    rec = candidates[0][3]
    return {
        "id": rec.get("Id", "") or "",
        "name": rec.get("Name", "") or "",
        "first_name": rec.get("FirstName", "") or "",
        "last_name": rec.get("LastName", "") or "",
        "email": rec.get("Email", "") or "",
        "title": rec.get("Title", "") or "",
    }


def _build_description(ctx: dict, contact: dict[str, str], slack_user_name: str) -> str:
    brand = (ctx.get("brand") or "").strip()
    ceo = (ctx.get("ceo") or "").strip()
    article_type = (ctx.get("article_type") or "brand").strip().lower()
    alert_date = (ctx.get("alert_date") or "").strip()
    top_neg = int(ctx.get("top_stories_neg") or 0)
    dashboard_url = (ctx.get("dashboard_url") or "").strip()
    contact_name = (contact.get("name") or "").strip()
    contact_email = (contact.get("email") or "").strip()
    contact_title = (contact.get("title") or "").strip()

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
                "Suggested outreach contact:",
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


def _create_with_owner_fallback(
    sf_object,
    payload: dict,
    preferred_owner_id: str,
    fallback_owner_id: str,
) -> tuple[dict, str]:
    owner_candidates = []
    if preferred_owner_id:
        owner_candidates.append(preferred_owner_id)
    if fallback_owner_id and fallback_owner_id not in owner_candidates:
        owner_candidates.append(fallback_owner_id)
    owner_candidates.append("")

    result = None
    last_err = None
    owner_used = ""
    for candidate in owner_candidates:
        candidate_payload = dict(payload)
        if candidate:
            candidate_payload["OwnerId"] = candidate
        else:
            candidate_payload.pop("OwnerId", None)
        try:
            result = sf_object.create(candidate_payload)
            owner_used = candidate
            break
        except SalesforceMalformedRequest as exc:
            last_err = exc
            if "INACTIVE_OWNER_OR_USER" in str(exc) and candidate:
                print(f"[WARN] Inactive owner {candidate}; trying next fallback owner")
                continue
            raise

    if result is None and last_err is not None:
        raise last_err
    if not result.get("success"):
        raise RuntimeError(f"Salesforce create failed: {result}")
    return result, owner_used


def _create_salesforce_task(ctx: dict, slack_user_name: str) -> tuple[str, str]:
    brand = (ctx.get("brand") or "").strip()
    if not brand:
        raise RuntimeError("Missing company/brand in button payload")

    sf = _sf_client()
    account_id, account_name, account_owner_id = _find_account(sf, brand)
    contact = _find_comms_contact(sf, account_id)
    contact_id = (contact.get("id") or "").strip()
    contact_name = (contact.get("name") or "").strip()

    due_date = (datetime.now(timezone.utc).date() + timedelta(days=OUTREACH_TASK_DUE_DAYS)).isoformat()
    description = _build_description(ctx, contact, slack_user_name)

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

    result, owner_used = _create_with_owner_fallback(
        sf.Task,
        task_payload,
        preferred_owner_id=account_owner_id,
        fallback_owner_id=OUTREACH_TASK_OWNER_ID,
    )
    task_id = result.get("id", "")
    extra = f"account={account_name}" if account_name else "account=(none)"
    if contact_name:
        extra += f", contact={contact_name}"
    if owner_used:
        extra += f", owner={owner_used}"
    else:
        extra += ", owner=(integration/default)"
    return task_id, extra


def _lead_last_name(contact: dict[str, str]) -> str:
    last_name = (contact.get("last_name") or "").strip()
    if last_name:
        return last_name
    name = (contact.get("name") or "").strip()
    if name:
        return name.split()[-1]
    return OUTREACH_LEAD_LAST_NAME_FALLBACK


def _create_salesforce_lead(ctx: dict, slack_user_name: str) -> tuple[str, str]:
    brand = (ctx.get("brand") or "").strip()
    if not brand:
        raise RuntimeError("Missing company/brand in button payload")

    sf = _sf_client()
    account_id, account_name, account_owner_id = _find_account(sf, brand)
    contact = _find_comms_contact(sf, account_id)

    lead_company = account_name or brand
    lead_last_name = _lead_last_name(contact)
    description = _build_description(ctx, contact, slack_user_name)

    lead_payload = {
        "Company": lead_company,
        "LastName": lead_last_name,
        "Description": description[:32000],
    }
    if OUTREACH_LEAD_STATUS:
        lead_payload["Status"] = OUTREACH_LEAD_STATUS
    if (contact.get("first_name") or "").strip():
        lead_payload["FirstName"] = contact["first_name"].strip()
    if (contact.get("email") or "").strip():
        lead_payload["Email"] = contact["email"].strip()
    if (contact.get("title") or "").strip():
        lead_payload["Title"] = contact["title"].strip()

    result, owner_used = _create_with_owner_fallback(
        sf.Lead,
        lead_payload,
        preferred_owner_id=account_owner_id,
        fallback_owner_id=OUTREACH_LEAD_OWNER_ID,
    )
    lead_id = result.get("id", "")
    extra = f"company={lead_company}, last_name={lead_last_name}"
    if (contact.get("name") or "").strip():
        extra += f", source_contact={contact['name'].strip()}"
    if owner_used:
        extra += f", owner={owner_used}"
    else:
        extra += ", owner=(integration/default)"
    return lead_id, extra


def _interaction_key(payload: dict, action: dict) -> str:
    action_id = (action.get("action_id") or "").strip()
    action_ts = (action.get("action_ts") or "").strip()
    user_id = ((payload.get("user") or {}).get("id") or "").strip()
    msg_ts = ((payload.get("container") or {}).get("message_ts") or "").strip()
    channel_id = ((payload.get("channel") or {}).get("id") or "").strip()
    raw = "|".join([action_id, action_ts, user_id, msg_ts, channel_id])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


ACTION_CONFIG = {
    OUTREACH_TASK_ACTION_ID: {
        "label": "Salesforce review task",
        "record_type": "task",
        "creator": _create_salesforce_task,
    },
    OUTREACH_LEAD_ACTION_ID: {
        "label": "Salesforce lead",
        "record_type": "lead",
        "creator": _create_salesforce_lead,
    },
}


def _process_action(
    interaction_key: str,
    action_id: str,
    ctx: dict,
    slack_user_id: str,
    slack_user_name: str,
    channel_id: str,
    message_ts: str,
):
    action_cfg = ACTION_CONFIG[action_id]
    action_ref = interaction_key[:12]
    brand = (ctx.get("brand") or "").strip()
    print(
        f"[INFO] slack_action_start ref={action_ref} action_id={action_id} "
        f"user={slack_user_id or '(unknown)'} company={brand or '(unknown)'}"
    )
    conn = _db_conn()
    if conn is None:
        return f"Could not create {action_cfg['label']}: database connection unavailable."
    try:
        _ensure_action_table_once(conn)
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
            status, record_type, record_id, prior_error = _get_existing_result(conn, interaction_key)
            record_label = "Salesforce lead" if record_type == "lead" else "Salesforce task"
            if status == "success":
                msg = f"Already processed. {record_label}: {record_id or '(created)'}"
            elif status == "failed":
                msg = f"This click already failed earlier: {prior_error or 'unknown error'}"
            else:
                msg = "This click is already being processed."
            return msg
        try:
            record_id, extra = action_cfg["creator"](ctx, slack_user_name)
            _set_action_result(
                conn,
                interaction_key,
                "success",
                record_type=action_cfg["record_type"],
                record_id=record_id,
            )
            print(
                f"[INFO] slack_action_success ref={action_ref} action_id={action_id} "
                f"record_id={record_id or '(missing)'}"
            )
            return f"Created {action_cfg['label']} `{record_id}` ({extra})."
        except Exception as exc:
            print(f"[ERROR] slack_interaction_failed ref={action_ref} error={exc}")
            error_text = str(exc).replace("\n", " ").strip()
            _set_action_result(conn, interaction_key, "failed", error=error_text[:1000])
            detail = f" {error_text[:200]}" if error_text else ""
            return f"Could not create {action_cfg['label']}. Ref: `{action_ref}`.{detail}"
    finally:
        conn.close()


@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "slack-salesforce-actions"})


@app.get("/debug/queue-health")
def queue_health():
    status = {
        "ok": False,
        "service": "slack-salesforce-actions",
        "mode": "cloud_tasks" if _cloud_tasks_enabled() else "inline_fallback",
        "tasks_library_loaded": bool(tasks_v2),
        "cloud_tasks_configured": _cloud_tasks_enabled(),
        "project": CLOUD_TASKS_PROJECT or "",
        "location": CLOUD_TASKS_LOCATION or "",
        "queue": CLOUD_TASKS_QUEUE or "",
        "queue_path": "",
        "queue_lookup_ok": False,
        "queue_lookup_error": "",
    }

    if not _cloud_tasks_enabled():
        return jsonify(status), 200

    queue_path = ""
    try:
        queue_path = _cloud_tasks_queue_path()
        client = tasks_v2.CloudTasksClient()
        client.get_queue(name=queue_path)
        status["ok"] = True
        status["queue_path"] = queue_path
        status["queue_lookup_ok"] = True
    except Exception as exc:
        status["queue_path"] = queue_path
        status["queue_lookup_error"] = str(exc)

    return jsonify(status), 200


@app.post("/internal/process-action")
def process_action_task():
    raw_body = request.get_data(cache=True)
    ok, err = _verify_task_signature(raw_body, request.headers.get("X-Action-Task-Signature", ""))
    if not ok:
        return jsonify({"ok": False, "error": err}), 403

    try:
        payload = json.loads((raw_body or b"{}").decode("utf-8"))
    except Exception:
        return jsonify({"ok": False, "error": "invalid task payload"}), 400

    action_id = (payload.get("action_id") or "").strip()
    if action_id not in ACTION_CONFIG:
        return jsonify({"ok": False, "error": f"unsupported action: {action_id}"}), 400

    ctx = payload.get("ctx") or {}
    if not isinstance(ctx, dict):
        return jsonify({"ok": False, "error": "invalid task context"}), 400

    message = _process_action(
        interaction_key=(payload.get("interaction_key") or "").strip(),
        action_id=action_id,
        ctx=ctx,
        slack_user_id=(payload.get("slack_user_id") or "").strip(),
        slack_user_name=(payload.get("slack_user_name") or "").strip(),
        channel_id=(payload.get("channel_id") or "").strip(),
        message_ts=(payload.get("message_ts") or "").strip(),
    )
    response_url = (payload.get("response_url") or "").strip()
    _post_slack_followup(response_url, message)
    return jsonify({"ok": True, "message": message}), 200


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
    if action_id not in ACTION_CONFIG:
        return jsonify({"response_type": "ephemeral", "text": f"Unsupported action: {action_id}"}), 200

    ctx, ctx_error = _decode_action_context(action.get("value") or "")
    if ctx_error:
        return jsonify({"ok": False, "error": ctx_error}), 403

    user_obj = payload.get("user") or {}
    slack_user_id = (user_obj.get("id") or "").strip()
    slack_user_name = (user_obj.get("username") or user_obj.get("name") or slack_user_id).strip()
    channel_id = ((payload.get("channel") or {}).get("id") or "").strip()
    message_ts = ((payload.get("container") or {}).get("message_ts") or "").strip()
    response_url = (payload.get("response_url") or "").strip()
    interaction_key = _interaction_key(payload, action)
    task_payload = {
        "interaction_key": interaction_key,
        "action_id": action_id,
        "ctx": ctx,
        "slack_user_id": slack_user_id,
        "slack_user_name": slack_user_name,
        "channel_id": channel_id,
        "message_ts": message_ts,
        "response_url": response_url,
    }

    if _cloud_tasks_enabled():
        try:
            _enqueue_action_task(request.url_root.rstrip("/"), task_payload)
            return jsonify(
                {
                    "response_type": "ephemeral",
                    "text": "Processing request... I will post a follow-up here shortly.",
                }
            ), 200
        except Exception as exc:
            print(f"[ERROR] slack_action_enqueue_failed ref={interaction_key[:12]} error={exc}")
            return jsonify(
                {
                    "response_type": "ephemeral",
                    "text": "Could not queue the Salesforce action. Please try again in a moment.",
                }
            ), 200

    message = _process_action(
        interaction_key=interaction_key,
        action_id=action_id,
        ctx=ctx,
        slack_user_id=slack_user_id,
        slack_user_name=slack_user_name,
        channel_id=channel_id,
        message_ts=message_ts,
    )
    return jsonify({"response_type": "ephemeral", "text": message}), 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=False)
