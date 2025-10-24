#!/usr/bin/env python3
"""
Mailgun email + alert cooldown utilities.
"""
from __future__ import annotations

import os
import json
import requests
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from email.utils import format_datetime
from typing import List, Dict, Any, Optional
# --- Safe env helpwers ---
def _get_int_env(name: str, default: int) -> int:
    val = os.environ.get(name)
    try:
        return int(str(val).strip()) if val not in (None, "") else default
    except (ValueError, TypeError):
        return default

def _get_float_env(name: str, default: float) -> float:
    val = os.environ.get(name)
    try:
        return float(str(val).strip()) if val not in (None, "") else default
    except (ValueError, TypeError):
        return default

# Config
NEGATIVE_THRESHOLD = _get_float_env("NEGATIVE_THRESHOLD", 0.4)
ALERT_COOLDOWN_DAYS = _get_int_env("ALERT_COOLDOWN_DAYS", 180)
EASTERN = ZoneInfo("US/Eastern")
SOFT_SHIFT_HOURS = _get_int_env("SOFT_SHIFT_HOURS", 6)
LAST_ALERT_DATES_PATH = os.environ.get("LAST_ALERT_DATES_PATH", "data/email_alerts/last_alert_dates.json")
ALERT_SEND_MODE = (os.environ.get("ALERT_SEND_MODE") or "same_morning").lower()

def _ensure_parents(p: str) -> None:
    d = os.path.dirname(p)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def read_last_alert_dates() -> Dict[str, str]:
    try:
        with open(LAST_ALERT_DATES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}

def write_last_alert_dates(d: Dict[str, str]) -> None:
    _ensure_parents(LAST_ALERT_DATES_PATH)
    with open(LAST_ALERT_DATES_PATH, "w", encoding="utf-8") as f:
        json.dump(d, f, indent=2, sort_keys=True)

def now_eastern_date_str() -> str:
    now = datetime.now(tz=EASTERN)
    if now.hour < SOFT_SHIFT_HOURS:
        return (now.date() - timedelta(days=1)).isoformat()
    return now.date().isoformat()

def _compute_delivery_time_rfc2822(run_date_str: str, mode: str) -> Optional[str]:
    mode = (mode or ALERT_SEND_MODE or "same_morning").lower()
    now_et = datetime.now(tz=EASTERN)
    if mode == "same_morning":
        today_9 = datetime(now_et.year, now_et.month, now_et.day, 9, 0, 0, tzinfo=EASTERN)
        return format_datetime(today_9) if now_et < today_9 else None
    # next_morning
    run_date = datetime.fromisoformat(run_date_str).date()
    send_day = run_date + timedelta(days=1)
    send_time_et = datetime(send_day.year, send_day.month, send_day.day, 9, 0, 0, tzinfo=EASTERN)
    return format_datetime(send_time_et)

def send_mailgun_summary(
    run_date_str: str,
    entities_to_alert: List[Dict[str, Any]],
    MAILGUN_API_KEY: str,
    MAILGUN_DOMAIN: str,
    MAILGUN_FROM: str,
    MAILGUN_TO: List[str],
    entity_type: str = "Brand",
    region: str | None = None,
    schedule_mode: str | None = None,
) -> bool:
    """
    Send a single Mailgun email summarizing all entities that met the threshold.
    For CEOs, show 'CEO Name (Company)' in the list.
    """
    if not (MAILGUN_API_KEY and MAILGUN_DOMAIN and MAILGUN_FROM and MAILGUN_TO):
        print("Mailgun config missing; skipping.")
        return False

    # Subject
    count = len(entities_to_alert)
    plural = f"{entity_type}s" if count != 1 else entity_type
    subject = f"High Negative Sentiment Alert for {count} {plural}"

    # Body
    content_html = [f"<p>The following {plural.lower()} have high negative sentiment for {run_date_str}:</p>", "<ul>"]
    for row in entities_to_alert:
        name = row.get("name")  # brand/company name
        ceo = row.get("ceo")    # may be None for Brand rows
        neg = row.get("neg")
        tot = row.get("tot")
        if not (name and isinstance(neg, int) and isinstance(tot, int) and tot > 0):
            continue
        pct = round((neg / tot) * 100)

        # If this is a CEO alert and we have a CEO name, display "CEO (Company)"
        display = f"{ceo} ({name})" if (entity_type == "CEO" and ceo) else name
        content_html.append(f"<li><strong>{display}:</strong> {neg}/{tot} ({pct}%) negative articles.</li>")
    content_html.append("</ul>")
    content_html = "\n".join(content_html)

    # Delivery time (same-morning vs next-morning @ 9am ET)
    delivery_time = _compute_delivery_time_rfc2822(run_date_str, schedule_mode)

    # Mailgun endpoint (US vs EU)
    base = "https://api.eu.mailgun.net" if (region or "").lower() == "eu" else "https://api.mailgun.net"
    url = f"{base}/v3/{MAILGUN_DOMAIN}/messages"
    data = {
        "from": MAILGUN_FROM,
        "to": MAILGUN_TO,
        "subject": subject,
        "html": content_html,
    }
    if delivery_time:
        data["o:deliverytime"] = delivery_time

    try:
        r = requests.post(url, auth=("api", MAILGUN_API_KEY), data=data, timeout=30)
        r.raise_for_status()
        print(f"Mailgun summary queued ({delivery_time or 'immediate'}).")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Mailgun error: {e}")
        return False


def check_and_send_alerts(
    entities: List[Dict[str, Any]],
    run_date: str,
    MAILGUN_API_KEY: str,
    MAILGUN_DOMAIN: str,
    MAILGUN_FROM: str,
    MAILGUN_TO: List[str],
    entity_type: str = "Brand",
    region: str | None = None,
    schedule_mode: str | None = None,
) -> None:
    last_alerts = read_last_alert_dates()
    today_str = now_eastern_date_str()
    to_alert: List[Dict[str, Any]] = []
    for row in entities:
        name, neg, tot = row.get("name") or row.get("brand"), row.get("neg") or row.get("negative", 0), row.get("tot") or row.get("total", 0)
        if not (name and tot):
            continue
        if (neg / tot) < NEGATIVE_THRESHOLD:
            continue
        key = f"{entity_type}:{name}"
        prev = last_alerts.get(key)
        if prev:
            try:
                days = (datetime.fromisoformat(today_str).date() - datetime.fromisoformat(prev).date()).days
                if days < ALERT_COOLDOWN_DAYS:
                    continue
            except Exception:
                pass
        ceo = row.get("ceo")  # new line
        to_alert.append(
            {"name": name, "neg": int(neg), "tot": int(tot), **({"ceo": ceo} if ceo else {})}
        )
    if to_alert:
        if send_mailgun_summary(run_date, to_alert, MAILGUN_API_KEY, MAILGUN_DOMAIN, MAILGUN_FROM, MAILGUN_TO, entity_type, region, schedule_mode):
            for e in to_alert:
                last_alerts[f"{entity_type}:{e['name']}"] = today_str
            write_last_alert_dates(last_alerts)
    else:
        print("No entities meet threshold or are in cooldown.")
