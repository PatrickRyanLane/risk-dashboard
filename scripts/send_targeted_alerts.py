#!/usr/bin/env python3
"""
Send targeted crisis alerts for specific brands (and their CEOs).
Uses the same gates and summary logic as send_crisis_alerts.py.
"""

import os
import json
from datetime import datetime, timedelta

import send_crisis_alerts as sca


def _norm(val: str) -> str:
    return sca.normalize_name(val or "").strip().lower()


def _parse_targets():
    raw = os.getenv("TARGET_BRANDS", "").strip()
    if not raw:
        return []
    return [b.strip() for b in raw.split(",") if b.strip()]


def _load_channel_map():
    raw = os.getenv("BRAND_CHANNEL_MAP", "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception:
        print("⚠️  BRAND_CHANNEL_MAP is not valid JSON; ignoring.")
        return {}
    out = {}
    for key, val in data.items():
        if not key or not val:
            continue
        out[_norm(key)] = str(val).strip()
    return out


def main():
    channel_map = _load_channel_map()
    default_channel = os.getenv("TARGET_DEFAULT_CHANNEL", sca.SLACK_CHANNEL)
    targets = _parse_targets()
    if not targets:
        if channel_map:
            targets = list(channel_map.keys())
        else:
            print("No TARGET_BRANDS or BRAND_CHANNEL_MAP provided. Exiting.")
            return 0

    sca.DRY_RUN = os.getenv("DRY_RUN", "0") == "1"

    df = sca.load_negative_summary_db(sca.NEGATIVE_HISTORY_DAYS)
    if df is None or df.empty:
        print("No DB negative summary data found. Exiting.")
        return 0

    targets_norm = {_norm(t) for t in targets}
    df["company_norm"] = df["company"].apply(_norm)
    df = df[df["company_norm"].isin(targets_norm)]

    if df.empty:
        print("No matching targeted brands found in summary.")
        return 0

    conn = sca.get_db_conn()
    if conn is None:
        print("No DB connection available. Exiting.")
        return 0

    history = sca.load_alert_history_db(conn)
    llm_cache = sca.load_llm_cache_db(conn, datetime.utcnow().date().isoformat())

    serp_brand_counts = {}
    serp_ceo_counts = {}
    top_stories_brand = {}
    top_stories_ceo = {}
    top_stories_brand_items = {}
    top_stories_ceo_items = {}
    if sca.SERP_GATE_ENABLED:
        b_unctrl, c_unctrl, b_neg, c_neg = sca.load_serp_counts_db(sca.SERP_GATE_DAYS)
        serp_brand_counts = b_unctrl
        serp_ceo_counts = c_unctrl
        top_stories_brand, top_stories_ceo = sca.load_top_stories_counts_db(sca.SERP_GATE_DAYS)
        top_stories_brand_items, top_stories_ceo_items = sca.load_top_stories_items_db(sca.SERP_GATE_DAYS)

    alerts_remaining_today = sca.MAX_ALERTS_PER_DAY
    updates_made = False
    llm_calls = 0

    skip_cooldown = os.getenv("TARGET_SKIP_COOLDOWN", "1") == "1"
    for _, row in df.iterrows():
        if alerts_remaining_today <= 0:
            print("🛑 Daily limit hit mid-run. Stopping alerts for today.")
            break

        brand = row["company"]
        count = row["negative_count"]
        headlines = row["top_headlines"]
        article_type = str(row.get("article_type", "brand")).lower().strip()
        ceo_name = str(row.get("ceo", "")).strip()

        # Always include CEOs for target brands
        if article_type == "brand":
            pass
        elif article_type == "ceo":
            pass
        else:
            continue

        date_str = str(row["date"])
        try:
            row_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            continue
        server_now = datetime.now().date()
        if row_date not in {server_now, server_now - timedelta(days=1)}:
            continue

        if sca.SERP_GATE_ENABLED:
            if article_type == "ceo":
                serp_count = serp_ceo_counts.get((brand, ceo_name), 0)
                top_total, top_neg = top_stories_ceo.get(ceo_name, (0, 0))
            else:
                serp_count = serp_brand_counts.get(brand, 0)
                top_total, top_neg = top_stories_brand.get(brand, (0, 0))
            if sca.SERP_TOP_STORIES_REQUIRED and top_total <= 0:
                continue
            if top_neg < sca.SERP_TOP_STORIES_NEG_MIN:
                continue
            if serp_count < sca.SERP_GATE_MIN:
                continue

        history_key = f"{brand}_{article_type}"
        last_alert = history.get(history_key)
        if not last_alert and article_type == "brand":
            last_alert = history.get(brand)
        if last_alert and not skip_cooldown:
            last_date = datetime.fromisoformat(last_alert)
            if datetime.utcnow() - last_date < timedelta(hours=sca.ALERT_COOLDOWN_HOURS):
                continue

        owner_email, owner_name = sca.get_salesforce_owner(brand)
        slack_id = sca.get_slack_user_id(owner_email)

        summary_text = ""
        llm_key = f"{brand}|{ceo_name}|{article_type}|{date_str}"
        if sca.LLM_API_KEY and llm_calls < sca.LLM_SUMMARY_MAX_CALLS:
            if llm_key in llm_cache:
                summary_text = llm_cache.get(llm_key, "")
            else:
                if article_type == "ceo":
                    top_items = top_stories_ceo_items.get((date_str, ceo_name), [])
                else:
                    top_items = top_stories_brand_items.get((date_str, brand), [])
                top_titles = [i.get("title", "").strip().strip('"') for i in top_items if i.get("title")]
                if not top_titles:
                    raw_heads = str(headlines).split("|")
                    top_titles = [h.strip().strip('"') for h in raw_heads if h.strip()]
                prompt = sca.build_summary_prompt(
                    article_type,
                    ceo_name if article_type == "ceo" else brand,
                    top_titles[:5],
                )
                summary_text = sca.call_llm_text(prompt, sca.LLM_API_KEY, sca.LLM_MODEL)
                llm_cache[llm_key] = summary_text
                llm_calls += 1

        if article_type == "ceo":
            top_items = top_stories_ceo_items.get((date_str, ceo_name), [])
        else:
            top_items = top_stories_brand_items.get((date_str, brand), [])

        channel = channel_map.get(_norm(brand), default_channel)
        sca.send_slack_alert(
            brand,
            ceo_name,
            article_type,
            count,
            0,
            headlines,
            top_items,
            slack_id,
            owner_name,
            summary_text,
            None,
            channel=channel,
        )

        jitter_seconds = 0 if sca.DRY_RUN else int(os.getenv("ALERT_JITTER_SECONDS", "0"))
        effective_timestamp = datetime.utcnow() + timedelta(seconds=jitter_seconds)
        if sca.DRY_RUN:
            print(f"👀 [DRY RUN] Would send targeted alert for {brand} ({article_type}).")
        else:
            history[history_key] = effective_timestamp.isoformat()
            updates_made = True
            alerts_remaining_today -= 1

    if updates_made and not sca.DRY_RUN:
        sca.upsert_alert_history_db(conn, history)
        sca.upsert_llm_cache_db(conn, llm_cache)
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
