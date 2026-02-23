import math
import os
from datetime import datetime
from typing import List, Tuple

import psycopg2
from psycopg2.extras import execute_values

from ingest_v2 import ingest_article_mentions_rows, ingest_serp_results_rows


def get_conn():
    dsn = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not dsn:
        print("[WARN] DATABASE_URL not set; skipping DB upsert.")
        return None
    return psycopg2.connect(dsn)


def _is_nan(value) -> bool:
    return isinstance(value, float) and math.isnan(value)


def _sanitize_value(value):
    if value is None:
        return ""
    if _is_nan(value):
        return ""
    return value


def _normalize_llm_fields(row: dict) -> dict:
    llm_label = str(row.get("llm_label") or "").strip()
    llm_sentiment_label = str(row.get("llm_sentiment_label") or "").strip()
    if llm_sentiment_label and not llm_label:
        row["llm_label"] = llm_sentiment_label
    if llm_label and not llm_sentiment_label:
        row["llm_sentiment_label"] = llm_label
    return row


def _df_to_records(df) -> list[dict]:
    records = []
    for record in df.to_dict(orient="records"):
        cleaned = {key: _sanitize_value(value) for key, value in record.items()}
        records.append(_normalize_llm_fields(cleaned))
    return records


def upsert_articles_mentions(df, entity_type: str, date_str: str) -> int:
    conn = get_conn()
    if conn is None or df is None or df.empty:
        return 0
    try:
        return ingest_article_mentions_rows(conn, _df_to_records(df), entity_type, date_str)
    finally:
        conn.close()


def upsert_serp_results(df, entity_type: str, date_str: str) -> int:
    conn = get_conn()
    if conn is None or df is None or df.empty:
        return 0
    try:
        return ingest_serp_results_rows(conn, _df_to_records(df), entity_type, date_str)
    finally:
        conn.close()


def _parse_dates_and_values(date_history: str, value_history: str) -> List[Tuple]:
    dates = [d.strip() for d in (date_history or "").split("|") if d.strip()]
    values = [v.strip() for v in (value_history or "").split("|") if v.strip()]
    if not dates or not values or len(dates) != len(values):
        return []
    out = []
    for d, v in zip(dates, values):
        try:
            dval = datetime.strptime(d, "%Y-%m-%d").date()
        except ValueError:
            continue
        try:
            fval = float(v)
        except ValueError:
            fval = None
        out.append((dval, fval))
    return out


def upsert_stock_df(df) -> int:
    conn = get_conn()
    if conn is None or df is None or df.empty:
        return 0

    daily_rows = []
    snapshot_rows = []

    for _, row in df.iterrows():
        ticker = str(row.get("ticker", "") or "").strip()
        company = str(row.get("company", "") or "").strip()
        if not ticker or not company:
            continue
        series = _parse_dates_and_values(row.get("date_history"), row.get("price_history"))
        for dval, pval in series:
            if pval is None:
                continue
            daily_rows.append((ticker, company, dval, pval))

        last_updated = row.get("last_updated")
        try:
            last_ts = datetime.fromisoformat(str(last_updated)) if last_updated else None
        except ValueError:
            last_ts = None

        def to_float(val):
            try:
                return float(val) if val not in (None, "") else None
            except ValueError:
                return None

        opening_price = to_float(row.get("opening_price"))
        daily_change_pct = to_float(row.get("daily_change"))
        if daily_change_pct is None:
            daily_change_pct = to_float(row.get("daily_change_pct"))
        seven_day_change_pct = to_float(row.get("seven_day_change"))
        if seven_day_change_pct is None:
            seven_day_change_pct = to_float(row.get("seven_day_change_pct"))

        prices_only = [p for _, p in series if p is not None]
        if daily_change_pct is None and len(prices_only) >= 2:
            prev = prices_only[-2]
            last = prices_only[-1]
            if prev:
                daily_change_pct = ((last - prev) / prev) * 100
        if seven_day_change_pct is None and len(prices_only) >= 8:
            prev7 = prices_only[-8]
            last = prices_only[-1]
            if prev7:
                seven_day_change_pct = ((last - prev7) / prev7) * 100

        snapshot_rows.append((
            ticker, company, last_ts.date() if last_ts else None, opening_price,
            daily_change_pct, seven_day_change_pct, last_ts
        ))

    if not daily_rows and not snapshot_rows:
        conn.close()
        return 0

    with conn:
        with conn.cursor() as cur:
            if daily_rows:
                execute_values(cur, """
                    insert into stock_prices_daily (ticker, company, date, price)
                    values %s
                    on conflict (ticker, date) do update set
                      price = excluded.price,
                      company = excluded.company
                """, daily_rows, page_size=1000)
            if snapshot_rows:
                execute_values(cur, """
                    insert into stock_price_snapshots (
                      ticker, company, as_of_date, opening_price,
                      daily_change_pct, seven_day_change_pct, last_updated
                    ) values %s
                    on conflict (ticker, last_updated) do update set
                      opening_price = excluded.opening_price,
                      daily_change_pct = excluded.daily_change_pct,
                      seven_day_change_pct = excluded.seven_day_change_pct,
                      as_of_date = excluded.as_of_date,
                      company = excluded.company
                """, snapshot_rows, page_size=1000)

    conn.close()
    return len(daily_rows)


def upsert_trends_df(df) -> int:
    conn = get_conn()
    if conn is None or df is None or df.empty:
        return 0

    daily_rows = []
    snapshot_rows = []

    for _, row in df.iterrows():
        company = str(row.get("company", "") or "").strip()
        if not company:
            continue
        date_history = row.get("date_history")
        trends_history = row.get("trends_history")
        last_updated = row.get("last_updated")
        avg_interest = row.get("avg_interest")
        if avg_interest in (None, ""):
            avg_interest = row.get("average_interest")

        for dval, ival in _parse_dates_and_values(date_history, trends_history):
            if ival is None:
                continue
            daily_rows.append((company, dval, int(ival)))

        try:
            last_ts = datetime.fromisoformat(str(last_updated)) if last_updated else None
        except ValueError:
            last_ts = None
        try:
            avg_val = float(avg_interest) if avg_interest not in (None, "") else None
        except ValueError:
            avg_val = None
        snapshot_rows.append((company, avg_val, last_ts))

    if not daily_rows and not snapshot_rows:
        conn.close()
        return 0

    with conn:
        with conn.cursor() as cur:
            if daily_rows:
                deduped = {}
                for company, dval, ival in daily_rows:
                    deduped[(company, dval)] = ival
                daily_rows = [(c, d, i) for (c, d), i in deduped.items()]
                execute_values(cur, """
                    insert into trends_daily (company, date, interest)
                    values %s
                    on conflict (company, date) do update set
                      interest = excluded.interest
                """, daily_rows, page_size=1000)
            if snapshot_rows:
                deduped = {}
                for company, avg_val, last_ts in snapshot_rows:
                    if last_ts is None:
                        continue
                    deduped[(company, last_ts)] = avg_val
                snapshot_rows = [(c, v, ts) for (c, ts), v in deduped.items()]
                execute_values(cur, """
                    insert into trends_snapshots (company, avg_interest, last_updated)
                    values %s
                    on conflict (company, last_updated) do update set
                      avg_interest = excluded.avg_interest
                """, snapshot_rows, page_size=1000)

    conn.close()
    return len(daily_rows)
