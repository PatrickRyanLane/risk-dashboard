#!/usr/bin/env python3
import csv
from datetime import datetime

from psycopg2.extras import execute_values


def parse_dates_and_values(date_history, value_history):
    dates = [d.strip() for d in (date_history or "").split("|") if d.strip()]
    values = [v.strip() for v in (value_history or "").split("|") if v.strip()]
    if not dates or not values:
        return []
    if len(dates) != len(values):
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


def ingest_trends_csv(conn, file_obj):
    reader = csv.DictReader(file_obj)
    daily_rows = []
    snapshot_rows = []

    for row in reader:
        company = (row.get("company") or "").strip()
        if not company:
            continue
        date_history = row.get("date_history")
        trends_history = row.get("trends_history")
        last_updated = row.get("last_updated")
        avg_interest = row.get("avg_interest")

        for dval, ival in parse_dates_and_values(date_history, trends_history):
            if ival is None:
                continue
            daily_rows.append((company, dval, int(ival)))

        if last_updated:
            try:
                last_ts = datetime.fromisoformat(last_updated)
            except ValueError:
                last_ts = None
        else:
            last_ts = None
        try:
            avg_val = float(avg_interest) if avg_interest not in (None, "") else None
        except ValueError:
            avg_val = None

        snapshot_rows.append((company, avg_val, last_ts))

    if daily_rows:
        deduped = {}
        for company, dval, ival in daily_rows:
            deduped[(company, dval)] = ival
        daily_rows = [(c, d, i) for (c, d), i in deduped.items()]
        sql = """
            insert into trends_daily (company, date, interest)
            values %s
            on conflict (company, date) do update set
              interest = excluded.interest
        """
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, daily_rows, page_size=1000)

    if snapshot_rows:
        deduped = {}
        for company, avg_val, last_ts in snapshot_rows:
            if last_ts is None:
                continue
            deduped[(company, last_ts)] = avg_val
        snapshot_rows = [(c, v, ts) for (c, ts), v in deduped.items()]
        sql = """
            insert into trends_snapshots (company, avg_interest, last_updated)
            values %s
            on conflict (company, last_updated) do update set
              avg_interest = excluded.avg_interest
        """
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, snapshot_rows, page_size=1000)

    return len(daily_rows)


def ingest_stock_csv(conn, file_obj):
    reader = csv.DictReader(file_obj)
    daily_rows = []
    snapshot_rows = []

    for row in reader:
        ticker = (row.get("ticker") or "").strip()
        company = (row.get("company") or "").strip()
        if not ticker or not company:
            continue
        date_history = row.get("date_history")
        price_history = row.get("price_history")
        last_updated = row.get("last_updated")

        series = parse_dates_and_values(date_history, price_history)
        for dval, pval in series:
            if pval is None:
                continue
            daily_rows.append((ticker, company, dval, pval))

        if last_updated:
            try:
                last_ts = datetime.fromisoformat(last_updated)
            except ValueError:
                last_ts = None
        else:
            last_ts = None

        def to_float(val):
            try:
                return float(val) if val not in (None, "") else None
            except ValueError:
                return None

        opening_price = to_float(row.get("opening_price"))
        daily_change_pct = to_float(row.get("daily_change_pct"))
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

        snapshot_rows.append((ticker, company, last_ts.date() if last_ts else None, opening_price,
                              daily_change_pct, seven_day_change_pct, last_ts))

    if daily_rows:
        sql = """
            insert into stock_prices_daily (ticker, company, date, price)
            values %s
            on conflict (ticker, date) do update set
              price = excluded.price,
              company = excluded.company
        """
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, daily_rows, page_size=1000)

    if snapshot_rows:
        sql = """
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
        """
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, snapshot_rows, page_size=1000)

    return len(daily_rows)


def ingest_roster_csv(conn, file_obj):
    reader = csv.DictReader(file_obj)
    rows = []
    for row in reader:
        company = (row.get('Company') or row.get('company') or '').strip()
        if not company:
            continue
        ceo = (row.get('CEO') or row.get('ceo') or '').strip()
        ceo_alias = (row.get('CEO Alias') or row.get('ceo alias') or row.get('alias') or '').strip()
        websites = (row.get('Websites') or row.get('websites') or row.get('Website') or row.get('website') or '').strip()
        stock = (row.get('Stock') or row.get('stock') or '').strip()
        sector = (row.get('Sector') or row.get('sector') or '').strip()
        rows.append((ceo, company, ceo_alias, websites, stock, sector))

    if not rows:
        return 0

    sql = """
        insert into roster (ceo, company, ceo_alias, websites, stock, sector)
        values %s
        on conflict (company) do update set
          ceo = excluded.ceo,
          ceo_alias = excluded.ceo_alias,
          websites = excluded.websites,
          stock = excluded.stock,
          sector = excluded.sector
    """
    with conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
    return len(rows)


def ingest_boards_csv(conn, file_obj):
    reader = csv.DictReader(file_obj)
    rows = []
    for row in reader:
        ceo = (row.get('CEO') or row.get('ceo') or '').strip()
        company = (row.get('Company') or row.get('company') or '').strip()
        url = (row.get('URL') or row.get('url') or '').strip()
        source = (row.get('Source') or row.get('source') or '').strip()
        last_updated = (row.get('last_updated') or row.get('Last Updated') or '').strip()
        if not ceo or not url:
            continue
        try:
            last_ts = datetime.fromisoformat(last_updated) if last_updated else None
        except ValueError:
            last_ts = None
        domain = ''
        try:
            from urllib.parse import urlparse
            domain = (urlparse(url).hostname or '').replace('www.', '')
        except Exception:
            domain = ''
        rows.append((ceo, company, url, domain, source, last_ts))

    if not rows:
        return 0

    sql = """
        insert into boards (ceo_id, company_id, url, domain, source, last_updated)
        select ceo.id, c.id, v.url, v.domain, v.source, v.last_updated::timestamptz
        from (values %s) as v(ceo, company, url, domain, source, last_updated)
        join companies c on c.name = v.company
        join ceos ceo on ceo.name = v.ceo and ceo.company_id = c.id
        on conflict (ceo_id, url) do update set
          domain = excluded.domain,
          source = excluded.source,
          last_updated = excluded.last_updated
    """
    with conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
    return len(rows)
