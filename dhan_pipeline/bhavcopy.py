"""NSE daily Full Bhavcopy (sec_bhavdata_full) -> BigQuery.

Repeatable *process* only. Every value (project, dataset, table, dates) comes
from `cfg` and the arguments the calling file passes to `run_bhavcopy`.

Source: https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_DDMMYYYY.csv
A 404 (or a stale file whose own DATE1 != the date requested) means no trading
that day -- it is silently skipped.
"""
import io
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

# ---- Process constants (part of the pipeline, not your setup) ----
BASE_URL = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
}
COLUMN_MAP = {
    "SYMBOL": "symbol", "SERIES": "series", "DATE1": "date",
    "PREV_CLOSE": "prev_close", "OPEN_PRICE": "open_price", "HIGH_PRICE": "high_price",
    "LOW_PRICE": "low_price", "LAST_PRICE": "last_price", "CLOSE_PRICE": "close_price",
    "AVG_PRICE": "avg_price", "TTL_TRD_QNTY": "ttl_trd_qnty", "TURNOVER_LACS": "turnover_lacs",
    "NO_OF_TRADES": "no_of_trades", "DELIV_QTY": "deliv_qty", "DELIV_PER": "deliv_per",
}
NUMERIC_COLS = [c for c in COLUMN_MAP.values() if c not in ("symbol", "series", "date")]

DATE_FMT = "%Y-%m-%d"   # the format the calling file uses for start/end dates


def fetch_one(day, session):
    """Download + parse one day's bhavcopy. Returns None if there's no genuine
    data for this exact date (404, or a holiday where NSE serves the previous
    trading day's file under this URL -- caught via the file's own DATE1)."""
    url = BASE_URL.format(date=day.strftime("%d%m%Y"))
    resp = session.get(url, headers=HEADERS, timeout=15)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()

    df = pd.read_csv(io.StringIO(resp.text))
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns=COLUMN_MAP)
    df = df[[c for c in COLUMN_MAP.values() if c in df.columns]]

    for col in ("symbol", "series"):
        df[col] = df[col].astype(str).str.strip()
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["date"] = pd.to_datetime(
        df["date"].astype(str).str.strip(), format="%d-%b-%Y"
    ).dt.strftime(DATE_FMT)

    requested = day.strftime(DATE_FMT)
    if (df["date"] != requested).any():
        return None  # stale/holiday file served under this date's URL -- discard
    return df


def fetch_range(start, end, delay=0.5):
    """Fetch + parse bhavcopy for every calendar day in [start, end].
    Non-trading days (weekends/holidays) 404 and are silently skipped."""
    frames = []
    with requests.Session() as session:
        day = start
        while day <= end:
            df = fetch_one(day, session)
            if df is not None:
                frames.append(df)
            day += timedelta(days=1)
            time.sleep(delay)  # be polite to NSE's archive host
    if not frames:
        return pd.DataFrame(columns=list(COLUMN_MAP.values()))
    return pd.concat(frames, ignore_index=True)


def dedup_against_bq(client, table_id, df):
    """Drop rows already present in BigQuery for this date range."""
    try:
        client.get_table(table_id)
    except Exception:
        return df  # table doesn't exist yet -> everything is new

    if df.empty:
        return df
    existing = client.query(
        f"SELECT DISTINCT date, symbol, series FROM `{table_id}` "
        f"WHERE date BETWEEN '{df['date'].min()}' AND '{df['date'].max()}'"
    ).to_dataframe()
    if existing.empty:
        return df
    df = df.merge(existing, on=["date", "symbol", "series"], how="left", indicator=True)
    return df[df["_merge"] == "left_only"].drop(columns="_merge")


def run_bhavcopy(cfg, start_str, end_str, delay=0.5):
    """Fetch NSE Full Bhavcopy for [start_str, end_str] (both 'YYYY-MM-DD'),
    dedup against BigQuery, and append only new (date, symbol, series) rows.

    Reads project/dataset/table from cfg (cfg.bhav_ref). All values stay in the
    caller; this is just the process.
    """
    from google.cloud import bigquery
    from .auth import bq_client

    cfg.require("project_id", "dataset_id", "bhav_table")
    table_id = cfg.bhav_ref

    start = datetime.strptime(start_str, DATE_FMT)
    end = datetime.strptime(end_str, DATE_FMT)
    if end < start:
        raise ValueError("End date must be on/after start date")

    client = bq_client(cfg)
    client.create_dataset(f"{cfg.project_id}.{cfg.dataset_id}", exists_ok=True)

    df = fetch_range(start, end, delay=delay)
    days = sorted(df["date"].unique()) if not df.empty else []
    print(f"Fetched {len(df)} rows across {len(days)} trading day(s) "
          f"between {start_str} and {end_str}: {days}")

    df = dedup_against_bq(client, table_id, df)

    if df.empty:
        print("Nothing new to load (all rows already in BigQuery).")
        return {"fetched_days": days, "loaded": 0, "table": table_id}

    client.load_table_from_dataframe(
        df, table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True),
    ).result()
    print(f"Loaded {len(df)} new rows into {table_id}")
    return {"fetched_days": days, "loaded": len(df), "table": table_id}
