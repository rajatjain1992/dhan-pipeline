"""NSE PR-zip market-cap (mcap*.csv) -> BigQuery.

Repeatable *process* only. Every value (project, dataset, table, dates) comes
from `cfg` and the arguments the calling file passes to `run_mcap`.

Source: https://nsearchives.nseindia.com/archives/equities/bhavcopy/pr/PR{DDMMYY}.zip
NOTE the zip filename uses a 2-digit year (DDMMYY), unlike sec_bhavdata_full
(DDMMYYYY). Only the mcap*.csv member of the zip is used. A non-trading day
returns a genuine 404 and is silently skipped.
"""
import io
import time
import zipfile
from datetime import datetime, timedelta

import pandas as pd
import requests

# ---- Process constants (part of the pipeline, not your setup) ----
BASE_URL = "https://nsearchives.nseindia.com/archives/equities/bhavcopy/pr/PR{date}.zip"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
}
COLUMN_MAP = {
    "Trade Date": "trade_date", "Symbol": "symbol", "Series": "series",
    "Security Name": "security_name", "Category": "category",
    "Last Trade Date": "last_trade_date", "Face Value(Rs.)": "face_value",
    "Issue Size": "issue_size", "Close Price/Paid up value(Rs.)": "close_price",
    "Market Cap(Rs.)": "market_cap",
}
STRING_COLS = ["symbol", "series", "security_name", "category"]
NUMERIC_COLS = ["face_value", "issue_size", "close_price", "market_cap"]

DATE_FMT = "%Y-%m-%d"   # the format the calling file uses for start/end dates


def fetch_one(day, session):
    """Download the PR zip for one date, extract mcap*.csv, and parse it.
    Returns None on 404 (non-trading day) or if the zip/member is missing."""
    url = BASE_URL.format(date=day.strftime("%d%m%y"))
    resp = session.get(url, headers=HEADERS, timeout=20)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()

    try:
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
    except zipfile.BadZipFile:
        return None

    mcap_name = next((n for n in zf.namelist() if n.lower().startswith("mcap")), None)
    if mcap_name is None:
        return None

    with zf.open(mcap_name) as f:
        df = pd.read_csv(f)
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns=COLUMN_MAP)
    df = df[[c for c in COLUMN_MAP.values() if c in df.columns]]

    for col in STRING_COLS + ["trade_date", "last_trade_date"]:
        df[col] = df[col].astype(str).str.strip()
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["trade_date"] = pd.to_datetime(df["trade_date"], format="%d %b %Y").dt.strftime(DATE_FMT)
    df["last_trade_date"] = pd.to_datetime(
        df["last_trade_date"], format="%d %b %Y", errors="coerce"
    ).dt.strftime(DATE_FMT)

    requested = day.strftime(DATE_FMT)
    if (df["trade_date"] != requested).any():
        return None  # safety net, in case NSE ever serves a stale file here too
    return df


def fetch_range(start, end, delay=0.5):
    """Fetch + parse mcap data for every calendar day in [start, end].
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
        f"SELECT DISTINCT trade_date, symbol, series FROM `{table_id}` "
        f"WHERE trade_date BETWEEN '{df['trade_date'].min()}' AND '{df['trade_date'].max()}'"
    ).to_dataframe()
    if existing.empty:
        return df
    df = df.merge(existing, on=["trade_date", "symbol", "series"], how="left", indicator=True)
    return df[df["_merge"] == "left_only"].drop(columns="_merge")


def run_mcap(cfg, start_str, end_str, delay=0.5):
    """Fetch NSE PR-zip market cap for [start_str, end_str] (both 'YYYY-MM-DD'),
    dedup against BigQuery, and append only new (trade_date, symbol, series) rows.

    Reads project/dataset/table from cfg (cfg.mcap_ref). All values stay in the
    caller; this is just the process.
    """
    from google.cloud import bigquery
    from .auth import bq_client

    cfg.require("project_id", "dataset_id", "mcap_table")
    table_id = cfg.mcap_ref

    start = datetime.strptime(start_str, DATE_FMT)
    end = datetime.strptime(end_str, DATE_FMT)
    if end < start:
        raise ValueError("End date must be on/after start date")

    client = bq_client(cfg)
    client.create_dataset(f"{cfg.project_id}.{cfg.dataset_id}", exists_ok=True)

    df = fetch_range(start, end, delay=delay)
    days = sorted(df["trade_date"].unique()) if not df.empty else []
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
