"""BSE daily Equity Bhavcopy -> BigQuery.

Mirrors bhavcopy.py's shape (same fetch/dedup/append flow), but BSE has two
real differences from NSE's sec_bhavdata_full that this module has to work
around:

1. No delivery-quantity/delivery-% columns -- BSE doesn't publish those in
   this file, unlike NSE's DELIV_QTY/DELIV_PER.
2. A non-trading day does NOT 404. BSE returns HTTP 200 with its own
   homepage HTML in place of the CSV, so "was there a file today" has to be
   detected from the response Content-Type, not the status code.

Repeatable *process* only. Every value (project, dataset, table, dates) comes
from `cfg` and the arguments the calling file passes to `run_bse_bhavcopy`.

Source: https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{YYYYMMDD}_F_0000.CSV
This is BSE's adoption of the SEBI-mandated unified bhavcopy format -- verified
live it starts exactly 2024-01-01; nothing before that date exists at this URL.
"""
import io
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

# ---- Process constants (part of the pipeline, not your setup) ----
BASE_URL = "https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{date}_F_0000.CSV"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
}
COLUMN_MAP = {
    "TradDt": "date", "TckrSymb": "symbol", "SctySrs": "series", "ISIN": "isin",
    "FinInstrmId": "scrip_code", "FinInstrmNm": "security_name",
    "PrvsClsgPric": "prev_close", "OpnPric": "open_price", "HghPric": "high_price",
    "LwPric": "low_price", "LastPric": "last_price", "ClsPric": "close_price",
    "TtlTradgVol": "ttl_trd_qnty", "TtlTrfVal": "turnover", "TtlNbOfTxsExctd": "no_of_trades",
}
NUMERIC_COLS = [c for c in COLUMN_MAP.values()
               if c not in ("symbol", "series", "isin", "scrip_code", "security_name", "date")]

DATE_FMT = "%Y-%m-%d"   # the format the calling file uses for start/end dates
EARLIEST_DATE = datetime(2024, 1, 1)  # verified: nothing exists before this at this URL


def fetch_one(day, session):
    """Download + parse one day's BSE bhavcopy. Returns None if there's no
    genuine data for this date (weekend/holiday, or before EARLIEST_DATE).

    Unlike NSE, BSE serves HTTP 200 with its own homepage HTML in place of
    the CSV on a non-trading day -- there's no 404 to catch, so a non-trading
    day is detected from the response Content-Type instead.
    """
    url = BASE_URL.format(date=day.strftime("%Y%m%d"))
    resp = session.get(url, headers=HEADERS, timeout=15)
    if resp.status_code != 200:
        return None
    content_type = resp.headers.get("Content-Type", "")
    if "html" in content_type.lower():
        return None  # BSE's homepage fallback -- no bhavcopy for this date

    df = pd.read_csv(io.StringIO(resp.text))
    df.columns = [c.strip() for c in df.columns]
    df = df[df["Sgmt"] == "CM"]  # this file also happens to carry only CM rows, but be explicit
    df = df.rename(columns=COLUMN_MAP)
    df = df[[c for c in COLUMN_MAP.values() if c in df.columns]]

    for col in ("symbol", "series", "isin", "security_name"):
        df[col] = df[col].astype(str).str.strip()
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["date"] = pd.to_datetime(df["date"]).dt.strftime(DATE_FMT)

    requested = day.strftime(DATE_FMT)
    if (df["date"] != requested).any():
        return None  # stale file served under this date's URL -- discard
    return df


def fetch_range(start, end, delay=0.5):
    """Fetch + parse BSE bhavcopy for every calendar day in [start, end].
    Non-trading days, and anything before EARLIEST_DATE, are silently skipped."""
    frames = []
    with requests.Session() as session:
        day = max(start, EARLIEST_DATE)
        if day > start:
            print(f"Note: BSE unified bhavcopy only exists from {EARLIEST_DATE.strftime(DATE_FMT)} "
                  f"onward -- clamping start date up from {start.strftime(DATE_FMT)}.")
        while day <= end:
            df = fetch_one(day, session)
            if df is not None:
                frames.append(df)
            day += timedelta(days=1)
            time.sleep(delay)  # be polite to BSE's host
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


def run_bse_bhavcopy(cfg, start_str, end_str, delay=0.5):
    """Fetch BSE Equity Bhavcopy for [start_str, end_str] (both 'YYYY-MM-DD'),
    dedup against BigQuery, and append only new (date, symbol, series) rows.

    Reads project/dataset/table from cfg (cfg.bse_bhav_ref). All values stay
    in the caller; this is just the process. Dates before 2024-01-01 are
    clamped up (see EARLIEST_DATE) since BSE has no data there at this URL.
    """
    from google.cloud import bigquery
    from .auth import bq_client

    cfg.require("project_id", "dataset_id", "bse_bhav_table")
    table_id = cfg.bse_bhav_ref

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
