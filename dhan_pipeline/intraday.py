"""Dhan intraday minute/15m OHLCV -> BigQuery.

Fetches intraday candles for every scrip in a mapping across a series of date
windows (n windows of `window_days`, stepping `step_days` apart), cleans the
rows, dedups against what's already in BigQuery, and appends only new rows.

Repeatable *process* only. Every value (token, project, dataset, table, the
scrip mapping, and all the date/interval knobs) is supplied by the caller.
"""
import time
from datetime import datetime, timedelta

import requests
import pandas as pd

from .auth import bq_client

# ---- Process constants ----
INTRADAY_URL = "https://api.dhan.co/v2/charts/intraday"
DATE_FMT = "%Y-%m-%d"
COLUMNS = ["scrip", "exchange", "security_id", "timestamp", "interval_m",
           "open", "high", "low", "close", "volume"]
NUMERIC_COLS = ["open", "high", "low", "close", "volume"]


def _schema():
    from google.cloud import bigquery
    return [
        bigquery.SchemaField("scrip", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("exchange", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("security_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("timestamp", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("interval_m", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("open", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("high", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("low", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("close", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("volume", "FLOAT64", mode="NULLABLE"),
    ]


def build_windows(start_date, n, window_days, step_days):
    """Return [(from_str, to_str), ...]: n windows, each `window_days` long,
    each starting `step_days` after the previous. All 'YYYY-MM-DD'."""
    start = datetime.strptime(start_date, DATE_FMT) if isinstance(start_date, str) else start_date
    windows = []
    for i in range(n):
        f = start + timedelta(days=i * step_days)
        t = f + timedelta(days=window_days)
        windows.append((f.strftime(DATE_FMT), t.strftime(DATE_FMT)))
    return windows


def fetch_one(cfg, security_id, exchange_segment, instrument, interval, from_date, to_date):
    """One Dhan intraday REST call. Returns (DataFrame or None, error_or_None)."""
    headers = {
        "access-token": cfg.dhan_access_token,
        "client-id": cfg.dhan_client_id,
        "Content-Type": "application/json",
    }
    payload = {
        "securityId": str(security_id),
        "exchangeSegment": exchange_segment,
        "instrument": instrument,
        "interval": str(interval),
        "fromDate": from_date,
        "toDate": to_date,
    }
    try:
        r = requests.post(INTRADAY_URL, json=payload, headers=headers, timeout=30)
    except Exception as e:
        return None, f"request error: {e}"
    if r.status_code != 200:
        return None, f"HTTP {r.status_code}: {r.text[:200]}"

    raw = r.json()
    data = raw.get("data", raw)
    if not data or not data.get("timestamp"):
        return None, raw.get("remarks", raw)

    df = pd.DataFrame({
        "timestamp": data["timestamp"],
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"],
        "volume": data["volume"],
    })
    return df, None


def clean(df):
    """Strip scrip/exchange, coerce types, drop exact duplicate candles."""
    if df.empty:
        return df
    df = df.copy()
    df["scrip"] = df["scrip"].astype(str).str.strip()
    df["exchange"] = df["exchange"].astype(str).str.strip()
    df["security_id"] = df["security_id"].astype(str).str.strip()
    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce").astype("int64")
    df["interval_m"] = pd.to_numeric(df["interval_m"], errors="coerce").astype(int)
    for c in NUMERIC_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.drop_duplicates(subset=["security_id", "timestamp", "interval_m"])
    return df[COLUMNS]


def ensure_table(bq, table_ref):
    from google.cloud import bigquery
    try:
        bq.get_table(table_ref)
    except Exception:
        bq.create_table(bigquery.Table(table_ref, schema=_schema()))
        print(f"Created table {table_ref}")


def dedup_against_bq(bq, table_ref, df, interval):
    """Drop candles already in BQ (matched on security_id, timestamp, interval_m)."""
    from google.cloud import bigquery
    if df.empty:
        return df
    lo, hi = int(df["timestamp"].min()), int(df["timestamp"].max())
    try:
        existing = bq.query(
            f"SELECT DISTINCT security_id, timestamp, interval_m FROM `{table_ref}` "
            f"WHERE interval_m = @interval AND timestamp BETWEEN @lo AND @hi",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("interval", "INT64", int(interval)),
                bigquery.ScalarQueryParameter("lo", "INT64", lo),
                bigquery.ScalarQueryParameter("hi", "INT64", hi),
            ]),
        ).to_dataframe()
    except Exception:
        return df  # table missing / unreadable -> treat all as new
    if existing.empty:
        return df
    existing["security_id"] = existing["security_id"].astype(str)
    merged = df.merge(existing, on=["security_id", "timestamp", "interval_m"],
                      how="left", indicator=True)
    return merged[merged["_merge"] == "left_only"].drop(columns="_merge")


def latest_intraday_date(cfg, scrip=None, interval=None):
    """Most recent candle date already in the intraday table (or None).

    Pre-run coverage check. `scrip`/`interval` optionally narrow it.
    """
    from google.cloud import bigquery
    cfg.require("project_id", "dataset_id", "intraday_table")
    bq = bq_client(cfg)
    where, params = [], []
    if scrip:
        where.append("scrip = @scrip")
        params.append(bigquery.ScalarQueryParameter("scrip", "STRING", scrip))
    if interval:
        where.append("interval_m = @interval")
        params.append(bigquery.ScalarQueryParameter("interval", "INT64", int(interval)))
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    try:
        df = bq.query(
            f"SELECT TIMESTAMP_SECONDS(MAX(timestamp)) AS max_ts FROM `{cfg.intraday_ref}` {clause}",
            job_config=bigquery.QueryJobConfig(query_parameters=params),
        ).to_dataframe()
    except Exception as e:
        print(f"Intraday table not found / not readable: {e}")
        return None
    latest = df["max_ts"].iloc[0] if not df.empty else None
    if latest is None or pd.isna(latest):
        print(f"No data yet in {cfg.intraday_ref}"
              f"{' for ' + scrip if scrip else ''}.")
        return None
    print(f"Latest intraday candle in BQ{': ' + scrip if scrip else ''}: {latest}")
    return latest


def run_intraday(cfg, scrip_mapping, start_date="2024-01-01", n=9,
                 window_days=4, step_days=7, interval=15, pause_s=0.5,
                 scrip_col="scrip", security_id_col="security_id",
                 exchange_col="exc_seg", instrument_col="instrument_type"):
    """Fetch intraday candles for every scrip in `scrip_mapping` across n date
    windows, clean, dedup against BigQuery, and append only new rows.

    Args:
        cfg: Config with dhan creds, project_id, dataset_id, intraday_table.
        scrip_mapping: DataFrame with columns for scrip/security_id/exchange/
            instrument (names configurable via *_col args).
        start_date: 'YYYY-MM-DD' of the first window.
        n: number of windows.
        window_days: length of each window (your original "4").
        step_days: gap between window starts (your original "7").
        interval: candle interval in minutes (15, 5, 1, ...).
    """
    cfg.require("project_id", "dataset_id", "intraday_table",
                "dhan_client_id", "dhan_access_token")
    table_ref = cfg.intraday_ref

    windows = build_windows(start_date, n, window_days, step_days)
    total = len(scrip_mapping)
    print(f"{total} scrips x {n} windows "
          f"({window_days}d wide, {step_days}d step) @ {interval}m")

    bq = bq_client(cfg)
    ensure_table(bq, table_ref)

    frames, success, failure = [], 0, 0
    for from_date, to_date in windows:
        print(f"\nWindow {from_date} -> {to_date}")
        for _, row in scrip_mapping.iterrows():
            df, err = fetch_one(
                cfg, row[security_id_col], row[exchange_col],
                row[instrument_col], interval, from_date, to_date)
            if df is not None:
                df["scrip"] = row[scrip_col]
                df["exchange"] = row[exchange_col]
                df["security_id"] = str(row[security_id_col])
                df["interval_m"] = int(interval)
                frames.append(df[COLUMNS])
                success += 1
            else:
                failure += 1
                print(f"  ✗ {row[scrip_col]}: {err}")
            time.sleep(pause_s)

    print(f"\nAPI: {success} ok / {failure} failed")

    if not frames:
        print("Nothing fetched.")
        return {"fetched": 0, "loaded": 0, "failed": failure, "table": table_ref}

    all_data = clean(pd.concat(frames, ignore_index=True))
    print(f"Fetched {len(all_data)} clean candles.")

    all_data = dedup_against_bq(bq, table_ref, all_data, interval)
    if all_data.empty:
        print("Nothing new to load (all candles already in BigQuery).")
        return {"fetched": len(frames), "loaded": 0, "failed": failure, "table": table_ref}

    from google.cloud import bigquery
    bq.load_table_from_dataframe(
        all_data, table_ref,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    ).result()
    print(f"Loaded {len(all_data)} new rows into {table_ref}")
    return {"fetched": len(frames), "loaded": len(all_data),
            "failed": failure, "table": table_ref}
