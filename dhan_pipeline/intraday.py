"""Dhan intraday minute/15m OHLCV -> BigQuery.

Fetches intraday candles for every scrip in a mapping across a series of date
windows (n windows of `window_days`, stepping `step_days` apart), cleans the
rows, dedups against what's already in BigQuery, and appends only new rows.

Repeatable *process* only. Every value (token, project, dataset, table, the
scrip mapping, and all the date/interval knobs) is supplied by the caller.
"""
import time
from datetime import date, datetime, timedelta

import requests
import pandas as pd
from tqdm.auto import tqdm

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
    each starting `step_days` after the previous. All 'YYYY-MM-DD'.

    NOTE: if step_days > window_days, there are GAPS between windows -- days
    fall through uncovered. This is fine for run_intraday's original use
    (sampling recent weeks), but NOT safe for a full-history reload. Use
    build_contiguous_windows for that instead.
    """
    start = datetime.strptime(start_date, DATE_FMT) if isinstance(start_date, str) else start_date
    windows = []
    for i in range(n):
        f = start + timedelta(days=i * step_days)
        t = f + timedelta(days=window_days)
        windows.append((f.strftime(DATE_FMT), t.strftime(DATE_FMT)))
    return windows


def build_contiguous_windows(start_date, end_date, window_days):
    """Return [(from_str, to_str), ...] chunks of `window_days` that exactly
    tile [start_date, end_date] with NO gaps and NO manual n/step math --
    the count of windows is computed automatically from the span. The last
    window is clipped to end_date so it never overshoots.
    """
    start = start_date if isinstance(start_date, date) else datetime.strptime(start_date, DATE_FMT).date()
    end = end_date if isinstance(end_date, date) else datetime.strptime(end_date, DATE_FMT).date()
    if end < start:
        raise ValueError(f"end_date {end} is before start_date {start}")

    windows = []
    cur = start
    while cur <= end:
        window_end = min(cur + timedelta(days=window_days), end + timedelta(days=1))
        windows.append((cur.strftime(DATE_FMT), window_end.strftime(DATE_FMT)))
        cur = cur + timedelta(days=window_days)
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
        with tqdm(total=total, desc=f"{from_date} -> {to_date}", unit="scrip") as bar:
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
                    tqdm.write(f"  ✗ {row[scrip_col]}: {err}")

                bar.update(1)
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


def flagged_scrip_date_bounds(cfg, flagged_scrips, interval=15):
    """Which of `flagged_scrips` actually have interval_m=`interval` rows in
    the intraday table, plus the OVERALL oldest/newest candle date across
    just those eligible scrips.

    Returns (eligible_scrips, oldest_date, newest_date). eligible_scrips is
    empty (and the dates None) if none of the flagged scrips are in the
    intraday table at this interval.
    """
    from google.cloud import bigquery
    cfg.require("project_id", "dataset_id", "intraday_table")
    if not flagged_scrips:
        return [], None, None

    bq = bq_client(cfg)
    try:
        df = bq.query(
            f"SELECT scrip, MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts "
            f"FROM `{cfg.intraday_ref}` "
            f"WHERE interval_m = @interval AND scrip IN UNNEST(@scrips) "
            f"GROUP BY scrip",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("interval", "INT64", int(interval)),
                bigquery.ArrayQueryParameter("scrips", "STRING", list(flagged_scrips)),
            ]),
        ).to_dataframe()
    except Exception as e:
        print(f"Intraday table not found / not readable: {e}")
        return [], None, None

    if df.empty:
        return [], None, None

    eligible = sorted(df["scrip"].unique())
    oldest = pd.to_datetime(int(df["min_ts"].min()), unit="s", utc=True) \
        .tz_convert("Asia/Kolkata").date()
    newest = pd.to_datetime(int(df["max_ts"].max()), unit="s", utc=True) \
        .tz_convert("Asia/Kolkata").date()
    return eligible, oldest, newest


def delete_scrips_interval(bq, table_ref, scrips, interval):
    from google.cloud import bigquery
    if not scrips:
        return
    bq.query(
        f"DELETE FROM `{table_ref}` WHERE interval_m = @interval AND scrip IN UNNEST(@scrips)",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("interval", "INT64", int(interval)),
            bigquery.ArrayQueryParameter("scrips", "STRING", list(scrips)),
        ]),
    ).result()


def run_intraday_reload_for_flagged(cfg, scrip_mapping, flagged_scrips, interval=15,
                                    window_days=5, pause_s=0.5, dry_run=False,
                                    scrip_col="scrip", security_id_col="security_id",
                                    exchange_col="exc_seg", instrument_col="instrument_type"):
    """After a split-check flags some scrips (e.g. from run_daily_reload's
    `flagged_scrips`), reload their intraday history at `interval` minutes.

    Only scrips that ALREADY have interval_m=`interval` rows are touched --
    a flagged scrip that was never fetched at this interval is skipped (there
    is nothing stale to fix). For eligible scrips: their entire existing span
    (oldest -> newest candle currently in BigQuery) is deleted and refetched,
    using build_contiguous_windows so the window count is computed
    automatically from the span -- no window_days/step_days/n arithmetic to
    get wrong and silently miss days.

    Args:
        cfg: Config with dhan creds, project_id, dataset_id, intraday_table.
        scrip_mapping: full scrip list (scrip, security_id, exc_seg,
            instrument_type) -- used to look up eligible scrips' API params.
        flagged_scrips: scrip names to check/reload (e.g. from
            run_daily_reload(...)["flagged_scrips"]).
        interval: candle interval in minutes to check/reload (your ask: 15).
        window_days: size of each contiguous fetch chunk.
        dry_run: if True, reports eligible scrips + span + window count but
            does not delete or fetch anything.
    """
    cfg.require("project_id", "dataset_id", "intraday_table",
                "dhan_client_id", "dhan_access_token")
    table_ref = cfg.intraday_ref

    if not flagged_scrips:
        print("No flagged scrips given -- nothing to do.")
        return {"eligible": [], "not_eligible": [], "loaded": 0, "table": table_ref}

    eligible, oldest, newest = flagged_scrip_date_bounds(cfg, flagged_scrips, interval)
    not_eligible = [s for s in flagged_scrips if s not in eligible]

    print(f"{len(flagged_scrips)} flagged scrip(s); "
          f"{len(eligible)} have interval_m={interval} data, "
          f"{len(not_eligible)} do not (skipped): {not_eligible}")

    if not eligible:
        return {"eligible": [], "not_eligible": not_eligible, "loaded": 0, "table": table_ref}

    windows = build_contiguous_windows(oldest, newest, window_days)
    print(f"Eligible span: {oldest} -> {newest}  "
          f"({len(windows)} contiguous {window_days}d window(s), auto-computed)")

    if dry_run:
        print(f"[DRY RUN] would delete existing interval_m={interval} rows for "
              f"{len(eligible)} scrip(s): {eligible}")
        print(f"[DRY RUN] would fetch {len(windows)} window(s) x {len(eligible)} scrip(s) "
              f"and reload {table_ref}.")
        return {"eligible": eligible, "not_eligible": not_eligible, "loaded": 0,
                "oldest": oldest, "newest": newest, "windows": len(windows),
                "table": table_ref, "dry_run": True}

    bq = bq_client(cfg)
    ensure_table(bq, table_ref)

    print(f"Deleting existing interval_m={interval} rows for {len(eligible)} scrip(s)...")
    delete_scrips_interval(bq, table_ref, eligible, interval)

    sub_mapping = scrip_mapping[scrip_mapping[scrip_col].isin(eligible)]

    frames, success, failure = [], 0, 0
    for from_date, to_date in windows:
        with tqdm(total=len(sub_mapping), desc=f"{from_date} -> {to_date}", unit="scrip") as bar:
            for _, row in sub_mapping.iterrows():
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
                    tqdm.write(f"  ✗ {row[scrip_col]}: {err}")

                bar.update(1)
                time.sleep(pause_s)

    print(f"\nAPI: {success} ok / {failure} failed")

    if not frames:
        print("Nothing fetched.")
        return {"eligible": eligible, "not_eligible": not_eligible, "loaded": 0,
                "failed": failure, "table": table_ref}

    all_data = clean(pd.concat(frames, ignore_index=True))
    print(f"Fetched {len(all_data)} clean candles.")

    from google.cloud import bigquery
    bq.load_table_from_dataframe(
        all_data, table_ref,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    ).result()
    print(f"✅ Loaded {len(all_data)} rows into {table_ref} for {len(eligible)} scrip(s).")

    return {"eligible": eligible, "not_eligible": not_eligible, "loaded": len(all_data),
            "failed": failure, "oldest": oldest, "newest": newest,
            "windows": len(windows), "table": table_ref, "dry_run": False}
