"""NIFTY expired/rolling options intraday (Dhan) -> BigQuery.

Downloads ATM-N..ATM+N (CE + PE) for the chosen expiry codes for every trading
day in a range, in weekly tranches. Safe to kill and resume: it skips
(trade_date, expiry_code) pairs already in BigQuery.

Repeatable *process* only. Every value (token, project, dataset, table, dates,
the NSE expiry CSV path, and the strategy knobs) is supplied by the caller —
via `cfg` and the arguments to `run_options`. Nothing project-specific lives here.

Dhan docs: https://dhanhq.co/docs/v2/expired-options-data/
"""
import time
import concurrent.futures
from datetime import date, timedelta, datetime

import requests
import pandas as pd

from .auth import bq_client

# ---- Process constants (part of the pipeline, not your setup) ----
URL = "https://api.dhan.co/v2/charts/rollingoption"
OPTION_TYPES = [("CALL", "CE"), ("PUT", "PE")]
DATE_FMT = "%Y-%m-%d"

# NSE trading holidays used to skip non-trading weekdays. Extend as needed.
NSE_HOLIDAYS = {
    date(2025, 2, 26), date(2025, 3, 14), date(2025, 3, 31),
    date(2025, 4, 10), date(2025, 4, 14), date(2025, 4, 18),
    date(2025, 5, 1), date(2025, 8, 15), date(2025, 8, 27),
    date(2025, 10, 2), date(2025, 10, 22), date(2025, 11, 5),
    date(2025, 12, 25), date(2026, 1, 15), date(2026, 1, 26),
    date(2026, 3, 3), date(2026, 3, 26), date(2026, 3, 31),
    date(2026, 4, 3), date(2026, 4, 14), date(2026, 5, 1),
}


def _schema():
    from google.cloud import bigquery
    return [
        bigquery.SchemaField("trade_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("expiry_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("expiry_flag", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("expiry_code", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("security_ticker", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("security_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("interval", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("timestamp", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("strike_price", "NUMERIC", mode="NULLABLE"),
        bigquery.SchemaField("option_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("strike_offset", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("spot", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("open", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("high", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("low", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("close", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("volume", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("iv", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("oi", "INT64", mode="NULLABLE"),
    ]


# ---- Expiry lookup from the NSE CSV (ground truth) ----
def build_expiry_lookup(csv_path, expiry_codes):
    """{ trade_date: { expiry_code: {"expiry_date": date, "expiry_flag": str} } }"""
    print(f"Loading NSE CSV: {csv_path} ...")
    df = pd.read_csv(csv_path, usecols=["Date", "Expiry"])
    df["Date"] = pd.to_datetime(df["Date"], format="mixed", dayfirst=True)
    df["Expiry"] = pd.to_datetime(df["Expiry"], format="mixed", dayfirst=True)
    df = df.drop_duplicates()

    last_expiry_of_month = {}
    for e in sorted(df["Expiry"].dropna().unique()):
        e = pd.Timestamp(e)
        last_expiry_of_month[(e.year, e.month)] = e

    lookup = {}
    for trade_date, grp in df.groupby("Date"):
        future = sorted(e for e in grp["Expiry"].dropna().unique() if e >= trade_date)
        if not future:
            continue
        lookup[trade_date.date()] = {}
        for code in expiry_codes:
            idx = code - 1
            if idx >= len(future):
                continue
            exp = pd.Timestamp(future[idx])
            flag = "MONTH" if exp == last_expiry_of_month.get((exp.year, exp.month)) else "WEEK"
            lookup[trade_date.date()][code] = {"expiry_date": exp.date(), "expiry_flag": flag}

    print(f"  -> {len(lookup)} trade dates | "
          f"{sum(len(v) for v in lookup.values())} (date, expiry_code) pairs")
    return lookup


# ---- Helpers ----
def offset_to_str(n):
    if n == 0:
        return "ATM"
    return f"ATM+{n}" if n > 0 else f"ATM{n}"


def get_trading_days(start, end):
    """Weekdays in [start, end) that aren't NSE holidays. End is non-inclusive."""
    days, d = [], start
    while d < end:
        if d.weekday() < 5 and d not in NSE_HOLIDAYS:
            days.append(d)
        d += timedelta(days=1)
    return days


def get_already_loaded(bq, table_ref):
    """Set of (trade_date, expiry_code) already in BQ — the resume key."""
    try:
        df = bq.query(
            f"SELECT DISTINCT trade_date, expiry_code FROM `{table_ref}`"
        ).to_dataframe()
        return set(zip(pd.to_datetime(df["trade_date"]).dt.date,
                       df["expiry_code"].astype(int)))
    except Exception:
        return set()


# ---- API call ----
def fetch_one(cfg, offset, option_type, from_date, to_date,
              expiry_flag, expiry_code, security_id, interval):
    """Try the given expiry_flag; if empty, try the other. Returns (raw, flag)."""
    headers = {"Content-Type": "application/json",
               "access-token": cfg.dhan_access_token}
    other_flag = "MONTH" if expiry_flag == "WEEK" else "WEEK"
    for flag in [expiry_flag, other_flag]:
        payload = {
            "exchangeSegment": "NSE_FNO",
            "interval": str(interval),
            "securityId": security_id,
            "instrument": "OPTIDX",
            "expiryFlag": flag,
            "expiryCode": expiry_code,
            "strike": offset_to_str(offset),
            "drvOptionType": option_type,
            "requiredData": ["open", "high", "low", "close",
                             "volume", "iv", "oi", "spot", "strike"],
            "fromDate": from_date,
            "toDate": to_date,
        }
        try:
            r = requests.post(URL, json=payload, headers=headers, timeout=30)
            raw = r.json()
            key = "ce" if option_type == "CALL" else "pe"
            data = raw.get("data", {}).get(key, {})
            if data and data.get("timestamp"):
                return raw, flag
        except Exception as e:
            print(f"        API error ({flag}): {e}")
        time.sleep(0.2)
    return {}, expiry_flag


def parse_one(raw, offset, label, expiry_flag, expiry_date, expiry_code,
              trade_date, security_ticker, security_id, interval):
    key = "ce" if label == "CE" else "pe"
    data = raw.get("data", {}).get(key, {})
    if not data or not data.get("timestamp"):
        return None
    df = pd.DataFrame({
        "timestamp": data["timestamp"],
        "strike_price": data["strike"],
        "spot": data["spot"],
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"],
        "volume": data["volume"],
        "iv": data["iv"],
        "oi": data["oi"],
    })
    df["trade_date"] = str(trade_date)
    df["expiry_date"] = str(expiry_date) if expiry_date else "UNKNOWN"
    df["expiry_flag"] = expiry_flag
    df["expiry_code"] = expiry_code
    df["security_ticker"] = security_ticker
    df["security_id"] = str(security_id)
    df["interval"] = interval
    df["option_type"] = label
    df["strike_offset"] = offset_to_str(offset)
    return df


# ---- BQ helpers ----
def ensure_table(bq, table_ref):
    from google.cloud import bigquery
    try:
        bq.get_table(table_ref)
        print(f"✅ Table exists: {table_ref}")
    except Exception:
        table = bigquery.Table(table_ref, schema=_schema())
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="trade_date")
        table.clustering_fields = ["option_type", "expiry_code", "strike_price"]
        bq.create_table(table)
        print(f"🌓 Created: {table_ref} (partitioned trade_date, clustered)")


def prepare_df(df):
    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime(DATE_FMT)
    df["expiry_date"] = pd.to_datetime(df["expiry_date"], errors="coerce").dt.strftime(DATE_FMT)
    df["expiry_date"] = df["expiry_date"].where(df["expiry_date"].notna(), None)
    df["timestamp"] = df["timestamp"].astype("int64")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df["oi"] = pd.to_numeric(df["oi"], errors="coerce")
    df["expiry_code"] = df["expiry_code"].astype(int)
    df["interval"] = df["interval"].astype(int)
    df["strike_price"] = pd.to_numeric(df["strike_price"], errors="coerce").round(2)
    for col in ["spot", "open", "high", "low", "close", "iv"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    col_order = [
        "trade_date", "expiry_date", "expiry_flag", "expiry_code",
        "security_ticker", "security_id", "interval",
        "timestamp", "strike_price", "option_type", "strike_offset",
        "spot", "open", "high", "low", "close", "volume", "iv", "oi",
    ]
    records = df[[c for c in col_order if c in df.columns]].to_dict(orient="records")
    return [_scrub_nan(r) for r in records]


def _scrub_nan(record):
    """Replace NaN/Inf with None. json.dumps writes bare NaN/Infinity tokens
    for these, which are not valid JSON -- BigQuery's parser then rejects the
    whole row ('Parser terminated before end of string'). pandas' df.where(
    notna(), None) is not reliable here: on numeric columns it can silently
    keep NaN instead of None due to dtype re-casting, so scrub explicitly
    after to_dict() instead."""
    import math
    for k, v in record.items():
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            record[k] = None
    return record


def bq_write_job(cfg, table_ref, df, label):
    """Runs in a thread: fresh client, convert to JSON records, append to BQ."""
    from google.cloud import bigquery
    try:
        bq = bq_client(cfg)
        records = prepare_df(df)
        bq.load_table_from_json(
            records, table_ref,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND", schema=_schema()),
        ).result()
        return label, len(records)
    except Exception as e:
        return label, f"ERROR: {e}"


def parallel_bq_write(cfg, table_ref, frames, max_workers):
    if not frames:
        return 0
    total = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(bq_write_job, cfg, table_ref, df, label): label
                   for label, df in frames.items()}
        for future in concurrent.futures.as_completed(futures):
            label, result = future.result()
            if isinstance(result, int):
                total += result
                print(f"    ✅ BQ wrote {result:>7,} rows  [{label}]")
            else:
                print(f"    ❌ BQ failed [{label}]: {result}")
    return total


def _to_date(d):
    """Accept 'YYYY-MM-DD' string or a datetime.date and return a date."""
    if isinstance(d, date) and not isinstance(d, datetime):
        return d
    if isinstance(d, datetime):
        return d.date()
    return datetime.strptime(d, DATE_FMT).date()


def latest_option_date(cfg, security_ticker="NIFTY"):
    """Return the most recent trade_date already in the option table (or None).

    A quick pre-run coverage check: call it before run_options to see where you
    left off. Reads cfg.option_ref.
    """
    cfg.require("project_id", "dataset_id", "option_table")
    bq = bq_client(cfg)
    try:
        df = bq.query(
            f"SELECT MAX(trade_date) AS max_date FROM `{cfg.option_ref}` "
            f"WHERE security_ticker = @ticker",
            job_config=_ticker_param(security_ticker),
        ).to_dataframe()
    except Exception as e:
        print(f"Option table not found / not readable: {e}")
        return None
    latest = df["max_date"].iloc[0] if not df.empty else None
    if latest is None or pd.isna(latest):
        print(f"No {security_ticker} data yet in {cfg.option_ref}.")
        return None
    print(f"Latest {security_ticker} option date in BQ: {latest}")
    return latest


def _ticker_param(ticker):
    from google.cloud import bigquery
    return bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("ticker", "STRING", ticker)])


# ---- Orchestration ----
def run_options(cfg, start_date, end_date, nse_csv_path=None,
                expiry_codes=(1, 2), offsets=range(-10, 11),
                interval=1, tranche_days=5, bq_max_workers=4,
                security_ticker="NIFTY", security_id=13,
                per_call_pause=0.1):
    """Download options for [start_date, end_date) and load to cfg.option_ref.

    Args:
        cfg: Config with dhan_access_token, project_id, dataset_id, option_table.
        start_date, end_date: 'YYYY-MM-DD' strings (or date). end is EXCLUSIVE.
        nse_csv_path: OPTIONAL path to the NSE expiry CSV (Date, Expiry columns).
            If None, fetching still works — fetch_one tries both expiry flags —
            but the stored expiry_date column will be null.
        expiry_codes, offsets, interval, tranche_days, bq_max_workers: knobs.
    """
    cfg.require("project_id", "dataset_id", "option_table", "dhan_access_token")
    table_ref = cfg.option_ref
    expiry_codes = list(expiry_codes)
    offsets = list(offsets)

    start = _to_date(start_date)
    end = _to_date(end_date)

    expiry_lookup = build_expiry_lookup(nse_csv_path, expiry_codes) if nse_csv_path else {}
    if not nse_csv_path:
        print("No NSE CSV given -> expiry_date will be null; both expiry flags tried per call.")
    bq = bq_client(cfg)
    ensure_table(bq, table_ref)

    trading_days = get_trading_days(start, end)
    already_done = get_already_loaded(bq, table_ref)

    pending_pairs = [(d, code) for d in trading_days for code in expiry_codes
                     if (d, code) not in already_done]
    total_calls = len(pending_pairs) * len(offsets) * len(OPTION_TYPES)

    print(f"\nSecurity    : {security_ticker}  (id={security_id})")
    print(f"Date range  : {start} -> {end} (end exclusive)")
    print(f"Expiry codes: {expiry_codes}")
    print(f"Pending pairs   : {len(pending_pairs)}")
    print(f"Total API calls : {total_calls:,}  (~{total_calls * 0.35 / 3600:.1f} hours)")
    print(f"Tranche size    : {tranche_days} days\n")

    pending_dates = sorted(set(d for d, _ in pending_pairs))
    tranches = [pending_dates[i:i + tranche_days]
                for i in range(0, len(pending_dates), tranche_days)]

    grand_total = 0
    for t_idx, tranche_dates in enumerate(tranches):
        print(f"\n{'='*65}")
        print(f"TRANCHE [{t_idx+1}/{len(tranches)}]  {tranche_dates[0]} -> "
              f"{tranche_dates[-1]}  ({len(tranche_dates)} days)")
        print(f"{'='*65}")

        bq_frames = {}
        for trade_date in tranche_dates:
            for expiry_code in expiry_codes:
                if (trade_date, expiry_code) in already_done:
                    print(f"  ⏭  Skip  {trade_date} expiry_code={expiry_code}")
                    continue
                info = expiry_lookup.get(trade_date, {}).get(expiry_code, {})
                expiry_date = info.get("expiry_date")
                expiry_flag = info.get("expiry_flag", "WEEK")

                from_str = trade_date.strftime(DATE_FMT)
                to_str = (trade_date + timedelta(days=1)).strftime(DATE_FMT)
                print(f"\n  📅 {trade_date}  expiry_code={expiry_code}"
                      f"  expiry={expiry_date}  flag={expiry_flag}")

                day_frames, success, fail = [], 0, 0
                for offset in offsets:
                    for api_type, label in OPTION_TYPES:
                        raw, flag_used = fetch_one(
                            cfg, offset, api_type, from_str, to_str,
                            expiry_flag, expiry_code, security_id, interval)
                        parsed = parse_one(
                            raw, offset, label, flag_used, expiry_date,
                            expiry_code, trade_date, security_ticker,
                            security_id, interval)
                        if parsed is not None and not parsed.empty:
                            day_frames.append(parsed)
                            success += 1
                        else:
                            fail += 1
                        time.sleep(per_call_pause)

                if day_frames:
                    combined = pd.concat(day_frames, ignore_index=True)
                    bq_frames[f"{trade_date}_exp{expiry_code}"] = combined
                    print(f"     -> {len(combined):,} rows | {success} ok / {fail} empty")
                else:
                    print("     -> ❌ No data")

        if bq_frames:
            print(f"\n  📤 Writing {len(bq_frames)} datasets to BQ in parallel...")
            written = parallel_bq_write(cfg, table_ref, bq_frames, bq_max_workers)
            grand_total += written
            print(f"  Tranche total: {written:,} rows written")
            for key in bq_frames:
                d_str, exp_str = key.rsplit("_exp", 1)
                already_done.add((date.fromisoformat(d_str), int(exp_str)))

    print(f"\n{'='*65}\n  DONE. Total rows loaded: {grand_total:,}\n{'='*65}")
    return {"loaded": grand_total, "table": table_ref}
