"""Daily OHLCV built by aggregating hourly (60m) intraday candles -> BigQuery.

A workaround path for when Dhan's own daily endpoint misbehaves for certain
scrips: fetch 60-minute bars in parallel (rate-limited, retried), filter to
market hours, roll them up to one daily bar per scrip, then upsert via the
same staging+MERGE path as `run_daily`. Rows are tagged `exchange='TEMP'` so
they're identifiable and easy to clean up separately from the main fetch.

Repeatable *process* only. Every value (dates, table names, worker/rate
knobs, market hours) is supplied by the caller via `cfg` and the arguments to
`run_daily_from_hourly`.
"""
import time
import threading
from datetime import datetime, timedelta, time as dtime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from tqdm.auto import tqdm

from .auth import bq_client, dhan_client
from .fetch import generate_row_id
from . import bq as bqmod

DATE_FMT = "%Y-%m-%d"
TEMP_EXCHANGE = "TEMP"


class RateLimiter:
    """Simple thread-safe "at most N calls/sec" limiter."""

    def __init__(self, rate_per_sec):
        self.rate = rate_per_sec
        self.lock = threading.Lock()
        self.last_called = 0

    def wait(self):
        with self.lock:
            now = time.time()
            wait_time = max(0, (1 / self.rate) - (now - self.last_called))
            if wait_time > 0:
                time.sleep(wait_time)
            self.last_called = time.time()


def build_windows(start_date, n, step_days, window_days):
    """[(from_str, to_str), ...]: n windows, `window_days` long, `step_days` apart."""
    start = datetime.strptime(start_date, DATE_FMT) if isinstance(start_date, str) else start_date
    windows = []
    for i in range(n):
        f = start + timedelta(days=i * step_days)
        t = f + timedelta(days=window_days)
        windows.append((f.strftime(DATE_FMT), t.strftime(DATE_FMT)))
    return windows


def fetch_hourly(connect, rate_limiter, row, from_date, to_date, interval,
                 market_start, market_end, max_retries):
    """One scrip's hourly candles for [from_date, to_date), market-hours only.
    Retries with backoff on any error. Returns a tidy DataFrame or None."""
    for attempt in range(max_retries):
        try:
            rate_limiter.wait()
            resp = connect.intraday_minute_data(
                security_id=row["security_id"],
                exchange_segment=row["exc_seg"],
                instrument_type=row["instrument_type"],
                interval=interval,
                from_date=from_date,
                to_date=to_date,
            )
            if resp["status"] != "success":
                raise Exception(resp.get("remarks", "API failure"))

            df = pd.DataFrame(resp["data"])
            if df.empty:
                return None

            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
            df["timestamp"] = df["timestamp"].dt.tz_convert("Asia/Kolkata")
            df = df[(df["timestamp"].dt.time >= market_start) &
                    (df["timestamp"].dt.time <= market_end)]
            if df.empty:
                return None

            df["scrip"] = row["scrip"]
            df["exchange"] = TEMP_EXCHANGE
            df["security_id"] = int(row["security_id"])
            df["interval_m"] = interval
            df.columns = ["open", "high", "low", "close", "volume", "timestamp",
                         "scrip", "exchange", "security_id", "interval_m"]
            return df[["scrip", "exchange", "security_id", "timestamp",
                      "interval_m", "open", "high", "low", "close", "volume"]]

        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(0.5 * (attempt + 1))
            else:
                print(f"❌ Failed: {row['scrip']} -> {e}")
                return None


def aggregate_daily(intraday_df):
    """Roll up hourly bars to one daily OHLCV row per (trade_date, scrip,
    exchange, security_id), plus the row_id used for the staging+MERGE upsert."""
    df = intraday_df.copy()
    df["trade_date"] = df["timestamp"].dt.date

    daily = df.groupby(
        ["trade_date", "scrip", "exchange", "security_id"], as_index=False
    ).agg(open=("open", "first"), high=("high", "max"),
          low=("low", "min"), close=("close", "last"), volume=("volume", "sum"))

    daily["row_id"] = daily.apply(generate_row_id, axis=1)
    return daily


def run_daily_from_hourly(cfg, scrip_mapping, start_date, n=1,
                          step_days=7, window_days=1, interval=60,
                          max_workers=4, rate_per_sec=4, max_retries=3,
                          market_start=dtime(9, 15), market_end=dtime(18, 30),
                          clear_old_temp=True):
    """Fetch hourly candles across n windows, aggregate to daily, upsert as
    exchange='TEMP' rows, then drop TEMP rows older than start_date.

    Args:
        cfg: Config with project_id, dataset_id, daily_table, staging_table.
        scrip_mapping: DataFrame with scrip, security_id, exc_seg, instrument_type.
        start_date: 'YYYY-MM-DD' of the first window.
        n, step_days, window_days: window schedule (your original n=1, step=7, window=1).
        interval: candle minutes for the underlying fetch (your original 60).
        max_workers, rate_per_sec, max_retries: parallel-fetch tuning.
        market_start, market_end: datetime.time bounds for the market-hours filter.
        clear_old_temp: if True, delete stale TEMP rows before this run (both
            the pre-run wipe and the post-run "older than start_date" cleanup
            from your original script).
    """
    cfg.require("project_id", "dataset_id", "daily_table", "staging_table",
                "dhan_client_id", "dhan_access_token")
    client = bq_client(cfg)
    connect = dhan_client(cfg)
    rate_limiter = RateLimiter(rate_per_sec)

    if clear_old_temp:
        client.query(
            f"DELETE FROM `{cfg.daily_ref}` WHERE exchange = '{TEMP_EXCHANGE}'"
        ).result()
        print("Old TEMP data deleted.")

    windows = build_windows(start_date, n, step_days, window_days)
    print(f"Total scrips: {len(scrip_mapping)}")

    all_intraday, success, failure = [], 0, 0
    for from_date, to_date in windows:
        print(f"\nFetching: {from_date} -> {to_date}")
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [
                ex.submit(fetch_hourly, connect, rate_limiter, row, from_date,
                         to_date, interval, market_start, market_end, max_retries)
                for _, row in scrip_mapping.iterrows()
            ]
            for f in tqdm(as_completed(futures), total=len(futures), desc="Fetching"):
                result = f.result()
                if result is not None:
                    all_intraday.append(result)
                    success += 1
                else:
                    failure += 1

    print(f"\nSuccess: {success}, Failures: {failure}")

    if not all_intraday:
        print("No data fetched.")
        return {"fetched": 0, "uploaded": 0, "table": cfg.daily_ref}

    intraday_df = pd.concat(all_intraday, ignore_index=True)
    daily_data = aggregate_daily(intraday_df)

    uploaded = bqmod.upsert_daily(cfg, client, daily_data)
    print(f"✅ Upserted {uploaded} rows into {cfg.daily_ref}.")

    if clear_old_temp:
        client.query(
            f"DELETE FROM `{cfg.daily_ref}` "
            f"WHERE exchange = '{TEMP_EXCHANGE}' AND trade_date < '{windows[0][0]}'"
        ).result()
        print("Old TEMP cleanup done.")

    check = client.query(
        f"SELECT trade_date, COUNT(*) records FROM `{cfg.daily_ref}` "
        f"WHERE exchange = '{TEMP_EXCHANGE}' GROUP BY trade_date ORDER BY trade_date"
    ).to_dataframe()
    print(check)

    return {"fetched": len(intraday_df), "uploaded": uploaded,
            "failed": failure, "table": cfg.daily_ref, "check": check}
