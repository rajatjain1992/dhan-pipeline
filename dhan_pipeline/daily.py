"""High-level daily flow: fetch last 2 days -> split-check 2nd-last day -> upsert.

This is the whole notebook cell condensed into one call. A script/notebook just:

    cfg = Config(...)
    result = run_daily(cfg)

`run_daily` returns a dict with the fetched frame, the flags frame, failed
scrips, and the upserted row count.
"""
from datetime import date, timedelta

import pandas as pd

from .auth import bq_client, gspread_client
from .scrips import load_scrip_mapping
from .fetch import fetch_ohlcv
from . import bq as bqmod
from .splitcheck import split_dates, detect_corporate_actions


def run_daily(cfg, scrip_mapping=None, from_date=None, to_date=None,
              flags_csv="corporate_action_flags.csv", write_flags_to_bq=True):
    """Run the daily pipeline end to end.

    - Fetches the last 2 calendar days by default (widen the window if a holiday
      means only one trading day is returned).
    - Compares the 2nd-last trading day against BigQuery to detect splits.
    - Uploads the last day for all fetched scrips (cfg.upload_mismatched=True),
      or only for matching scrips if you set cfg.upload_mismatched=False.
    """
    client = bq_client(cfg)

    if scrip_mapping is None:
        gc = gspread_client(cfg)
        scrip_mapping = load_scrip_mapping(cfg, gc)

    # Default window: last 4 calendar days -> guarantees >= 2 trading days even
    # across a weekend/holiday. We then key off the two most-recent dates found.
    if to_date is None:
        to_date = date.today().isoformat()
    if from_date is None:
        from_date = (date.today() - timedelta(days=4)).isoformat()

    fetched, failed = fetch_ohlcv(cfg, scrip_mapping, from_date, to_date)
    print(f"Fetched {len(fetched)} rows across {fetched['scrip'].nunique() if not fetched.empty else 0} scrips.")

    if fetched.empty:
        return {"fetched": fetched, "flags": pd.DataFrame(), "failed": failed, "uploaded": 0,
                "last_date": None, "check_date": None}

    last_date, check_date = split_dates(fetched)
    print(f"last_date={last_date}  check_date={check_date}")

    # ---- Split / corporate-action detection on the 2nd-last day ----
    flags = pd.DataFrame()
    if check_date is not None:
        scrips = fetched["scrip"].unique().tolist()
        bq_check = bqmod.read_daily(cfg, client, scrips, check_date, check_date)
        flags = detect_corporate_actions(cfg, fetched, bq_check, check_date)

    mismatch_scrips = sorted(flags[flags["reason"] == "mismatch"]["scrip"].unique()) if not flags.empty else []
    if mismatch_scrips:
        print(f"\n⚠️  {len(mismatch_scrips)} scrip(s) flagged (suspected split/adjustment): {mismatch_scrips}")
        if flags_csv:
            flags.to_csv(flags_csv, index=False)
            print(f"   flags saved -> {flags_csv}")
        if write_flags_to_bq:
            bqmod.write_flags(cfg, client, flags)
            print(f"   flags appended -> {cfg.flag_ref}")
    else:
        print("✅ No split/adjustment mismatch on the 2nd-last day.")

    # ---- Upload the LAST day's data ----
    upload_df = fetched[fetched["trade_date"] == last_date].copy()
    if not cfg.upload_mismatched and mismatch_scrips:
        before = len(upload_df)
        upload_df = upload_df[~upload_df["scrip"].isin(mismatch_scrips)]
        print(f"   skip-on-mismatch: dropped {before - len(upload_df)} rows for flagged scrips.")

    uploaded = bqmod.upsert_daily(cfg, client, upload_df)
    print(f"✅ Upserted {uploaded} rows into {cfg.daily_ref} for {last_date}.")

    if failed:
        print(f"\n⚠️  Failed to fetch {len(failed)} scrip(s): {failed}")

    return {"fetched": fetched, "flags": flags, "failed": failed, "uploaded": uploaded,
            "last_date": last_date, "check_date": check_date}
