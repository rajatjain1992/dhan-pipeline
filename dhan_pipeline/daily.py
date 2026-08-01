"""High-level daily flow: fetch a window -> split-check 2nd-last day -> upsert.

This is the repeatable *process*. All variables (dates, scrip source, table
names, creds) come from `cfg` and the arguments passed by the calling file.
"""
from datetime import date, timedelta

import pandas as pd

from .auth import bq_client, gspread_client
from .scrips import load_scrip_mapping
from .fetch import fetch_ohlcv
from . import bq as bqmod
from .splitcheck import split_dates, detect_corporate_actions


def recent_window(days=4):
    """Return (from_date, to_date) covering the last `days` calendar days.

    A helper the calling file MAY use to get >= 2 trading days across a weekend/
    holiday. The file stays in control of the dates — it can also hardcode them.
    """
    today = date.today()
    return (today - timedelta(days=days)).isoformat(), today.isoformat()


def run_daily(cfg, from_date, to_date, scrip_mapping=None,
              flags_csv="corporate_action_flags.csv", write_flags_to_bq=True):
    """Run the daily pipeline end to end.

    Args:
        cfg: Config with all project values filled in by the caller.
        from_date, to_date: window to fetch (ISO strings). Supply >= 2 trading
            days so the 2nd-last day is available for the split check.
        scrip_mapping: optional pre-loaded/subset mapping; if None it is loaded
            from the Google Sheet in cfg.
    """
    cfg.require("project_id", "dataset_id", "daily_table",
                "dhan_client_id", "dhan_access_token")

    client = bq_client(cfg)

    if scrip_mapping is None:
        gc = gspread_client(cfg)
        scrip_mapping = load_scrip_mapping(cfg, gc)

    fetched, failed = fetch_ohlcv(cfg, scrip_mapping, from_date, to_date)
    print(f"Fetched {len(fetched)} rows across "
          f"{fetched['scrip'].nunique() if not fetched.empty else 0} scrips.")

    if fetched.empty:
        return {"fetched": fetched, "flags": pd.DataFrame(), "failed": failed,
                "uploaded": 0, "last_date": None, "check_date": None}

    last_date, check_date = split_dates(fetched)
    print(f"last_date={last_date}  check_date={check_date}")

    # ---- Split / corporate-action detection on the 2nd-last day ----
    flags = pd.DataFrame()
    if check_date is not None:
        scrips = fetched["scrip"].unique().tolist()
        bq_check = bqmod.read_daily(cfg, client, scrips, check_date, check_date)
        flags = detect_corporate_actions(cfg, fetched, bq_check, check_date)

    mismatch_scrips = (sorted(flags[flags["reason"] == "mismatch"]["scrip"].unique())
                       if not flags.empty else [])
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
