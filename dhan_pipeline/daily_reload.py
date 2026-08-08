"""Daily table reload: full lifetime reupload, or a targeted date-range
reupload with split/bonus/dividend detection at the range boundary.

Two modes:
  "lifetime" -> wipe the whole daily table, refetch every scrip's full
      history from `lifetime_start` to today, reload. A full refetch always
      returns Dhan's current post-split-adjusted prices, so no separate
      split check is needed in this mode.
  "range"    -> wipe only [from_date, to_date], refetch that window (plus a
      lookback buffer), check the OLDEST date in the range against what's
      currently stored in BigQuery for that date (before it gets deleted).
      Scrips whose price moved get their ENTIRE lifetime history deleted +
      refetched (their whole series is stale post-split), everyone else
      just gets the range reuploaded.

Every run also clears out any exchange='TEMP' rows first -- those are
leftovers from the daily-from-hourly rollup path (`run_daily_from_hourly`),
which tags its rows TEMP and lives in the same daily table. This module
manages the "real" data, so stale TEMP rows are wiped before it does anything
else, regardless of mode.

`dry_run=True` runs the whole flow (fetch + split-check) but skips every
DELETE/upsert against BigQuery -- prints what *would* happen instead, so you
can sanity-check dates/mode before committing.

Repeatable *process* only. Every value (dates, mode, buffer, table names,
the scrip mapping) is supplied by the caller via `cfg` and the arguments to
`run_daily_reload`.
"""
from datetime import date, datetime, timedelta

import pandas as pd
from tqdm.auto import tqdm

from .auth import bq_client
from .fetch import fetch_ohlcv
from . import bq as bqmod
from .splitcheck import detect_corporate_actions

DATE_FMT = "%Y-%m-%d"
LIFETIME_START = "2001-01-01"
TEMP_EXCHANGE = "TEMP"


def _today():
    return date.today().strftime(DATE_FMT)


def _to_date(d):
    return d if isinstance(d, date) else datetime.strptime(d, DATE_FMT).date()


def delete_range(client, table_ref, from_date, to_date):
    from google.cloud import bigquery
    client.query(
        f"DELETE FROM `{table_ref}` WHERE trade_date BETWEEN @from_date AND @to_date",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("from_date", "DATE", from_date),
            bigquery.ScalarQueryParameter("to_date", "DATE", to_date),
        ]),
    ).result()


def delete_scrips(client, table_ref, scrips):
    from google.cloud import bigquery
    if not scrips:
        return
    client.query(
        f"DELETE FROM `{table_ref}` WHERE scrip IN UNNEST(@scrips)",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ArrayQueryParameter("scrips", "STRING", list(scrips)),
        ]),
    ).result()


def delete_all(client, table_ref):
    client.query(f"DELETE FROM `{table_ref}` WHERE TRUE").result()


def delete_temp(client, table_ref):
    client.query(
        f"DELETE FROM `{table_ref}` WHERE exchange = '{TEMP_EXCHANGE}'"
    ).result()


def _count_temp(client, table_ref):
    return _count_where(client, table_ref, f"exchange = '{TEMP_EXCHANGE}'")


def _count_where(client, table_ref, where="TRUE"):
    try:
        df = client.query(
            f"SELECT COUNT(*) AS n FROM `{table_ref}` WHERE {where}"
        ).to_dataframe()
        return int(df["n"].iloc[0])
    except Exception:
        return 0


def run_daily_reload(cfg, scrip_mapping, mode, from_date=None, to_date=None,
                     buffer_days=5, lifetime_start=LIFETIME_START,
                     flags_csv="corporate_action_flags.csv", write_flags_to_bq=True,
                     clear_temp=True, dry_run=False):
    """Reload the daily table: full lifetime, or a targeted date range.

    Args:
        cfg: Config with project_id, dataset_id, daily_table, staging_table,
            flag_table, dhan creds.
        scrip_mapping: DataFrame with scrip, security_id, exc_seg, instrument_type.
        mode: "lifetime" or "range".
        from_date, to_date: required for mode="range" ('YYYY-MM-DD').
        buffer_days: mode="range" only -- how many extra calendar days BEFORE
            from_date to fetch, purely as lookback context. REMINDER: this
            does not change what gets split-checked (that's always the oldest
            trading day within [from_date, to_date] itself) -- it only widens
            what's fetched. Set it generously (e.g. 5-10) if your date range
            starts on/right after a likely weekend/holiday gap, so the fetch
            window comfortably contains real trading days.
        lifetime_start: mode="lifetime" (and the flagged-scrip refetch in
            range mode) start date. Empty results before a scrip's listing
            date are harmless.
        clear_temp: delete any exchange='TEMP' rows (leftovers from
            run_daily_from_hourly) before doing anything else. On by default.
        dry_run: if True, still fetches from Dhan and runs the split-check
            (so you see exactly what would be flagged/deleted/uploaded), but
            skips every DELETE and upsert against BigQuery.
    """
    cfg.require("project_id", "dataset_id", "daily_table", "staging_table",
                "dhan_client_id", "dhan_access_token")
    client = bq_client(cfg)
    bqmod.ensure_table(client, cfg.daily_ref, bqmod.DAILY_SCHEMA)

    if dry_run:
        print("=== DRY RUN: no deletes or uploads will happen ===\n")

    if clear_temp:
        temp_n = _count_temp(client, cfg.daily_ref)
        if temp_n:
            if dry_run:
                print(f"[DRY RUN] would delete {temp_n} exchange='TEMP' row(s).")
            else:
                delete_temp(client, cfg.daily_ref)
                print(f"Deleted {temp_n} exchange='TEMP' row(s).")
        else:
            print("No exchange='TEMP' rows to clear.")

    if mode == "lifetime":
        return _run_lifetime(cfg, client, scrip_mapping, lifetime_start, dry_run)
    if mode == "range":
        if not from_date or not to_date:
            raise ValueError("mode='range' requires from_date and to_date")
        return _run_range(cfg, client, scrip_mapping, from_date, to_date,
                          buffer_days, lifetime_start, flags_csv,
                          write_flags_to_bq, dry_run)
    raise ValueError(f"mode must be 'lifetime' or 'range', got {mode!r}")


def _run_lifetime(cfg, client, scrip_mapping, lifetime_start, dry_run):
    print(f"LIFETIME reload: {lifetime_start} -> {_today()} for {len(scrip_mapping)} scrips.")

    if dry_run:
        # No fetch: a real lifetime fetch (2001 -> today, every scrip) is the
        # expensive part -- skip it so dry_run stays cheap. Still show the real
        # count of rows currently in the table via a plain COUNT(*), so the
        # preview is grounded in actual numbers, not just intent.
        existing_rows = _count_where(client, cfg.daily_ref)
        print(f"[DRY RUN] would delete ALL {existing_rows:,} existing row(s) "
              f"from {cfg.daily_ref}.")
        print(f"[DRY RUN] would fetch full lifetime history for {len(scrip_mapping)} "
              f"scrip(s) from {lifetime_start} -> {_today()} (not fetched in dry run).")
        print(f"[DRY RUN] would upsert the result into {cfg.daily_ref}.")
        return {"mode": "lifetime", "existing_rows": existing_rows, "fetched": 0,
                "uploaded": 0, "failed": [], "flagged_scrips": [],
                "table": cfg.daily_ref, "dry_run": True}

    print("Deleting all existing rows...")
    delete_all(client, cfg.daily_ref)

    fetched, failed = fetch_ohlcv(cfg, scrip_mapping, lifetime_start, _today(),
                                  desc="Lifetime fetch")
    print(f"Fetched {len(fetched)} rows across "
          f"{fetched['scrip'].nunique() if not fetched.empty else 0} scrips.")

    uploaded = bqmod.upsert_daily(cfg, client, fetched)
    print(f"✅ Upserted {uploaded} rows into {cfg.daily_ref}.")

    if failed:
        print(f"⚠️  Failed to fetch {len(failed)} scrip(s): {failed}")

    return {"mode": "lifetime", "fetched": len(fetched), "uploaded": uploaded,
            "failed": failed, "flagged_scrips": [], "table": cfg.daily_ref,
            "dry_run": False}


def _run_range(cfg, client, scrip_mapping, from_date, to_date, buffer_days,
               lifetime_start, flags_csv, write_flags_to_bq, dry_run):
    from_d, to_d = _to_date(from_date), _to_date(to_date)
    fetch_from = (from_d - timedelta(days=buffer_days)).strftime(DATE_FMT)

    print(f"RANGE reload: {from_date} -> {to_date}  "
          f"(fetching from {fetch_from} with {buffer_days}d buffer) "
          f"for {len(scrip_mapping)} scrips.")

    # ---- 1. Fetch BEFORE any deletion, so BQ still has the old values to check against ----
    fetched, failed = fetch_ohlcv(cfg, scrip_mapping, fetch_from, to_date,
                                  desc="Range fetch")
    print(f"Fetched {len(fetched)} rows across "
          f"{fetched['scrip'].nunique() if not fetched.empty else 0} scrips.")

    if fetched.empty:
        print("Nothing fetched -- nothing to do.")
        return {"mode": "range", "fetched": 0, "uploaded": 0, "failed": failed,
                "flagged_scrips": [], "table": cfg.daily_ref, "dry_run": dry_run}

    # ---- 2. Split-check at the OLDEST date within the selected range itself ----
    in_range = fetched[(fetched["trade_date"] >= from_d) & (fetched["trade_date"] <= to_d)]
    check_date = in_range["trade_date"].min() if not in_range.empty else None

    flags = pd.DataFrame()
    flagged_scrips = []
    if check_date is not None:
        scrips = fetched["scrip"].unique().tolist()
        bq_check = bqmod.read_daily(cfg, client, scrips, check_date, check_date)
        flags = detect_corporate_actions(cfg, fetched, bq_check, check_date)
        flagged_scrips = (sorted(flags[flags["reason"] == "mismatch"]["scrip"].unique())
                          if not flags.empty else [])

    if flagged_scrips:
        print(f"\n⚠️  {len(flagged_scrips)} scrip(s) flagged at {check_date} "
              f"(suspected split/adjustment): {flagged_scrips}")
        if dry_run:
            print("[DRY RUN] would save/append flags, would NOT write them now.")
        else:
            if flags_csv:
                flags.to_csv(flags_csv, index=False)
                print(f"   flags saved -> {flags_csv}")
            if write_flags_to_bq:
                bqmod.write_flags(cfg, client, flags)
                print(f"   flags appended -> {cfg.flag_ref}")
    else:
        print(f"\n✅ No split/adjustment mismatch at {check_date}.")

    # ---- 3. Delete: the range for everyone, plus full lifetime for flagged scrips ----
    if dry_run:
        print(f"\n[DRY RUN] would delete range {from_date} -> {to_date} for all scrips.")
        if flagged_scrips:
            print(f"[DRY RUN] would delete COMPLETE lifetime history for "
                  f"{len(flagged_scrips)} flagged scrip(s): {flagged_scrips}")
    else:
        print(f"\nDeleting range {from_date} -> {to_date} for all scrips...")
        delete_range(client, cfg.daily_ref, from_d, to_d)
        if flagged_scrips:
            print(f"Deleting COMPLETE lifetime history for {len(flagged_scrips)} flagged scrip(s)...")
            delete_scrips(client, cfg.daily_ref, flagged_scrips)

    # ---- 4. Upload: range data for clean scrips, full lifetime refetch for flagged ----
    clean_upload = in_range[~in_range["scrip"].isin(flagged_scrips)].copy()
    if dry_run:
        print(f"[DRY RUN] would upsert {len(clean_upload)} range rows for "
              f"{clean_upload['scrip'].nunique() if not clean_upload.empty else 0} clean scrips.")
        uploaded_range = 0
    else:
        uploaded_range = bqmod.upsert_daily(cfg, client, clean_upload)
        print(f"✅ Upserted {uploaded_range} range rows for "
              f"{clean_upload['scrip'].nunique() if not clean_upload.empty else 0} clean scrips.")

    uploaded_lifetime = 0
    if flagged_scrips:
        if dry_run:
            print(f"[DRY RUN] would refetch + upsert full lifetime history for "
                  f"{len(flagged_scrips)} flagged scrip(s) (no fetch performed in dry run).")
        else:
            flagged_mapping = scrip_mapping[scrip_mapping["scrip"].isin(flagged_scrips)]
            # security_id/exchange/instrument_type come from scrip_mapping, not BQ
            # (BQ's copy for these scrips was just deleted).
            lifetime_fetched, lifetime_failed = fetch_ohlcv(
                cfg, flagged_mapping, lifetime_start, _today(),
                desc="Lifetime refetch (flagged)")
            uploaded_lifetime = bqmod.upsert_daily(cfg, client, lifetime_fetched)
            print(f"✅ Upserted {uploaded_lifetime} lifetime rows for "
                  f"{len(flagged_scrips)} flagged scrip(s).")
            failed = failed + lifetime_failed

    if failed:
        print(f"\n⚠️  Failed to fetch {len(failed)} scrip(s): {failed}")

    return {"mode": "range", "fetched": len(fetched), "check_date": check_date,
            "flagged_scrips": flagged_scrips, "flags": flags,
            "uploaded_range": uploaded_range, "uploaded_lifetime": uploaded_lifetime,
            "uploaded": uploaded_range + uploaded_lifetime, "failed": failed,
            "table": cfg.daily_ref, "dry_run": dry_run}
