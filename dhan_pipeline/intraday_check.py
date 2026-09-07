"""Daily-vs-intraday reconciliation check.

The gap this closes: run_intraday() has no idea whether what it just stored
actually agrees with the daily table. If a scrip needs a full re-fetch (a
corporate action rolled through only the daily side, a partial/corrupted
upload, Dhan serving stale intraday candles) nothing today surfaces it -- the
intraday pipeline is blind to its own correctness.

Approach: roll intraday candles up to one OHLC bar per (scrip, trade_date)
*in BigQuery* (partition-pruned on trade_date, so this stays cheap) and diff
that against the real daily table. Unlike splitcheck.py's exact-equality
check (built to catch a literal retroactive split adjustment on the daily
feed itself), a 15m/1m->daily rollup will legitimately differ a little from
the vendor's own daily bar -- so this only flags a real problem: a field off
by more than `pct_threshold`, or a day present on one side and missing on
the other.

Flags are written to the SAME flag table as splitcheck.py (cfg.flag_ref,
bq.FLAG_SCHEMA) so there's one place to look, not two -- reason strings are
prefixed "intraday_" to tell the two checks apart.
"""
import pandas as pd
from google.cloud import bigquery

PRICE_FIELDS = ["open", "high", "low", "close"]


def _rollup_sql(intraday_table):
    return f"""
        SELECT
            scrip,
            security_id,
            trade_date,
            ARRAY_AGG(open ORDER BY timestamp ASC LIMIT 1)[OFFSET(0)] AS open,
            MAX(high) AS high,
            MIN(low) AS low,
            ARRAY_AGG(close ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS close,
            SUM(volume) AS volume,
            COUNT(*) AS candle_count
        FROM `{intraday_table}`
        WHERE trade_date BETWEEN @from_date AND @to_date
          AND interval_m = @interval
        GROUP BY scrip, security_id, trade_date
    """


def build_daily_from_intraday(cfg, client, from_date, to_date, interval=15, intraday_table=None):
    """Roll intraday candles up to one OHLC row per (scrip, trade_date)."""
    intraday_table = intraday_table or cfg.intraday_ref
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("from_date", "DATE", from_date),
        bigquery.ScalarQueryParameter("to_date", "DATE", to_date),
        bigquery.ScalarQueryParameter("interval", "INT64", int(interval)),
    ])
    return client.query(_rollup_sql(intraday_table), job_config=job_config).to_dataframe()


def compare_to_daily(cfg, client, from_date, to_date, interval=15,
                     pct_threshold=0.30, intraday_table=None):
    """Diff the intraday->daily rollup against cfg.daily_ref for [from_date, to_date].

    Returns a flags DataFrame (bq.FLAG_SCHEMA shape): run_ts, scrip,
    security_id, check_date, reason, field, dhan_value, bq_value -- where
    dhan_value = the intraday-rolled-up figure, bq_value = the stored daily
    figure (names kept consistent with splitcheck.py's schema, not literally
    "from Dhan" here). reason is one of:
      - "intraday_missing_daily"    : intraday has the day, daily table doesn't
      - "intraday_missing_intraday" : daily table has the day, intraday doesn't
      - "intraday_pct_diff"         : both present but a field differs by more
                                       than pct_threshold (default 30%)
    """
    rolled = build_daily_from_intraday(cfg, client, from_date, to_date, interval, intraday_table)
    if rolled.empty:
        return _empty_flags()

    # Only compare scrips actually tracked at this interval -- most of the
    # daily universe (~3400 scrips) is never fetched intraday (~800 scrips),
    # so an unscoped outer join would flag every untracked scrip as
    # "missing_intraday" every single run. That's not a data problem, it's
    # by design; scope to intraday's own scrip set instead.
    tracked_scrips = rolled["scrip"].unique().tolist()

    daily = client.query(
        f"""
        SELECT scrip, security_id, trade_date, open, high, low, close, volume
        FROM `{cfg.daily_ref}`
        WHERE trade_date BETWEEN @from_date AND @to_date
          AND scrip IN UNNEST(@scrips)
        """,
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("from_date", "DATE", from_date),
            bigquery.ScalarQueryParameter("to_date", "DATE", to_date),
            bigquery.ArrayQueryParameter("scrips", "STRING", tracked_scrips),
        ]),
    ).to_dataframe()

    rolled["trade_date"] = pd.to_datetime(rolled["trade_date"]).dt.date
    daily["trade_date"] = pd.to_datetime(daily["trade_date"]).dt.date

    merged = rolled.merge(
        daily, on=["scrip", "trade_date"], how="outer",
        suffixes=("_intraday", "_daily"), indicator=True,
    )

    run_ts = pd.Timestamp.utcnow()
    rows = []
    for _, r in merged.iterrows():
        sec_id = r.get("security_id_intraday")
        if pd.isna(sec_id):
            sec_id = r.get("security_id_daily")
        base = {"run_ts": run_ts, "scrip": r["scrip"],
                "security_id": str(sec_id) if pd.notna(sec_id) else "",
                "check_date": r["trade_date"]}

        if r["_merge"] == "left_only":
            rows.append({**base, "reason": "intraday_missing_daily", "field": None,
                         "dhan_value": None, "bq_value": None})
            continue
        if r["_merge"] == "right_only":
            rows.append({**base, "reason": "intraday_missing_intraday", "field": None,
                         "dhan_value": None, "bq_value": None})
            continue

        for field in PRICE_FIELDS:
            iv, dv = r.get(f"{field}_intraday"), r.get(f"{field}_daily")
            if pd.isna(iv) or pd.isna(dv) or dv == 0:
                continue
            pct = abs(iv - dv) / abs(dv)
            if pct > pct_threshold:
                rows.append({**base, "reason": "intraday_pct_diff", "field": field,
                             "dhan_value": float(iv), "bq_value": float(dv)})

    if not rows:
        return _empty_flags()
    return pd.DataFrame(rows, columns=[
        "run_ts", "scrip", "security_id", "check_date", "reason",
        "field", "dhan_value", "bq_value",
    ])


def run_intraday_daily_check(cfg, from_date, to_date, interval=15,
                             pct_threshold=0.30, write_flags_to_bq=True):
    """Standalone entry point: check [from_date, to_date] and print/store flags.

    Call this after run_intraday() with the same date range you just fetched
    (cheap -- both queries are trade_date-partition-pruned), or periodically
    over a wider range as a health check. Prints a per-scrip summary and
    returns the flags DataFrame.
    """
    from .auth import bq_client
    from . import bq as bqmod

    cfg.require("project_id", "dataset_id", "daily_table", "intraday_table")
    client = bq_client(cfg)

    flags = compare_to_daily(cfg, client, from_date, to_date, interval, pct_threshold)

    if flags.empty:
        print(f"✅ No mismatches: intraday (interval_m={interval}) agrees with "
              f"daily for {from_date} -> {to_date} (threshold {pct_threshold:.0%}).")
        return flags

    print(f"⚠️  {len(flags)} flag(s) across {flags['scrip'].nunique()} scrip(s) "
          f"for {from_date} -> {to_date}:")
    for scrip, grp in flags.groupby("scrip"):
        reasons = ", ".join(
            f"{r['reason']}" + (f"[{r['field']}]" if r["field"] else "") +
            f"@{r['check_date']}"
            for _, r in grp.iterrows()
        )
        print(f"  {scrip}: {reasons}")

    if write_flags_to_bq:
        bqmod.write_flags(cfg, client, flags)
        print(f"   flags appended -> {cfg.flag_ref}")

    return flags


def _empty_flags():
    return pd.DataFrame(columns=[
        "run_ts", "scrip", "security_id", "check_date", "reason",
        "field", "dhan_value", "bq_value",
    ])
