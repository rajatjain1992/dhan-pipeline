"""Data health check across the daily, intraday and bhavcopy tables.

One read-only entry point, `run_data_health_check`, returns a findings
DataFrame (check, severity, scrip, date, detail). Every query is bounded by a
`trade_date`/`date` filter, dry-run first, and refused if the estimate exceeds
`max_bytes`, so it stays inside the BigQuery free tier.

Checks:
  coverage_*            per-day scrip count vs the table's own median; a day that
                        another table has but this one lacks is a gap (holidays
                        are in no table, so they are never flagged)
  temp_only_day         daily has only exchange='TEMP' (hourly-rollup) rows for a day
  daily_*               duplicate keys, bad OHLC, non-positive prices
  intraday_*            duplicate timestamps, off-session candles, bad OHLC
  bhav_close_mismatch   daily close vs NSE bhavcopy close, adjustment-aware: a
                        constant ratio before an ex-date and 1.0 after is a
                        corporate-action adjustment (info), anything else is an error
  large_move            day-over-day move above a threshold, judged against NSE's
                        own prev_close (which NSE adjusts for corporate actions)
  intraday_vs_daily     15m/1m rollup disagrees with the daily bar (stale or
                        unadjusted intraday history)

Repeatable *process* only: every table name comes from `cfg`.
"""
import statistics
from datetime import date, datetime, timedelta, time as dtime

import pandas as pd

FINDING_COLS = ["check", "severity", "scrip", "date", "detail"]
SEVERITY_ORDER = {"error": 0, "warn": 1, "info": 2}


def _f(check, severity, scrip=None, d=None, detail=""):
    return {"check": check, "severity": severity, "scrip": scrip, "date": d, "detail": detail}


def _as_date(x):
    return pd.Timestamp(x).date()


# ---------------------------------------------------------------- analysis (pure)

def coverage_findings(sources, min_coverage=0.5):
    """sources: {name: DataFrame[d, n, (temp_n)]} of per-day scrip/row counts."""
    counts, temp_only = {}, {}
    for name, df in sources.items():
        if df is None or df.empty:
            continue
        counts[name] = {_as_date(r.d): int(r.n) for r in df.itertuples()}
        if "temp_n" in df.columns:
            temp_only[name] = {_as_date(r.d) for r in df.itertuples() if int(r.n) == 0 and int(r.temp_n) > 0}

    out, complete, typical = [], {}, {}
    for name, c in counts.items():
        med = statistics.median(v for v in c.values() if v > 0) if any(v > 0 for v in c.values()) else 0
        typical[name] = med
        thr = med * min_coverage
        complete[name] = {d for d, n in c.items() if n >= thr and n > 0}
        for d, n in sorted(c.items()):
            if 0 < n < thr:
                out.append(_f("coverage_partial", "error", None, d,
                              f"{name}: {n} vs typical {med:.0f}"))
    for name, days in temp_only.items():
        for d in sorted(days):
            out.append(_f("temp_only_day", "error", None, d,
                          f"{name}: only exchange='TEMP' (hourly rollup) rows, real rows missing"))

    union = set().union(*complete.values()) if complete else set()
    latest = max(union) if union else None
    for name, c in counts.items():
        first = min(c)
        for d in sorted(union):
            if d < first or d in c or d in temp_only.get(name, set()):
                continue
            by = [n for n, comp in complete.items() if d in comp and n != name]
            late = d == latest
            out.append(_f("coverage_missing_day", "warn" if late else "error", None, d,
                          f"{name}: no rows; present in {', '.join(by)}"
                          + (" (latest day, may not be published yet)" if late else "")))
    return out


def daily_row_findings(df):
    out = []
    for r in df.itertuples():
        d = _as_date(r.d)
        if r.n_rows > 1:
            out.append(_f("daily_duplicate_rows", "error", r.scrip, d, f"{r.exchange}: {r.n_rows} rows for one key"))
        if r.bad_ohlc > 0:
            out.append(_f("daily_bad_ohlc", "error", r.scrip, d, f"{r.exchange}: high<low or open/close outside range"))
        if r.nonpos > 0:
            out.append(_f("daily_nonpositive_price", "error", r.scrip, d, f"{r.exchange}: price <= 0"))
    return out


def intraday_row_findings(df, interval):
    out = []
    for r in df.itertuples():
        d = _as_date(r.d)
        if r.n > r.nd:
            out.append(_f("intraday_duplicate_timestamps", "error", r.scrip, d,
                          f"{interval}m: {r.n} rows but {r.nd} distinct timestamps"))
        if r.off > 0:
            out.append(_f("intraday_off_session", "warn", r.scrip, d,
                          f"{interval}m: {r.off} candle(s) outside the trading session"))
        if r.bad > 0:
            out.append(_f("intraday_bad_ohlc", "error", r.scrip, d,
                          f"{interval}m: {r.bad} candle(s) with high<low or open/close outside range"))
    return out


def bhav_close_findings(df, const_tol=0.03):
    out = []
    for r in df.itertuples():
        if r.bad_days == 0:
            continue
        const = (r.max_bad_r / r.min_bad_r - 1) < const_tol
        max_bad, min_good = pd.to_datetime(r.max_bad_d), pd.to_datetime(r.min_good_d)
        prefix = pd.isna(min_good) or max_bad < min_good
        ratio = (r.min_bad_r + r.max_bad_r) / 2
        if const and prefix:
            out.append(_f("bhav_adjusted_history", "info", r.scrip, max_bad.date(),
                          f"daily/bhav close ratio {ratio:.3f} through {max_bad.date()}, 1.0 after: corporate-action adjusted"))
        else:
            out.append(_f("bhav_close_mismatch", "error", r.scrip, max_bad.date(),
                          f"{r.bad_days} of {r.days} days differ from NSE bhav close (ratio {r.min_bad_r:.3f} to {r.max_bad_r:.3f}), not an adjustment pattern"))
    return out


def large_move_findings(df, agree_tol=0.05):
    out = []
    for r in df.itertuples():
        d = _as_date(r.d)
        base = f"{r.exchange}: {r.dd_move:+.1%} close {r.pc:g} -> {r.close:g}"
        if pd.isna(r.bhav_move):
            out.append(_f("large_move", "warn", r.scrip, d, base + " (no bhavcopy row to verify)"))
        elif abs(r.dd_move - r.bhav_move) > agree_tol:
            out.append(_f("large_move_unadjusted_suspect", "error", r.scrip, d,
                          base + f"; NSE's own prev_close implies {r.bhav_move:+.1%}: likely unadjusted corporate action"))
        else:
            out.append(_f("large_move", "info", r.scrip, d, base + f"; confirmed by NSE prev_close ({r.bhav_move:+.1%})"))
    return out


def rollup_findings(flags, interval):
    out = []
    if flags is None or flags.empty:
        return out
    pct = flags[flags["reason"] == "intraday_pct_diff"]
    for scrip, g in pct.groupby("scrip"):
        ratio = (g["dhan_value"] / g["bq_value"]).median()
        out.append(_f("intraday_vs_daily", "error", scrip, max(g["check_date"]),
                      f"{interval}m rollup differs from daily on {g['check_date'].nunique()} day(s) "
                      f"({min(g['check_date'])} to {max(g['check_date'])}), intraday/daily ~ {ratio:.2f}: "
                      "stale or unadjusted intraday history, refetch"))
    return out


# ---------------------------------------------------------------- queries

def _queries(cfg, start, interval, move_threshold, bhav_tol, session):
    q = {}
    has_i, has_n, has_b = bool(cfg.intraday_table), bool(cfg.bhav_table), bool(cfg.bse_bhav_table)
    D = cfg.daily_ref
    q["cov_daily"] = (
        f"SELECT trade_date AS d, COUNT(DISTINCT IF(exchange!='TEMP', scrip, NULL)) n, "
        f"COUNT(DISTINCT IF(exchange='TEMP', scrip, NULL)) temp_n "
        f"FROM `{D}` WHERE trade_date >= '{start}' GROUP BY d")
    q["daily_rows"] = (
        f"SELECT * FROM (SELECT scrip, exchange, trade_date AS d, COUNT(*) n_rows, "
        f"COUNTIF(high<low OR close>high*1.0001 OR close<low*0.9999 OR open>high*1.0001 OR open<low*0.9999) bad_ohlc, "
        f"COUNTIF(open<=0 OR high<=0 OR low<=0 OR close<=0) nonpos "
        f"FROM `{D}` WHERE trade_date >= '{start}' AND exchange != 'TEMP' GROUP BY scrip, exchange, trade_date) "
        f"WHERE n_rows > 1 OR bad_ohlc > 0 OR nonpos > 0")
    if has_i:
        s0, s1 = session
        q["cov_intraday"] = (
            f"SELECT trade_date AS d, COUNT(DISTINCT scrip) n FROM `{cfg.intraday_ref}` "
            f"WHERE trade_date >= '{start}' AND interval_m = {int(interval)} GROUP BY d")
        q["intraday_rows"] = (
            f"SELECT * FROM (SELECT scrip, trade_date AS d, COUNT(*) n, COUNT(DISTINCT timestamp) nd, "
            f"COUNTIF(TIME(TIMESTAMP_SECONDS(timestamp),'Asia/Kolkata') NOT BETWEEN TIME '{s0:%H:%M:%S}' AND TIME '{s1:%H:%M:%S}') off, "
            f"COUNTIF(high<low OR close>high*1.0001 OR close<low*0.9999 OR open>high*1.0001 OR open<low*0.9999 "
            f"OR open<=0 OR high<=0 OR low<=0 OR close<=0) bad "
            f"FROM `{cfg.intraday_ref}` WHERE trade_date >= '{start}' AND interval_m = {int(interval)} "
            f"GROUP BY scrip, trade_date) WHERE n > nd OR off > 0 OR bad > 0")
    if has_n:
        q["cov_nse_bhav"] = (
            f"SELECT DATE(date) AS d, COUNT(*) n FROM `{cfg.bhav_ref}` WHERE date >= '{start}' GROUP BY d")
        q["bhav_close"] = (
            f"WITH b AS (SELECT symbol, DATE(date) d, close_price FROM `{cfg.bhav_ref}` "
            f"WHERE date >= '{start}' AND series='EQ' AND close_price>0), "
            f"d AS (SELECT scrip, trade_date, close FROM `{D}` WHERE trade_date >= '{start}' AND exchange='NSE_EQ'), "
            f"j AS (SELECT b.symbol scrip, b.d, d.close/b.close_price r FROM b JOIN d ON b.symbol=d.scrip AND b.d=d.trade_date) "
            f"SELECT scrip, COUNT(*) days, COUNTIF(ABS(r-1)>{bhav_tol}) bad_days, "
            f"MIN(IF(ABS(r-1)>{bhav_tol}, d, NULL)) min_bad_d, MAX(IF(ABS(r-1)>{bhav_tol}, d, NULL)) max_bad_d, "
            f"MIN(IF(ABS(r-1)<={bhav_tol}, d, NULL)) min_good_d, "
            f"MIN(IF(ABS(r-1)>{bhav_tol}, r, NULL)) min_bad_r, MAX(IF(ABS(r-1)>{bhav_tol}, r, NULL)) max_bad_r "
            f"FROM j GROUP BY scrip HAVING COUNTIF(ABS(r-1)>{bhav_tol}) > 0")
        q["large_move"] = (
            f"WITH x AS (SELECT scrip, exchange, trade_date d, close, "
            f"LAG(close) OVER (PARTITION BY scrip, exchange ORDER BY trade_date) pc "
            f"FROM `{D}` WHERE trade_date >= DATE_SUB(DATE '{start}', INTERVAL 10 DAY) "
            f"AND exchange IN ('NSE_EQ','BSE_EQ')), "
            f"m AS (SELECT * FROM x WHERE d >= '{start}' AND pc > 0 AND ABS(close/pc-1) > {move_threshold}) "
            f"SELECT m.scrip, m.exchange, m.d, m.close, m.pc, m.close/m.pc-1 AS dd_move, "
            f"b.close_price/b.prev_close-1 AS bhav_move "
            f"FROM m LEFT JOIN `{cfg.bhav_ref}` b ON m.exchange='NSE_EQ' AND b.symbol=m.scrip "
            f"AND DATE(b.date)=m.d AND b.series='EQ' AND b.date >= '{start}' AND b.prev_close>0")
    if has_b:
        q["cov_bse_bhav"] = (
            f"SELECT DATE(date) AS d, COUNT(*) n FROM `{cfg.bse_bhav_ref}` WHERE date >= '{start}' GROUP BY d")
    return q


# ---------------------------------------------------------------- entry point

def run_data_health_check(cfg, lookback_days=60, interval=15, min_coverage=0.5,
                          move_threshold=0.30, bhav_tolerance=0.02,
                          include_rollup=True, rollup_threshold=0.30,
                          session=(dtime(9, 15), dtime(15, 30)),
                          max_bytes=600_000_000, dry_run=False, show=6, client=None):
    """Run every check the configured tables allow and return a findings DataFrame.

    Needs cfg.daily_table. intraday_table, bhav_table (NSE) and bse_bhav_table
    switch on the checks that use them; checks whose table is unset are skipped.
    dry_run=True only estimates bytes (bills nothing) and returns {query: bytes}.
    """
    from google.cloud import bigquery
    from .auth import bq_client

    cfg.require("project_id", "dataset_id", "daily_table")
    client = client or bq_client(cfg)
    today = date.today()
    start = (today - timedelta(days=lookback_days)).isoformat()
    queries = _queries(cfg, start, interval, move_threshold, bhav_tolerance, session)

    est = {}
    for name, sql in queries.items():
        job = client.query(sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
        est[name] = job.total_bytes_processed
    total = sum(est.values())
    print(f"Health check window {start} -> {today}: {len(queries)} queries, ~{total/1e6:.0f} MB estimated"
          + (" (+ intraday rollup)" if include_rollup and cfg.intraday_table else ""))
    if dry_run:
        return est
    if total > max_bytes:
        raise ValueError(f"estimated {total/1e6:.0f} MB exceeds max_bytes={max_bytes/1e6:.0f} MB; "
                         "shorten lookback_days or raise max_bytes")

    frames = {name: client.query(sql).to_dataframe() for name, sql in queries.items()}

    findings = []
    findings += coverage_findings(
        {n: frames[k] for n, k in (("daily", "cov_daily"), ("intraday", "cov_intraday"),
                                    ("nse_bhav", "cov_nse_bhav"), ("bse_bhav", "cov_bse_bhav")) if k in frames},
        min_coverage)
    findings += daily_row_findings(frames["daily_rows"])
    if "intraday_rows" in frames:
        findings += intraday_row_findings(frames["intraday_rows"], interval)
    if "bhav_close" in frames:
        findings += bhav_close_findings(frames["bhav_close"])
        findings += large_move_findings(frames["large_move"])
    if include_rollup and cfg.intraday_table:
        from .intraday_check import compare_to_daily
        flags = compare_to_daily(cfg, client, start, today.isoformat(), interval, rollup_threshold)
        findings += rollup_findings(flags, interval)

    out = pd.DataFrame(findings, columns=FINDING_COLS)
    if not out.empty:
        out["_s"] = out["severity"].map(SEVERITY_ORDER)
        out = out.sort_values(["_s", "check", "date", "scrip"], na_position="last").drop(columns="_s").reset_index(drop=True)
    _print_report(out, show)
    return out


def _print_report(df, show):
    if df.empty:
        print("No findings: all checks passed.")
        return
    sev = df["severity"].value_counts()
    print("Findings: " + ", ".join(f"{int(sev.get(s, 0))} {s}" for s in ("error", "warn", "info")))
    for (check, severity), g in df.groupby(["check", "severity"], sort=False):
        print(f"\n[{severity}] {check} x{len(g)}")
        for r in g.head(show).itertuples():
            print(f"   {r.scrip or '-':<12} {str(r.date or ''):<11} {r.detail}")
        if len(g) > show:
            print(f"   ... {len(g) - show} more")
