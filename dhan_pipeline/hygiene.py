"""Data hygiene for the stored price tables (#980).

Two jobs the append/merge flows never do on their own:

1. Intraday split probe -- `run_intraday_split_probe`
   Intraday loads only ever APPEND new candles (intraday.dedup_against_bq drops
   anything already stored without comparing prices), so a split/bonus leaves
   old intraday candles unadjusted forever. The daily flows DO get fixed
   (split check -> lifetime reload). So the daily table is the reference:
   on a few old probe dates, roll intraday up to a day and compare its high and
   low against the daily high and low. A split shows as BOTH off by the SAME
   factor (e.g. 2.0 for 1:2, 1.25 for a 5:4 bonus) -- the old 30% intraday_check
   misses anything under 30%. Also picks up scrips the daily split check already
   flagged (`reason='mismatch'` in the flag table) since a date: the cascade.
   Flagged scrips can go straight to intraday.run_intraday_reload_for_flagged.

2. Scrip reconcile -- `run_reconcile_scrips`
   The reloads add new scrips but nothing removes rows of scrips that left the
   mapping. Rows are keyed by NAME, so a rename leaves the old name behind.
   Stored scrips not in the mapping are classified:
     renamed : its security_id is in the mapping under another name
               -> rows overlapping the new name are deleted, the rest renamed
     dropped : security_id not in the mapping (delisted, pruned, negative list)
               -> optionally copied to `<table>_orphans`, then deleted

Everything defaults to dry_run=True and every query carries a trade_date bound
or a maximum_bytes_billed cap. Nothing here changes an existing flow.
"""
from datetime import date, timedelta

import pandas as pd

from .auth import bq_client

TEMP_EXCHANGE = "TEMP"
PROBE_BACK_DAYS = (10, 45, 120, 400)
PROBE_WINDOW_DAYS = 7            # look back this far from a probe target for a trading day
PROBE_TOLERANCE = 0.005          # 0.5%: ignore tick/partial-candle noise
FACTOR_AGREEMENT = 0.02          # high and low factors must agree within 2% to call it a split
DEFAULT_MAX_BYTES = 5 * 10**9    # 5 GB cap per query (free tier is 1 TB a month)

# Natural key of each table WITHOUT `scrip` -- used to find rows that the new
# name already holds when a renamed scrip's old rows are folded into it.
TABLE_KEYS = {
    "daily": ["exchange", "security_id", "trade_date"],
    "intraday": ["security_id", "timestamp", "interval_m"],
}


# ---------------------------------------------------------------- pure helpers

def probe_windows(today, back_days=PROBE_BACK_DAYS, window_days=PROBE_WINDOW_DAYS):
    """[(target, lo, hi)]: each probe target date and the window searched for a
    trading day at or before it."""
    out = []
    for d in back_days:
        target = today - timedelta(days=int(d))
        out.append((target, target - timedelta(days=window_days - 1), target))
    return out


def classify_probe(intra, daily, tolerance=PROBE_TOLERANCE,
                   factor_agreement=FACTOR_AGREEMENT):
    """Compare intraday-rolled-up days against the daily table.

    intra, daily: frames with scrip, trade_date, high, low (intra = rolled up).
    Returns one row per scrip-day compared: scrip, trade_date, intra_high,
    daily_high, intra_low, daily_low, f_high, f_low, verdict where verdict is
      ok    : both within tolerance
      split : both off by more than tolerance, same factor (unadjusted history)
      diff  : off but not a clean factor (missing candles, bad bar) -- reported only
    """
    cols = ["scrip", "trade_date", "intra_high", "daily_high", "intra_low",
            "daily_low", "f_high", "f_low", "verdict"]
    if intra.empty or daily.empty:
        return pd.DataFrame(columns=cols)
    i = intra[["scrip", "trade_date", "high", "low"]].copy()
    d = daily[["scrip", "trade_date", "high", "low"]].copy()
    for f in (i, d):
        f["trade_date"] = pd.to_datetime(f["trade_date"]).dt.date
    m = i.merge(d, on=["scrip", "trade_date"], suffixes=("_i", "_d"))
    m = m[(m["high_i"] > 0) & (m["low_i"] > 0) & (m["high_d"] > 0) & (m["low_d"] > 0)]
    if m.empty:
        return pd.DataFrame(columns=cols)
    m["f_high"] = m["high_i"] / m["high_d"]
    m["f_low"] = m["low_i"] / m["low_d"]
    off_h = (m["f_high"] - 1).abs() > tolerance
    off_l = (m["f_low"] - 1).abs() > tolerance
    # same side of 1: a factor moves high and low the same way; a narrow intraday
    # range (high < 1, low > 1) is a data gap, not a split
    agree = ((((m["f_high"] / m["f_low"]) - 1).abs() <= factor_agreement)
             & ((m["f_high"] - 1) * (m["f_low"] - 1) > 0))
    m["verdict"] = "ok"
    m.loc[off_h | off_l, "verdict"] = "diff"
    m.loc[off_h & off_l & agree, "verdict"] = "split"
    m = m.rename(columns={"high_i": "intra_high", "high_d": "daily_high",
                          "low_i": "intra_low", "low_d": "daily_low"})
    return m[cols].sort_values(["scrip", "trade_date"]).reset_index(drop=True)


def latest_common_day(compared_days, windows):
    """Keep, per scrip and probe window, only the latest day both tables have."""
    if compared_days.empty:
        return compared_days
    parts = []
    for _, lo, hi in windows:
        w = compared_days[(compared_days["trade_date"] >= lo) & (compared_days["trade_date"] <= hi)]
        if not w.empty:
            parts.append(w.sort_values("trade_date").groupby("scrip").tail(1))
    if not parts:
        return compared_days.iloc[0:0]
    return pd.concat(parts).drop_duplicates(["scrip", "trade_date"]).reset_index(drop=True)


def classify_orphans(stored, mapping, protect=(), scrip_col="scrip",
                     security_id_col="security_id"):
    """Stored scrips that are not in the mapping, classified renamed / dropped.

    stored : frame with scrip, security_id (+ any count/date columns, kept)
    mapping: the active scrip mapping (sheet list minus negative list)
    protect: scrip names never touched (e.g. indices fetched from another list)
    Returns frame: scrip, security_id, kind ('renamed'|'dropped'), new_scrip, ...
    """
    out_cols = list(stored.columns) + ["kind", "new_scrip"]
    if stored.empty:
        return pd.DataFrame(columns=out_cols)
    s = stored.copy()
    s["security_id"] = s["security_id"].astype(str).str.strip()
    s["scrip"] = s["scrip"].astype(str).str.strip()
    mp = mapping[[scrip_col, security_id_col]].copy()
    mp.columns = ["scrip", "security_id"]
    mp["scrip"] = mp["scrip"].astype(str).str.strip()
    mp["security_id"] = mp["security_id"].astype(str).str.strip()

    keep = set(mp["scrip"]) | {str(p).strip() for p in protect}
    orphans = s[~s["scrip"].isin(keep)].copy()
    if orphans.empty:
        return pd.DataFrame(columns=out_cols)

    sid_to_name = (mp.drop_duplicates("security_id").set_index("security_id")["scrip"])
    orphans["new_scrip"] = orphans["security_id"].map(sid_to_name)
    orphans["kind"] = orphans["new_scrip"].notna().map({True: "renamed", False: "dropped"})
    return orphans[out_cols].sort_values(["kind", "scrip"]).reset_index(drop=True)


# ------------------------------------------------------------------- BigQuery

def _job(params, max_bytes):
    from google.cloud import bigquery
    return bigquery.QueryJobConfig(query_parameters=params,
                                   maximum_bytes_billed=int(max_bytes))


def _query(client, sql, params, max_bytes):
    return client.query(sql, job_config=_job(params, max_bytes)).to_dataframe()


def _exec(client, sql, params, max_bytes):
    job = client.query(sql, job_config=_job(params, max_bytes))
    job.result()
    return job.num_dml_affected_rows


def _probe_reads(cfg, client, windows, interval, max_bytes):
    from google.cloud import bigquery
    days = sorted({w_lo + timedelta(days=k)
                   for _, w_lo, w_hi in windows
                   for k in range((w_hi - w_lo).days + 1)})
    lo, hi = days[0], days[-1]
    params = [bigquery.ArrayQueryParameter("days", "DATE", days),
              bigquery.ScalarQueryParameter("lo", "DATE", lo),
              bigquery.ScalarQueryParameter("hi", "DATE", hi)]
    intra = _query(client, f"""
        SELECT scrip, trade_date, MAX(high) AS high, MIN(low) AS low
        FROM `{cfg.intraday_ref}`
        WHERE trade_date BETWEEN @lo AND @hi AND trade_date IN UNNEST(@days)
          AND interval_m = @interval
        GROUP BY scrip, trade_date
    """, params + [bigquery.ScalarQueryParameter("interval", "INT64", int(interval))], max_bytes)
    scrips = sorted(intra["scrip"].unique().tolist()) if not intra.empty else []
    if not scrips:
        return intra, pd.DataFrame(columns=["scrip", "trade_date", "high", "low"])
    daily = _query(client, f"""
        SELECT scrip, trade_date, high, low
        FROM `{cfg.daily_ref}`
        WHERE trade_date BETWEEN @lo AND @hi AND trade_date IN UNNEST(@days)
          AND exchange != '{TEMP_EXCHANGE}' AND scrip IN UNNEST(@scrips)
    """, params + [bigquery.ArrayQueryParameter("scrips", "STRING", scrips)], max_bytes)
    return intra, daily


def daily_flagged_since(cfg, client, since, max_bytes=DEFAULT_MAX_BYTES):
    """Scrips the daily split check flagged (reason='mismatch') on or after `since`."""
    from google.cloud import bigquery
    if not cfg.flag_table:
        return []
    try:
        df = _query(client, f"""
            SELECT DISTINCT scrip FROM `{cfg.flag_ref}`
            WHERE reason = 'mismatch' AND DATE(run_ts) >= @since
        """, [bigquery.ScalarQueryParameter("since", "DATE", since)], max_bytes)
    except Exception as e:
        print(f"Flag table not readable ({e}); cascade skipped.")
        return []
    return sorted(df["scrip"].tolist())


def run_intraday_split_probe(cfg, interval=15, probe_back_days=PROBE_BACK_DAYS,
                             tolerance=PROBE_TOLERANCE, cascade_since=None,
                             today=None, write_flags_to_bq=False,
                             reload=False, scrip_mapping=None, dry_run=True,
                             max_bytes=DEFAULT_MAX_BYTES):
    """Find scrips whose stored intraday history is unadjusted for a split/bonus.

    Args:
        interval: intraday interval_m to probe (15 by default).
        probe_back_days: probe this many days back from `today` (each snapped to
            the latest day both tables hold in the 7 days before it). A split
            restates ALL history before its ex-date, so any probe older than the
            ex-date catches it.
        tolerance: per-field relative difference that counts as "off".
        cascade_since: also include scrips the daily split check flagged
            (flag table, reason='mismatch') on or after this date. None = skip.
        write_flags_to_bq: append split hits to cfg.flag_table as
            reason='intraday_split' (field high/low, dhan_value=intraday,
            bq_value=daily). Ignored when dry_run.
        reload: pass the flagged scrips to run_intraday_reload_for_flagged
            (needs scrip_mapping). It inherits dry_run.
        dry_run: no writes, no deletes, no fetches. Reads only.
    Returns dict: split_scrips, cascade_scrips, flagged (union), probe (per-day
        comparison frame), diff_scrips, reload (result or None).
    """
    cfg.require("project_id", "dataset_id", "daily_table", "intraday_table")
    client = bq_client(cfg)
    today = today or date.today()
    windows = probe_windows(today, probe_back_days)
    print(f"Probe {interval}m vs daily on windows: "
          + ", ".join(f"{lo}..{hi}" for _, lo, hi in windows))

    intra, daily = _probe_reads(cfg, client, windows, interval, max_bytes)
    compared = classify_probe(intra, daily, tolerance)
    probe = latest_common_day(compared, windows)

    split = probe[probe["verdict"] == "split"]
    diff = probe[probe["verdict"] == "diff"]
    split_scrips = sorted(split["scrip"].unique().tolist())
    diff_scrips = sorted(set(diff["scrip"]) - set(split_scrips))
    print(f"Compared {probe['scrip'].nunique() if not probe.empty else 0} scrips, "
          f"{len(probe)} scrip-days: {len(split_scrips)} split, {len(diff_scrips)} noisy diff.")
    for scrip, g in split.groupby("scrip"):
        r = g.iloc[0]
        print(f"  SPLIT {scrip} @ {r['trade_date']}: intraday/daily factor "
              f"high {r['f_high']:.3f} low {r['f_low']:.3f}")
    if diff_scrips:
        print(f"  noisy diff (not a clean factor, not flagged): {diff_scrips[:30]}"
              + (" ..." if len(diff_scrips) > 30 else ""))

    cascade = daily_flagged_since(cfg, client, cascade_since, max_bytes) if cascade_since else []
    if cascade:
        print(f"Cascade: {len(cascade)} scrip(s) flagged by the daily split check "
              f"since {cascade_since}: {cascade}")
    flagged = sorted(set(split_scrips) | set(cascade))

    if write_flags_to_bq and not split.empty:
        if dry_run:
            print(f"[DRY RUN] would append {len(split) * 2} intraday_split flag rows.")
        elif not cfg.flag_table:
            print("Skipping flag write: cfg.flag_table not set.")
        else:
            from . import bq as bqmod
            run_ts = pd.Timestamp.utcnow()
            rows = []
            for _, r in split.iterrows():
                for fld in ("high", "low"):
                    rows.append({"run_ts": run_ts, "scrip": r["scrip"], "security_id": "",
                                 "check_date": r["trade_date"], "reason": "intraday_split",
                                 "field": fld, "dhan_value": float(r[f"intra_{fld}"]),
                                 "bq_value": float(r[f"daily_{fld}"])})
            bqmod.write_flags(cfg, client, pd.DataFrame(rows))
            print(f"   {len(rows)} flag rows appended -> {cfg.flag_ref}")

    reload_result = None
    if reload and flagged:
        if scrip_mapping is None:
            print("reload=True needs scrip_mapping; skipped.")
        else:
            from .intraday import run_intraday_reload_for_flagged
            reload_result = run_intraday_reload_for_flagged(
                cfg, scrip_mapping, flagged, interval=interval, dry_run=dry_run)

    return {"split_scrips": split_scrips, "cascade_scrips": cascade, "flagged": flagged,
            "diff_scrips": diff_scrips, "probe": probe, "reload": reload_result}


def _table_ref(cfg, name):
    return {"daily": cfg.daily_ref, "intraday": cfg.intraday_ref}[name]


def stored_scrips(cfg, client, name, max_bytes=DEFAULT_MAX_BYTES):
    """Distinct scrip/security_id in one table with row count and date span.
    Reads only scrip, security_id, trade_date (+ exchange for daily)."""
    ref = _table_ref(cfg, name)
    where = f"WHERE exchange != '{TEMP_EXCHANGE}'" if name == "daily" else ""
    return _query(client, f"""
        SELECT scrip, CAST(security_id AS STRING) AS security_id,
               COUNT(*) AS n_rows, MIN(trade_date) AS first_date, MAX(trade_date) AS last_date
        FROM `{ref}` {where}
        GROUP BY scrip, security_id
    """, [], max_bytes)


def _row_key(alias=""):
    """'SCRIP|security_id' -- a scrip name can carry two security_ids (HEG did),
    so every statement matches the pair, never the name alone."""
    a = f"{alias}." if alias else ""
    return f"CONCAT({a}scrip, '|', CAST({a}security_id AS STRING))"


def _pair_keys(frame):
    return [f"{s}|{i}" for s, i in zip(frame["scrip"], frame["security_id"])]


# @keys[i] = 'OLD|sid' is renamed to @news[i]. `scrip IN UNNEST(@olds)` is
# redundant with the key match but lets BigQuery prune on the scrip cluster.
_RENAME_MAP = "(SELECT k, @news[OFFSET(i)] AS new_name FROM UNNEST(@keys) AS k WITH OFFSET i)"


def _rename_sql(ref, name):
    """Two statements per table, however many scrips were renamed."""
    keys = TABLE_KEYS[name]
    key_match = " AND ".join(
        f"CAST(n.{k} AS STRING) = CAST(o.{k} AS STRING)" if k == "security_id" else f"n.{k} = o.{k}"
        for k in keys)
    delete_overlap = f"""
        DELETE FROM `{ref}` o
        WHERE o.scrip IN UNNEST(@olds) AND {_row_key('o')} IN UNNEST(@keys)
          AND o.trade_date BETWEEN @lo AND @hi
          AND EXISTS (SELECT 1 FROM `{ref}` n, {_RENAME_MAP} m
                      WHERE m.k = {_row_key('o')} AND n.scrip = m.new_name
                        AND n.scrip IN UNNEST(@news)
                        AND n.trade_date BETWEEN @lo AND @hi AND {key_match})
    """
    update = f"""
        UPDATE `{ref}` t SET scrip = m.new_name
        FROM {_RENAME_MAP} m
        WHERE t.scrip IN UNNEST(@olds) AND {_row_key('t')} = m.k
          AND t.trade_date BETWEEN @lo AND @hi
    """
    return delete_overlap, update


def _dropped_where():
    return (f"WHERE scrip IN UNNEST(@scrips) AND {_row_key()} IN UNNEST(@keys) "
            f"AND trade_date BETWEEN @lo AND @hi")


def run_reconcile_scrips(cfg, scrip_mapping, tables=("daily", "intraday"),
                         protect=(), archive=True, dry_run=True,
                         scrip_col="scrip", security_id_col="security_id",
                         max_bytes=DEFAULT_MAX_BYTES):
    """Remove rows of scrips that left the mapping; fold renamed scrips into
    their new name.

    Args:
        scrip_mapping: the FULL active mapping (load_scrip_mapping: sheet list
            minus negative list) -- not an intraday subset, or every scrip
            outside the subset would read as dropped.
        tables: any of "daily", "intraday".
        protect: scrip names never touched (indices or anything loaded from a
            list other than the sheet). Check the dry-run list for these first.
        archive: copy dropped rows to `<table>_orphans` before deleting.
        dry_run: print the plan with row counts; change nothing.
    Returns dict per table: orphans frame + counts done.
    """
    from google.cloud import bigquery
    cfg.require("project_id", "dataset_id")
    client = bq_client(cfg)
    result = {}
    for name in tables:
        ref = _table_ref(cfg, name)
        stored = stored_scrips(cfg, client, name, max_bytes)
        orphans = classify_orphans(stored, scrip_mapping, protect, scrip_col, security_id_col)
        renamed = orphans[orphans["kind"] == "renamed"]
        dropped = orphans[orphans["kind"] == "dropped"]
        print(f"\n[{name}] {ref}: {stored['scrip'].nunique()} stored scrips, "
              f"{len(renamed)} renamed, {len(dropped)} dropped "
              f"({int(orphans['n_rows'].sum()) if not orphans.empty else 0} rows).")
        for _, r in renamed.iterrows():
            print(f"  renamed  {r['scrip']} -> {r['new_scrip']}  (sid {r['security_id']}, "
                  f"{r['n_rows']} rows {r['first_date']}..{r['last_date']})")
        for _, r in dropped.iterrows():
            print(f"  dropped  {r['scrip']}  (sid {r['security_id']}, "
                  f"{r['n_rows']} rows {r['first_date']}..{r['last_date']})")

        done = {"renamed": 0, "deleted_overlap": 0, "archived": 0, "deleted": 0}
        if dry_run or orphans.empty:
            if dry_run and not orphans.empty:
                print(f"  [DRY RUN] nothing changed in {name}.")
            result[name] = {"orphans": orphans, **done}
            continue

        if not renamed.empty:
            del_sql, upd_sql = _rename_sql(ref, name)
            p = [bigquery.ArrayQueryParameter("keys", "STRING", _pair_keys(renamed)),
                 bigquery.ArrayQueryParameter("olds", "STRING", sorted(set(renamed["scrip"]))),
                 bigquery.ArrayQueryParameter("news", "STRING", list(renamed["new_scrip"])),
                 bigquery.ScalarQueryParameter("lo", "DATE", renamed["first_date"].min()),
                 bigquery.ScalarQueryParameter("hi", "DATE", renamed["last_date"].max())]
            done["deleted_overlap"] = _exec(client, del_sql, p, max_bytes) or 0
            done["renamed"] = _exec(client, upd_sql, p, max_bytes) or 0

        if not dropped.empty:
            names = sorted(dropped["scrip"].unique().tolist())
            lo, hi = dropped["first_date"].min(), dropped["last_date"].max()
            p = [bigquery.ArrayQueryParameter("scrips", "STRING", names),
                 bigquery.ArrayQueryParameter("keys", "STRING", _pair_keys(dropped)),
                 bigquery.ScalarQueryParameter("lo", "DATE", lo),
                 bigquery.ScalarQueryParameter("hi", "DATE", hi)]
            where = _dropped_where()
            expected = int(_query(client, f"SELECT COUNT(*) AS n FROM `{ref}` {where}",
                                  p, max_bytes)["n"].iloc[0])
            if archive:
                arc = f"{ref}_orphans"
                client.query(f"CREATE TABLE IF NOT EXISTS `{arc}` LIKE `{ref}`").result()
                done["archived"] = _exec(client, f"INSERT INTO `{arc}` SELECT * FROM `{ref}` {where}",
                                         p, max_bytes) or 0
                if done["archived"] != expected:
                    print(f"  ABORT delete: archived {done['archived']} rows, expected {expected}. "
                          f"Nothing deleted from {ref}; check {arc}.")
                    result[name] = {"orphans": orphans, **done}
                    continue
            done["deleted"] = _exec(client, f"DELETE FROM `{ref}` {where}", p, max_bytes) or 0
        print(f"  done: {done}")
        result[name] = {"orphans": orphans, **done}
    return result
