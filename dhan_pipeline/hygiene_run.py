"""Fix-by-factor and the one-call daily `run_hygiene` (#980.1).

The probe (hygiene.run_intraday_split_probe) finds scrips whose stored intraday
candles are out of line with the daily table by a clean factor. Dhan's intraday
history IS adjusted (a 15m re-download of ANANDRATHI cleared it, 17 Nov 2026), so
the stored candles are what is stale. Instead of re-downloading, divide them by
the per-day factor: no Dhan calls, and 1m history older than Dhan keeps survives.

Safety:
  * a day is corrected only when >= MIN_RUN_DAYS days of that scrip share its
    factor -- a split shifts every earlier day alike; one odd day is a bad bar
  * volume is scaled only when the daily volume shows the same factor
  * the fix is idempotent: a corrected day matches daily and is never touched again
  * 1m is never re-downloaded; 15m falls back to re-download only if still wrong
"""
import contextlib
import io
from datetime import date, timedelta

import pandas as pd

from .auth import bq_client
from .hygiene import (DEFAULT_MAX_BYTES, FACTOR_AGREEMENT, PROBE_TOLERANCE, TEMP_EXCHANGE,
                      _exec, _query, classify_probe, run_intraday_split_probe,
                      run_reconcile_scrips)

HISTORY_FLOOR = date(2015, 1, 1)  # trade_date lower bound for full-history reads
MIN_RUN_DAYS = 3                  # a factor must repeat on >= this many days to be applied
RUN_MATCH = 0.005                 # days whose factors agree within 0.5% share a run
VOLUME_MATCH = 0.05               # volume scaled only if daily volume shows the factor within 5%
UPDATE_CHUNK = 20000              # scrip-days per UPDATE statement
SHARE_RATIO_MATCH = 0.002         # f within 0.2% of p/q (p, q <= 10) = split/bonus share ratio


def is_share_ratio(f, match=SHARE_RATIO_MATCH, max_term=10):
    """True when f is a split/bonus share ratio such as 2, 10, 3/2 or 1/5.
    Those change the share count, so volume moves by f too (verified: a fresh
    Dhan download of a 2:1 split has 2x the old volume). Demerger, rights and
    dividend factors (1.031, 3.186, 2.801 ...) change price only."""
    return any(abs(f * q / p - 1) <= match
               for p in range(1, max_term + 1) for q in range(1, max_term + 1) if p != q)


def day_factors(intra, daily, tolerance=PROBE_TOLERANCE, factor_agreement=FACTOR_AGREEMENT,
                min_run=MIN_RUN_DAYS, run_match=RUN_MATCH, volume_match=VOLUME_MATCH):
    """Per scrip-day correction for stored intraday candles.

    intra : rolled-up intraday days: scrip, trade_date, high, low, volume
    daily : daily rows:              scrip, trade_date, high, low, volume
    Returns (fix, skipped), both with columns scrip, trade_date, f, vf (+ reason in skipped):
      fix     : divide that day's prices by f, multiply its volume by vf
      skipped : 'split' days left alone. reason 'one_off': fewer than `min_run`
                days share the factor. reason 'daily_stale': f < 1, intraday is
                LOWER than daily. Corporate-action adjustment only ever lowers past
                prices, so the higher side is the stale one: here the daily table
                is unadjusted (JSWDULUX 1:10, Nov 2026) and intraday must not move.
    f  = sqrt(f_high * f_low), the day's intraday/daily price ratio.
    vf = f when f is a share ratio (split/bonus) or the day's intraday/daily volume
         ratio is ~1/f (daily volume was scaled too), else 1 (rights, demerger and
         dividend-type adjustments move price only). Decided per run of days
         sharing a factor (majority), so one noisy day cannot differ from its run.
    """
    cols = ["scrip", "trade_date", "f", "vf"]
    empty = pd.DataFrame(columns=cols)
    comp = classify_probe(intra, daily, tolerance, factor_agreement)
    split = comp[comp["verdict"] == "split"].copy()
    if split.empty:
        return empty, empty.assign(reason=pd.Series(dtype=str))
    split["f"] = (split["f_high"] * split["f_low"]) ** 0.5
    split = split.reset_index(drop=True)

    vols = []
    for df, name in ((intra, "vi"), (daily, "vd")):
        v = df[["scrip", "trade_date", "volume"]].rename(columns={"volume": name}).copy()
        v["trade_date"] = pd.to_datetime(v["trade_date"]).dt.date
        vols.append(v)
    split = split.merge(vols[0], on=["scrip", "trade_date"], how="left") \
                 .merge(vols[1], on=["scrip", "trade_date"], how="left")
    ratio = split["vi"].astype(float) / split["vd"].astype(float)
    scaled = (ratio * split["f"] - 1).abs()
    use = (split["vd"].astype(float) > 0) & (scaled <= volume_match) & (scaled < (ratio - 1).abs())
    use &= (split["f"] - 1).abs() > 2 * volume_match   # under 10% the volume test is noise
    use |= split["f"].map(is_share_ratio)          # split/bonus: Dhan's fresh download scales volume
    split["use"] = use.to_numpy()

    split["ok"] = False
    split["vf"] = 1.0
    for _, g in split.groupby("scrip"):
        f = g["f"].to_numpy()
        same = abs(f[:, None] / f[None, :] - 1) <= run_match
        n = same.sum(axis=1)
        share = (same @ g["use"].to_numpy().astype(float)) / n   # volume vote within the run
        split.loc[g.index, "ok"] = n >= min_run
        split.loc[g.index[share > 0.5], "vf"] = g["f"].to_numpy()[share > 0.5]

    split["reason"] = ""
    split.loc[~split["ok"], "reason"] = "one_off"
    split.loc[split["f"] < 1, "reason"] = "daily_stale"
    fix = split[split["reason"] == ""][cols].reset_index(drop=True)
    skipped = split[split["reason"] != ""][cols + ["reason"]].reset_index(drop=True)
    return fix, skipped


def _history_reads(cfg, client, scrips, interval, today, max_bytes):
    """Full-history day rollup (intraday) and daily rows for `scrips`."""
    from google.cloud import bigquery
    p = [bigquery.ArrayQueryParameter("scrips", "STRING", list(scrips)),
         bigquery.ScalarQueryParameter("lo", "DATE", HISTORY_FLOOR),
         bigquery.ScalarQueryParameter("hi", "DATE", today)]
    intra = _query(client, f"""
        SELECT scrip, trade_date, MAX(high) AS high, MIN(low) AS low, SUM(volume) AS volume
        FROM `{cfg.intraday_ref}`
        WHERE trade_date BETWEEN @lo AND @hi AND scrip IN UNNEST(@scrips)
          AND interval_m = @interval
        GROUP BY scrip, trade_date
    """, p + [bigquery.ScalarQueryParameter("interval", "INT64", int(interval))], max_bytes)
    daily = _query(client, f"""
        SELECT scrip, trade_date, high, low, volume
        FROM `{cfg.daily_ref}`
        WHERE trade_date BETWEEN @lo AND @hi AND scrip IN UNNEST(@scrips)
          AND exchange != '{TEMP_EXCHANGE}'
    """, p, max_bytes)
    return intra, daily


def factor_update_sql(ref):
    return f"""
        UPDATE `{ref}` t
        SET open = t.open / m.f, high = t.high / m.f, low = t.low / m.f,
            close = t.close / m.f, volume = t.volume * m.vf
        FROM (SELECT k, @fs[OFFSET(i)] AS f, @vfs[OFFSET(i)] AS vf
              FROM UNNEST(@keys) AS k WITH OFFSET i) m
        WHERE t.scrip IN UNNEST(@scrips) AND t.interval_m = @interval
          AND t.trade_date BETWEEN @lo AND @hi
          AND CONCAT(t.scrip, '|', CAST(t.trade_date AS STRING)) = m.k
    """


def fix_by_factor(cfg, client, scrips, interval, today=None, dry_run=False,
                  max_bytes=DEFAULT_MAX_BYTES):
    """Divide stored intraday prices of `scrips` at `interval` by their per-day
    factor, then re-check. No Dhan calls.

    Returns dict: fixed_days, rows_updated, factors (frame of applied days),
    skipped (frame), still_wrong (scrips with a split day left after the fix;
    in dry_run, the scrips that WOULD be touched or skipped).
    """
    from google.cloud import bigquery
    today = today or date.today()
    out = {"fixed_days": 0, "rows_updated": 0, "still_wrong": [], "daily_stale": [],
           "skipped": pd.DataFrame(columns=["scrip", "trade_date", "f", "vf", "reason"]),
           "factors": pd.DataFrame(columns=["scrip", "trade_date", "f", "vf"])}
    if not scrips:
        return out
    intra, daily = _history_reads(cfg, client, scrips, interval, today, max_bytes)
    fix, skipped = day_factors(intra, daily)
    stale = sorted(set(skipped.loc[skipped["reason"] == "daily_stale", "scrip"]))
    out.update(fixed_days=len(fix), skipped=skipped, factors=fix, daily_stale=stale)
    if fix.empty or dry_run:
        # nothing written: report the scrips a factor cannot fix
        out["still_wrong"] = sorted(set(skipped["scrip"]) - set(fix["scrip"]) - set(stale))
        return out

    sql = factor_update_sql(cfg.intraday_ref)
    for start in range(0, len(fix), UPDATE_CHUNK):
        c = fix.iloc[start:start + UPDATE_CHUNK]
        keys = [f"{s}|{d.isoformat()}" for s, d in zip(c["scrip"], c["trade_date"])]
        p = [bigquery.ArrayQueryParameter("keys", "STRING", keys),
             bigquery.ArrayQueryParameter("fs", "FLOAT64", [float(x) for x in c["f"]]),
             bigquery.ArrayQueryParameter("vfs", "FLOAT64", [float(x) for x in c["vf"]]),
             bigquery.ArrayQueryParameter("scrips", "STRING", sorted(set(c["scrip"]))),
             bigquery.ScalarQueryParameter("interval", "INT64", int(interval)),
             bigquery.ScalarQueryParameter("lo", "DATE", min(c["trade_date"])),
             bigquery.ScalarQueryParameter("hi", "DATE", max(c["trade_date"]))]
        out["rows_updated"] += _exec(client, sql, p, max_bytes) or 0

    touched = sorted(set(fix["scrip"]) | set(skipped["scrip"]))
    intra2, daily2 = _history_reads(cfg, client, touched, interval, today, max_bytes)
    after = classify_probe(intra2, daily2)
    left = after[(after["verdict"] == "split") & (after["f_high"] > 1)]   # f < 1 = daily side
    out["still_wrong"] = sorted(set(left["scrip"]) - set(stale))
    return out


def run_hygiene(cfg, scrip_mapping, mapping_15m=None, mapping_1m=None,
                cleanup="monday", today=None, cascade_days=3, max_scrips=25,
                redownload_fallback=True, dry_run=False, verbose=False,
                max_bytes=DEFAULT_MAX_BYTES):
    """One call, run daily after the intraday load.

      1. cleanup  : renamed/dropped scrips (run_reconcile_scrips) on Mondays;
                    cleanup=True forces it, cleanup=False skips it
      2. find     : split probe per interval + daily split flags of the last
                    `cascade_days` days
      3. fix      : divide stored prices by the per-day factor (no Dhan calls)
      4. verify   : re-check the fixed scrips
      5. fallback : 15m scrips still wrong are re-downloaded (15-day windows);
                    1m is never re-downloaded (its history is older than Dhan keeps)

    cfg: needs daily_table, intraday_table, flag_table (cfg_intraday).
    scrip_mapping: the FULL sheet list (cleanup). mapping_15m / mapping_1m: the
    intraday subsets; None skips that interval. dry_run=True changes nothing.
    """
    cfg.require("project_id", "dataset_id", "daily_table", "intraday_table")
    today = today or date.today()
    client = bq_client(cfg)

    def quiet():
        return contextlib.nullcontext() if verbose else contextlib.redirect_stdout(io.StringIO())

    tag = "[DRY RUN] " if dry_run else ""
    report = {}

    if cleanup is True or (cleanup == "monday" and today.weekday() == 0):
        with quiet():
            rec = run_reconcile_scrips(cfg, scrip_mapping, dry_run=dry_run, max_bytes=max_bytes)
        parts = []
        for t, r in rec.items():
            o = r["orphans"]
            ren = int((o["kind"] == "renamed").sum()) if not o.empty else 0
            drop = int((o["kind"] == "dropped").sum()) if not o.empty else 0
            parts.append(f"{t} {ren} renamed / {drop} dropped")
        print(f"{tag}Cleanup: " + ", ".join(parts))
        report["cleanup"] = rec
    else:
        print("Cleanup: skipped (runs Mondays; cleanup=True to force)")

    for interval, mapping in ((15, mapping_15m), (1, mapping_1m)):
        if mapping is None:
            continue
        with quiet():
            probe = run_intraday_split_probe(
                cfg, interval=interval, today=today,
                cascade_since=today - timedelta(days=cascade_days),
                dry_run=True, max_bytes=max_bytes)
        tracked = set(mapping["scrip"].astype(str).str.strip())
        found = [s for s in probe["flagged"] if s in tracked]
        todo, later = found[:max_scrips], found[max_scrips:]
        if not todo:
            print(f"{interval}m: ok")
            report[interval] = {"probe": probe, "found": []}
            continue

        with quiet():
            fx = fix_by_factor(cfg, client, todo, interval, today, dry_run, max_bytes)
        fac = fx["factors"]
        changed = sorted(set(fac["scrip"])) if not fac.empty else []
        line = (f"{tag}{interval}m: {len(found)} found -> factor fix on {len(changed)} "
                f"({fx['fixed_days']} days, {fx['rows_updated']} candles)")
        if not dry_run:
            line += f" -> still wrong {len(fx['still_wrong'])}"
        print(line + (f"; {len(later)} left for next run" if later else ""))
        if changed:
            first = fac.sort_values("trade_date").groupby("scrip").first()
            print("   " + ", ".join(f"{s} /{r['f']:.3f}" for s, r in first.iterrows()))
        if fx["daily_stale"]:
            print(f"   daily table stale (intraday left alone), reload daily for: {fx['daily_stale']}")

        reload = None
        if fx["still_wrong"] and not dry_run:
            if interval == 15 and redownload_fallback:
                from .intraday import run_intraday_reload_for_flagged
                with quiet():
                    reload = run_intraday_reload_for_flagged(
                        cfg, mapping, fx["still_wrong"], interval=15, window_days=15)
                print(f"   re-downloaded: {reload.get('eligible', [])}")
            else:
                print(f"   NOT fixed, check by hand: {fx['still_wrong']}")
        report[interval] = {"probe": probe, "found": found, "fix": fx, "reload": reload}
    return report
