"""Corporate-action (split / bonus / adjustment) detection.

Idea: fetch the last 2 trading days every run. The most-recent day is NEW data
to upload. The 2nd-last day already lives in BigQuery. If Dhan has retroactively
adjusted history (a split/bonus happened), Dhan's 2nd-last-day OHLC will no
longer equal what we stored -> that scrip is flagged.

Prices are rounded to `cfg.price_decimals` (default 2 = NSE tick) before an
exact-equality comparison, so genuine float-repr noise never causes a false flag.
"""
import pandas as pd

PRICE_COLS = ["open", "high", "low", "close"]


def split_dates(fetched):
    """Return (last_date, check_date) from the fetched frame.

    last_date  = most recent day (the new data to upload)
    check_date = the day before (compared against BigQuery). None if only 1 day.
    """
    dates = sorted(pd.Series(fetched["trade_date"]).dropna().unique())
    if not dates:
        return None, None
    last_date = dates[-1]
    check_date = dates[-2] if len(dates) > 1 else None
    return last_date, check_date


def detect_corporate_actions(cfg, fetched, bq_check, check_date):
    """Compare Dhan's check-date OHLC against BigQuery's stored OHLC.

    Returns a long-format flags DataFrame (one row per mismatching field), with
    columns: run_ts, scrip, security_id, check_date, reason, field,
    dhan_value, bq_value. Empty frame => nothing flagged.

    reason is one of:
      - "mismatch"   : both sides present but a price differs (suspected split)
      - "missing_bq" : Dhan has the day but BigQuery doesn't (usually a new scrip)
    """
    d = cfg.price_decimals
    run_ts = pd.Timestamp.utcnow()

    if check_date is None:
        return _empty_flags()

    dhan = fetched[fetched["trade_date"] == check_date].copy()
    if dhan.empty:
        return _empty_flags()

    bq = bq_check.copy()
    if not bq.empty:
        bq["trade_date"] = pd.to_datetime(bq["trade_date"]).dt.date
        bq = bq[bq["trade_date"] == check_date]

    merged = dhan.merge(
        bq[["scrip"] + PRICE_COLS] if not bq.empty else pd.DataFrame(columns=["scrip"] + PRICE_COLS),
        on="scrip", how="left", suffixes=("_dhan", "_bq"), indicator=True,
    )

    rows = []
    for _, r in merged.iterrows():
        base = {
            "run_ts": run_ts,
            "scrip": r["scrip"],
            "security_id": str(r.get("security_id", "")),
            "check_date": check_date,
        }
        if r["_merge"] != "both":
            rows.append({**base, "reason": "missing_bq", "field": None,
                         "dhan_value": None, "bq_value": None})
            continue
        for c in PRICE_COLS:
            dv, bv = r.get(f"{c}_dhan"), r.get(f"{c}_bq")
            if pd.isna(dv) or pd.isna(bv):
                continue
            if round(float(dv), d) != round(float(bv), d):
                rows.append({**base, "reason": "mismatch", "field": c,
                             "dhan_value": float(dv), "bq_value": float(bv)})

    if not rows:
        return _empty_flags()
    return pd.DataFrame(rows, columns=[
        "run_ts", "scrip", "security_id", "check_date", "reason",
        "field", "dhan_value", "bq_value",
    ])


def _empty_flags():
    return pd.DataFrame(columns=[
        "run_ts", "scrip", "security_id", "check_date", "reason",
        "field", "dhan_value", "bq_value",
    ])
