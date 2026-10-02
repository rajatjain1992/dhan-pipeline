"""Date planning: decide WHICH dates a flow should fetch.

mode="manual": use exactly the from/to the caller supplies (no BigQuery read).
mode="auto":   read per-day coverage from BigQuery (trade_date-pruned, a few MB)
               and return only the days that are missing or partial, as
               ready-to-run (from, to) ranges.

Trading days are not hardcoded. A weekday counts as a trading day if the flow's
own table, or any configured reference table (daily, intraday, NSE/BSE bhavcopy),
has a *complete* day for it (>= min_coverage x the median per-day count).
Weekdays that no table has, and that sit before the newest confirmed day, are
reported as inferred holidays and not fetched. A fully missed interior day looks
identical to a holiday unless a reference table is configured on `cfg`; partial
days are always caught from the flow's own counts.

Repeatable *process* only: every table name comes from `cfg`, every date from
the caller or from BigQuery.
"""
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
import statistics

IST = timezone(timedelta(hours=5, minutes=30))
DATE_FMT = "%Y-%m-%d"


@dataclass(frozen=True)
class _Spec:
    table_attr: str
    ref_attr: str
    date_col: str
    cutoff: time                  # IST time after which today's data is normally published
    count_expr: str = "COUNT(*)"
    extra_where: str = ""
    uses_interval: bool = False
    anchor_prev_day: bool = False  # run_daily's split check needs the previous stored day inside the range


# Cutoffs are observed publish times (bhavcopy Last-Modified, 2026-09-25): BSE ~16:39 IST,
# NSE ~23:55 IST. Dhan candles are normally complete shortly after the 15:30 close.
SPECS = {
    "daily": _Spec("daily_table", "daily_ref", "trade_date", time(16, 0),
                   "COUNT(DISTINCT scrip)", "AND exchange != 'TEMP'", anchor_prev_day=True),
    "intraday": _Spec("intraday_table", "intraday_ref", "trade_date", time(16, 0),
                      "COUNT(DISTINCT scrip)", uses_interval=True),
    "bhavcopy": _Spec("bhav_table", "bhav_ref", "date", time(23, 59)),
    "bse_bhavcopy": _Spec("bse_bhav_table", "bse_bhav_ref", "date", time(17, 0)),
}

REFERENCES = {
    "daily": ["intraday", "bhavcopy", "bse_bhavcopy"],
    "intraday": ["daily", "bhavcopy", "bse_bhavcopy"],
    "bhavcopy": ["daily", "bse_bhavcopy", "intraday"],
    "bse_bhavcopy": ["daily", "bhavcopy", "intraday"],
}


@dataclass
class FetchPlan:
    flow: str
    mode: str
    ranges: list = field(default_factory=list)     # [(from_iso, to_iso), ...] ready to run
    missing: dict = field(default_factory=dict)    # iso date -> why it is in the plan
    inferred_holidays: list = field(default_factory=list)
    notes: list = field(default_factory=list)

    @property
    def up_to_date(self):
        return not self.ranges

    @property
    def from_date(self):
        return self.ranges[0][0] if self.ranges else None

    @property
    def to_date(self):
        return self.ranges[-1][1] if self.ranges else None

    def show(self):
        print(f"Plan [{self.mode}] {self.flow}: "
              + ("nothing to fetch, up to date." if self.up_to_date
                 else f"{len(self.ranges)} range(s) to fetch"))
        for f, t in self.ranges:
            print(f"  {f} -> {t}")
        for d, why in self.missing.items():
            print(f"    missing {d}: {why}")
        if self.inferred_holidays:
            print(f"  inferred holidays (no table has them): {self.inferred_holidays}")
        for n in self.notes:
            print(f"  note: {n}")


def _to_date(d):
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    return datetime.strptime(str(d), DATE_FMT).date()


def _parse_cutoff(cutoff_ist):
    if isinstance(cutoff_ist, time):
        return cutoff_ist
    return datetime.strptime(cutoff_ist, "%H:%M").time()


def last_available_date(spec, now=None, cutoff_ist=None):
    """Newest calendar date whose data should already be published."""
    now = now or datetime.now(IST)
    cutoff = _parse_cutoff(cutoff_ist) if cutoff_ist else spec.cutoff
    return now.date() if now.time() >= cutoff else now.date() - timedelta(days=1)


def _sql(spec, ref, from_d, to_d, interval):
    interval_clause = f"AND interval_m = {int(interval)}" if spec.uses_interval else ""
    return (f"SELECT CAST(`{spec.date_col}` AS STRING) AS d, {spec.count_expr} AS n "
            f"FROM `{ref}` WHERE `{spec.date_col}` BETWEEN '{from_d}' AND '{to_d}' "
            f"{spec.extra_where} {interval_clause} GROUP BY d")


def _day_counts(client, spec, ref, from_d, to_d, interval):
    rows = client.query(_sql(spec, ref, from_d, to_d, interval)).result()
    return {date.fromisoformat(str(r["d"])): int(r["n"]) for r in rows}


def _complete_days(counts, min_coverage):
    if not counts:
        return set()
    threshold = statistics.median(counts.values()) * min_coverage
    return {d for d, n in counts.items() if n >= threshold}


def _cluster(days, gap):
    ranges, start, prev = [], days[0], days[0]
    for d in days[1:]:
        if (d - prev).days > gap:
            ranges.append((start, prev))
            start = d
        prev = d
    ranges.append((start, prev))
    return ranges


def _merge_overlaps(ranges):
    merged = []
    for s, e in sorted(ranges):
        if merged and s <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        else:
            merged.append((s, e))
    return merged


def decide(flow_counts, ref_counts, window_start, last_avail, min_coverage=0.5,
           merge_gap_days=4, anchor_prev_day=False, holidays=(), extra_trading_days=()):
    """Pure decision logic (no I/O). ref_counts = {source_name: {date: n}}.
    Returns (ranges, missing, inferred_holidays, notes)."""
    notes = []
    complete_flow = _complete_days(flow_counts, min_coverage)
    confirmed_by = {d: "own table" for d in complete_flow}
    for name, counts in ref_counts.items():
        for d in _complete_days(counts, min_coverage):
            confirmed_by.setdefault(d, name)
    for d in extra_trading_days:
        confirmed_by.setdefault(_to_date(d), "caller calendar")

    skip = {_to_date(h) for h in holidays}
    days, d = [], window_start
    while d <= last_avail:
        if d.weekday() < 5 and d not in skip:
            days.append(d)
        d += timedelta(days=1)

    boundary = max(confirmed_by) if confirmed_by else None
    if not flow_counts:
        notes.append("own table has no rows in the lookback window: planning the whole window; "
                     "use manual mode for older history")
    if not ref_counts:
        notes.append("no reference table configured/readable: a fully missed interior day cannot "
                     "be told from a holiday (partial days are still caught)")

    missing, inferred = {}, []
    for d in days:
        if d in complete_flow:
            continue
        partial = d in flow_counts
        if d in confirmed_by:
            how = "partial in own table" if partial else "absent from own table"
            missing[d] = f"{how}; trading day per {confirmed_by[d]}"
        elif boundary is None or d > boundary:
            missing[d] = "partial, newest data not confirmed" if partial else "newer than any stored data"
        elif partial:
            missing[d] = "partial in own table"
        else:
            inferred.append(d)

    if not missing:
        return [], {}, inferred, notes

    ranges = _cluster(sorted(missing), merge_gap_days)
    if anchor_prev_day:
        anchored = []
        for s, e in ranges:
            prev = max((x for x in complete_flow if x < s), default=None)
            anchored.append((prev or s, e))
        ranges = _merge_overlaps(anchored)
    return ranges, missing, inferred, notes


def _setup(cfg, flow, interval, lookback_days, now, cutoff_ist):
    if flow not in SPECS:
        raise ValueError(f"flow must be one of {sorted(SPECS)}, got {flow!r}")
    spec = SPECS[flow]
    cfg.require("project_id", "dataset_id", spec.table_attr)
    last_avail = last_available_date(spec, now, cutoff_ist)
    return spec, last_avail, last_avail - timedelta(days=lookback_days)


def _sources(cfg, flow):
    """(name, spec, ref) for the flow's own table, then each configured reference table."""
    out = [(flow, SPECS[flow], getattr(cfg, SPECS[flow].ref_attr))]
    refs = [(n, SPECS[n], getattr(cfg, SPECS[n].ref_attr)) for n in REFERENCES[flow]
            if getattr(cfg, SPECS[n].table_attr)]
    return out, refs


def estimate_plan_bytes(cfg, flow, interval=15, lookback_days=30, now=None,
                        cutoff_ist=None, client=None):
    """Dry-run every query plan_dates(mode='auto') would run. Reads no data and
    bills nothing. Returns {table_ref: bytes_processed}."""
    from google.cloud import bigquery
    from .auth import bq_client

    spec, last_avail, window_start = _setup(cfg, flow, interval, lookback_days, now, cutoff_ist)
    client = client or bq_client(cfg)
    own, refs = _sources(cfg, flow)
    out = {}
    for _, s, ref in own + refs:
        job = client.query(_sql(s, ref, window_start, last_avail, interval),
                           job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
        out[ref] = job.total_bytes_processed
    return out


def plan_dates(cfg, flow, mode="auto", from_date=None, to_date=None, interval=15,
               lookback_days=30, min_coverage=0.5, merge_gap_days=4, holidays=(),
               extra_trading_days=(), now=None, cutoff_ist=None, client=None):
    """Decide which date ranges `flow` should fetch.

    flow: "daily" | "intraday" | "bhavcopy" | "bse_bhavcopy".
    mode: "auto" (derive from BigQuery), "manual" (use from_date/to_date as given),
          or None to be asked interactively (Colab-friendly).

    Use the result like:
        plan = plan_dates(cfg_daily, "daily", mode="auto")
        plan.show()
        for f, t in plan.ranges:
            run_daily(cfg_daily, f, t)
    """
    if mode is None:
        mode = _ask_mode()
        if mode == "manual" and not (from_date and to_date):
            from_date = input("from_date (YYYY-MM-DD): ").strip()
            to_date = input("to_date   (YYYY-MM-DD): ").strip()

    if mode == "manual":
        if not (from_date and to_date):
            raise ValueError("manual mode needs from_date and to_date")
        f, t = _to_date(from_date), _to_date(to_date)
        if t < f:
            raise ValueError("to_date must be on/after from_date")
        plan = FetchPlan(flow=flow, mode="manual", ranges=[(f.isoformat(), t.isoformat())])
        spec = SPECS.get(flow)
        if spec and t > last_available_date(spec, now, cutoff_ist):
            plan.notes.append(f"to_date {t} is newer than the last date expected to be published; "
                              "the fetch may return nothing for it")
        return plan
    if mode != "auto":
        raise ValueError("mode must be 'auto' or 'manual'")

    from .auth import bq_client

    spec, last_avail, window_start = _setup(cfg, flow, interval, lookback_days, now, cutoff_ist)
    client = client or bq_client(cfg)
    own, refs = _sources(cfg, flow)

    flow_counts = _day_counts(client, spec, own[0][2], window_start, last_avail, interval)
    ref_counts, notes = {}, []
    for name, s, ref in refs:
        try:
            ref_counts[name] = _day_counts(client, s, ref, window_start, last_avail, interval)
        except Exception as e:
            notes.append(f"reference table {name} unreadable, ignored ({type(e).__name__})")

    ranges, missing, inferred, dnotes = decide(
        flow_counts, ref_counts, window_start, last_avail, min_coverage, merge_gap_days,
        spec.anchor_prev_day, holidays, extra_trading_days)

    return FetchPlan(
        flow=flow, mode="auto",
        ranges=[(s.isoformat(), e.isoformat()) for s, e in ranges],
        missing={d.isoformat(): why for d, why in sorted(missing.items())},
        inferred_holidays=[d.isoformat() for d in inferred],
        notes=notes + dnotes)


def _ask_mode():
    ans = input("Fetch dates: [A]uto (find missing from BigQuery) or [M]anual? [A/m]: ").strip().lower()
    return "manual" if ans.startswith("m") else "auto"
