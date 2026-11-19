"""Never spend a Dhan call on a date after today (#997).

Dhan's toDate is EXCLUSIVE (a window ending on day D is sent as D+1), so the
largest toDate that still covers today is today+1. Callers whose own end date
is inclusive pass it through `clip_inclusive_end`.
"""
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

DATE_FMT = "%Y-%m-%d"
IST = ZoneInfo("Asia/Kolkata")


class FutureDateError(ValueError):
    """The requested window starts after today: nothing can exist, no call is made."""


def today_ist(now=None):
    return (now or datetime.now(IST)).date()


def _d(x):
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    return datetime.strptime(str(x), DATE_FMT).date()


def _same_type(orig, d):
    return d.strftime(DATE_FMT) if isinstance(orig, str) else d


def clip_to_date(from_date, to_date, label="fetch", now=None):
    """API-level window (toDate exclusive). Raises FutureDateError if from_date is
    after today; otherwise returns to_date clipped to today+1 (covers today)."""
    today = today_ist(now)
    if _d(from_date) > today:
        raise FutureDateError(
            f"{label}: from_date {_d(from_date)} is after today {today}; no API call made.")
    cap = today + timedelta(days=1)
    t = _d(to_date)
    if t > cap:
        print(f"{label}: to_date {t} is in the future, clipped to {cap} (covers today {today}).")
        return _same_type(to_date, cap)
    return to_date


def clip_inclusive_end(start, end, label="fetch", now=None):
    """Caller-level INCLUSIVE end date: cap at today. Raises if start is after today."""
    today = today_ist(now)
    if _d(start) > today:
        raise FutureDateError(
            f"{label}: start {_d(start)} is after today {today}; no API call made.")
    e = _d(end)
    if e > today:
        print(f"{label}: end {e} is in the future, clipped to today {today}.")
        return _same_type(end, today)
    return end
