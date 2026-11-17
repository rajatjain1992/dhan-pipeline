"""Offline tests for dhan_pipeline.hygiene_run (#980.1). No BigQuery, no Dhan.

Run: python -m unittest tests.test_hygiene_run
"""
import unittest
import warnings
from datetime import date, timedelta
from unittest import mock

import pandas as pd

warnings.filterwarnings("ignore")

from dhan_pipeline import hygiene_run as hr  # noqa: E402
from tests.test_hygiene import FakeClient, cfg, is_write  # noqa: E402

D0 = date(2026, 9, 1)


def days(n):
    return [D0 + timedelta(days=i) for i in range(n)]


def frames(scrip, n, ex_idx, f, vol_scaled=True, odd=None):
    """Daily = adjusted truth. Intraday = stale before ex_idx by price factor f
    (volume 1/f if vol_scaled). `odd` = index of one extra off day after ex."""
    ds = days(n)
    daily = pd.DataFrame({"scrip": scrip, "trade_date": ds, "high": 110.0, "low": 90.0, "volume": 1000.0})
    intra = daily.copy()
    before = intra.index < ex_idx
    intra.loc[before, ["high", "low"]] *= f
    if vol_scaled:
        intra.loc[before, "volume"] /= f
    if odd is not None:
        intra.loc[odd, ["high", "low"]] *= 1.5
    return intra, daily


class DayFactors(unittest.TestCase):
    def test_split_before_ex_date_only(self):
        intra, daily = frames("X", 10, ex_idx=6, f=2.0)
        fix, skipped = hr.day_factors(intra, daily)
        self.assertEqual(sorted(fix["trade_date"]), days(10)[:6])
        self.assertTrue((abs(fix["f"] - 2.0) < 1e-9).all())
        self.assertTrue((abs(fix["vf"] - 2.0) < 1e-9).all())   # daily volume scaled -> scale ours
        self.assertTrue(skipped.empty)

    def test_rights_type_adjustment_price_only(self):
        intra, daily = frames("Y", 8, ex_idx=5, f=1.017, vol_scaled=False)
        fix, _ = hr.day_factors(intra, daily)
        self.assertEqual(len(fix), 5)
        self.assertTrue((fix["vf"] == 1.0).all())

    def test_single_odd_day_is_skipped_not_fixed(self):
        intra, daily = frames("Z", 10, ex_idx=0, f=1.0, odd=7)
        fix, skipped = hr.day_factors(intra, daily)
        self.assertTrue(fix.empty)
        self.assertEqual(list(skipped["trade_date"]), [days(10)[7]])

    def test_split_scales_volume_even_if_daily_volume_unadjusted(self):
        intra, daily = frames("P", 8, ex_idx=5, f=2.0, vol_scaled=False)
        fix, _ = hr.day_factors(intra, daily)
        self.assertTrue((abs(fix["vf"] - 2.0) < 1e-9).all())

    def test_share_ratio(self):
        for f in (2.0, 2.0015, 10.0, 0.1, 1.5, 0.2, 5 / 3):
            self.assertTrue(hr.is_share_ratio(f), f)
        for f in (1.031, 3.186, 2.801, 1.017, 1.008, 4.942, 1.627, 1.238, 0.117):
            self.assertFalse(hr.is_share_ratio(f), f)

    def test_narrow_intraday_range_is_not_a_split(self):
        intra, daily = frames("N", 10, ex_idx=0, f=1.0)
        intra.loc[:, "high"] *= 0.993                     # intraday missed both extremes
        intra.loc[:, "low"] *= 1.007
        fix, skipped = hr.day_factors(intra, daily)
        self.assertTrue(fix.empty and skipped.empty)

    def test_intraday_below_daily_is_daily_stale_not_fixed(self):
        # JSWDULUX: intraday re-downloaded (adjusted 1:10), daily table still unadjusted
        intra, daily = frames("JSW", 8, ex_idx=5, f=0.1)
        fix, skipped = hr.day_factors(intra, daily)
        self.assertTrue(fix.empty)
        self.assertEqual(len(skipped), 5)
        self.assertTrue((skipped["reason"] == "daily_stale").all())

    def test_small_factor_never_scales_volume(self):
        # NATCOPHARM 1.008: daily volume 1/1.008 is noise, not evidence
        intra, daily = frames("NP", 8, ex_idx=5, f=1.008, vol_scaled=True)
        fix, _ = hr.day_factors(intra, daily)
        self.assertEqual(len(fix), 5)
        self.assertTrue((fix["vf"] == 1.0).all())

    def test_volume_factor_voted_per_run(self):
        # rights-type 1.017, price only; one day's volume happens to look scaled
        intra, daily = frames("H", 10, ex_idx=8, f=1.017, vol_scaled=False)
        intra.loc[3, "volume"] /= 1.017
        fix, _ = hr.day_factors(intra, daily)
        self.assertEqual(len(fix), 8)
        self.assertTrue((fix["vf"] == 1.0).all())

    def test_after_fix_nothing_left(self):
        intra, daily = frames("X", 10, ex_idx=6, f=2.0)
        fix, _ = hr.day_factors(intra, daily)
        fixed = intra.merge(fix, on=["scrip", "trade_date"], how="left").fillna({"f": 1.0, "vf": 1.0})
        fixed["high"] /= fixed["f"]
        fixed["low"] /= fixed["f"]
        fixed["volume"] *= fixed["vf"]
        again, skipped = hr.day_factors(fixed[intra.columns], daily)
        self.assertTrue(again.empty and skipped.empty)              # idempotent


class Router:
    """Answers the two history reads; after an UPDATE returns the fixed intraday."""
    def __init__(self, intra, daily, flags=("PGIL",)):
        self.intra, self.daily, self.flags, self.updated = intra, daily, list(flags), False

    def __call__(self, sql):
        if "SUM(volume)" in sql:                                   # history rollup
            if self.updated:
                return self.daily.copy()
            return self.intra.copy()
        if "high, low, volume" in sql and "p.d.daily" in sql:       # history daily
            return self.daily.copy()
        if "MAX(high) AS high, MIN(low) AS low" in sql:             # probe rollup
            return self.intra[self.intra["trade_date"] >= D0][["scrip", "trade_date", "high", "low"]]
        if "FROM `p.d.daily`" in sql:                               # probe daily
            return self.daily[["scrip", "trade_date", "high", "low"]]
        if "FROM `p.d.flags`" in sql:
            return pd.DataFrame({"scrip": self.flags})
        raise AssertionError(sql)


class FixByFactor(unittest.TestCase):
    def test_live_update_params_and_verify(self):
        intra, daily = frames("X", 10, ex_idx=6, f=2.0)
        router = Router(intra, daily)
        fake = FakeClient(router, dml_rows={"UPDATE": 150})
        orig = fake.query

        def q(sql, job_config=None):
            job = orig(sql, job_config)
            if sql.split()[0].upper() == "UPDATE":
                router.updated = True
            return job
        fake.query = q
        out = hr.fix_by_factor(cfg(), fake, ["X"], 15, today=date(2026, 11, 17))
        upd = [i for i, s in enumerate(fake.sql) if s.startswith("UPDATE")]
        self.assertEqual(len(upd), 1)
        p = fake.params[upd[0]]
        self.assertEqual(p["keys"][0], f"X|{D0.isoformat()}")
        self.assertEqual(len(p["keys"]), 6)
        self.assertEqual(p["interval"], 15)
        self.assertIn("t.interval_m = @interval", fake.sql[upd[0]])
        self.assertIn("t.trade_date BETWEEN @lo AND @hi", fake.sql[upd[0]])
        self.assertEqual(out["rows_updated"], 150)
        self.assertEqual(out["still_wrong"], [])
        self.assertEqual(sum(s.count("SUM(volume)") for s in fake.sql), 2)   # read + verify

    def test_daily_stale_not_reported_still_wrong(self):
        intra, daily = frames("JSW", 10, ex_idx=6, f=0.1)
        fake = FakeClient(Router(intra, daily))
        out = hr.fix_by_factor(cfg(), fake, ["JSW"], 15, today=date(2026, 11, 17))
        self.assertFalse(any(is_write(s) for s in fake.sql))
        self.assertEqual(out["daily_stale"], ["JSW"])
        self.assertEqual(out["still_wrong"], [])

    def test_dry_run_writes_nothing(self):
        intra, daily = frames("X", 10, ex_idx=6, f=2.0)
        fake = FakeClient(Router(intra, daily))
        out = hr.fix_by_factor(cfg(), fake, ["X"], 15, dry_run=True)
        self.assertFalse(any(is_write(s) for s in fake.sql))
        self.assertEqual(out["fixed_days"], 6)


class RunHygiene(unittest.TestCase):
    def setUp(self):
        a_i, a_d = frames("A", 40, ex_idx=36, f=2.0)   # ex 7 Oct, after the 10-day probe (5 Oct)
        b_i, b_d = frames("B", 40, ex_idx=0, f=1.0)
        self.intra = pd.concat([a_i, b_i], ignore_index=True)
        self.daily = pd.concat([a_d, b_d], ignore_index=True)
        self.m15 = pd.DataFrame({"scrip": ["A", "B"]})

    def run_h(self, **kw):
        fake = FakeClient(Router(self.intra, self.daily, flags=[]))
        with mock.patch("dhan_pipeline.hygiene_run.bq_client", return_value=fake), \
             mock.patch("dhan_pipeline.hygiene.bq_client", return_value=fake), \
             mock.patch("dhan_pipeline.hygiene_run.run_reconcile_scrips") as rec, \
             mock.patch("dhan_pipeline.intraday.run_intraday_reload_for_flagged") as rl:
            rec.return_value = {}
            out = hr.run_hygiene(cfg(), pd.DataFrame({"scrip": ["A", "B"]}), self.m15,
                                 today=kw.pop("today", date(2026, 10, 15)), **kw)
        return out, fake, rec, rl

    def test_dry_run_no_writes_and_finds_A(self):
        out, fake, rec, rl = self.run_h(dry_run=True)
        self.assertFalse(any(is_write(s) for s in fake.sql), fake.sql)
        self.assertEqual(out[15]["found"], ["A"])
        self.assertEqual(out[15]["fix"]["fixed_days"], 36)
        rl.assert_not_called()

    def test_cleanup_only_on_monday(self):
        _, _, rec, _ = self.run_h(dry_run=True, today=date(2026, 10, 15))   # Thursday
        rec.assert_not_called()
        _, _, rec, _ = self.run_h(dry_run=True, today=date(2026, 10, 12))   # Monday
        rec.assert_called_once()
        self.assertTrue(rec.call_args.kwargs["dry_run"])

    def test_one_minute_never_redownloaded(self):
        # 1m with a scrip a factor cannot fix (single odd day) -> reported, no reload
        i, d = frames("C", 40, ex_idx=0, f=1.0, odd=35)
        fake = FakeClient(Router(i, d, flags=["C"]))
        with mock.patch("dhan_pipeline.hygiene_run.bq_client", return_value=fake), \
             mock.patch("dhan_pipeline.hygiene.bq_client", return_value=fake), \
             mock.patch("dhan_pipeline.intraday.run_intraday_reload_for_flagged") as rl:
            out = hr.run_hygiene(cfg(), pd.DataFrame({"scrip": ["C"]}), None,
                                 pd.DataFrame({"scrip": ["C"]}), today=date(2026, 10, 15),
                                 cleanup=False)
        rl.assert_not_called()
        self.assertEqual(out[1]["fix"]["still_wrong"], ["C"])


class DailyStale(unittest.TestCase):
    def test_refresh_daily_upserts_only_those_scrips(self):
        sm = pd.DataFrame({"scrip": ["JSW", "OTHER"], "security_id": ["1467", "1"],
                           "exc_seg": "NSE_EQ", "instrument_type": "EQUITY"})
        fetched = pd.DataFrame({"scrip": ["JSW"], "close": [300.0]})
        with mock.patch("dhan_pipeline.fetch.fetch_ohlcv", return_value=(fetched, [])) as f,              mock.patch("dhan_pipeline.bq.upsert_daily", return_value=1) as up:
            out = hr.refresh_daily(cfg(), FakeClient(lambda sql: None), sm, ["JSW", "GONE"],
                                   today=date(2026, 11, 17))
        self.assertEqual(list(f.call_args.args[1]["scrip"]), ["JSW"])
        self.assertEqual(f.call_args.args[2], "2001-01-01")
        self.assertEqual(up.call_args.args[0].staging_table, "daily_staging")
        self.assertEqual(out, {"scrips": ["JSW"], "rows": 1, "missing": ["GONE"], "failed": []})

    def test_refresh_daily_dry_run_fetches_nothing(self):
        sm = pd.DataFrame({"scrip": ["JSW"]})
        with mock.patch("dhan_pipeline.fetch.fetch_ohlcv") as f:
            hr.refresh_daily(cfg(), None, sm, ["JSW"], dry_run=True)
        f.assert_not_called()

    def test_run_hygiene_reloads_daily_then_rechecks(self):
        intra, daily = frames("JSW", 40, ex_idx=36, f=0.1)    # intraday adjusted, daily stale
        router = Router(intra, daily, flags=[])

        def refresh(cfg_, client, sm, scrips, today, dry_run):
            router.daily = intra.copy()                     # Dhan's fresh daily = adjusted
            return {"scrips": list(scrips), "rows": 40, "missing": []}
        fake = FakeClient(router)
        m = pd.DataFrame({"scrip": ["JSW"]})
        with mock.patch("dhan_pipeline.hygiene_run.bq_client", return_value=fake),              mock.patch("dhan_pipeline.hygiene.bq_client", return_value=fake),              mock.patch("dhan_pipeline.hygiene_run.refresh_daily", side_effect=refresh) as rd:
            out = hr.run_hygiene(cfg(), m, m, today=date(2026, 10, 15), cleanup=False)
        rd.assert_called_once()
        self.assertFalse(any(s.startswith("UPDATE") for s in fake.sql))   # intraday never moved
        again = out[15]["daily"]["recheck"]
        self.assertEqual(again["daily_stale"], [])
        self.assertEqual(again["fixed_days"], 0)


if __name__ == "__main__":
    unittest.main()
