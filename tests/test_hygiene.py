"""Offline tests for dhan_pipeline.hygiene (#980). No BigQuery, no Dhan.

Run: python -m unittest discover -s tests -v
"""
import unittest
import warnings
from datetime import date
from unittest import mock

import pandas as pd

warnings.filterwarnings("ignore")

from dhan_pipeline import hygiene as h  # noqa: E402
from dhan_pipeline.config import Config  # noqa: E402


def _d(s):
    return date.fromisoformat(s)


class ProbeWindows(unittest.TestCase):
    def test_windows(self):
        w = h.probe_windows(_d("2026-11-17"), (10, 45))
        self.assertEqual(w[0], (_d("2026-11-07"), _d("2026-11-01"), _d("2026-11-07")))
        self.assertEqual(w[1][0], _d("2026-10-03"))


class ClassifyProbe(unittest.TestCase):
    def frames(self, ih, il, dh, dl):
        intra = pd.DataFrame({"scrip": ["X"], "trade_date": ["2026-10-01"], "high": [ih], "low": [il]})
        daily = pd.DataFrame({"scrip": ["X"], "trade_date": ["2026-10-01"], "high": [dh], "low": [dl]})
        return intra, daily

    def verdict(self, *a):
        return h.classify_probe(*self.frames(*a))["verdict"].iloc[0]

    def test_ok_exact_and_within_tolerance(self):
        self.assertEqual(self.verdict(100, 90, 100, 90), "ok")
        self.assertEqual(self.verdict(100.3, 90, 100, 90), "ok")

    def test_split_1_to_2(self):
        self.assertEqual(self.verdict(200, 180, 100, 90), "split")

    def test_bonus_5_4_missed_by_old_30pct_check(self):
        self.assertEqual(self.verdict(125, 112.5, 100, 90), "split")

    def test_one_field_off_is_diff_not_split(self):
        self.assertEqual(self.verdict(104, 90, 100, 90), "diff")

    def test_factors_disagree_is_diff(self):
        self.assertEqual(self.verdict(200, 100, 100, 90), "diff")

    def test_empty_and_zero(self):
        self.assertTrue(h.classify_probe(pd.DataFrame(columns=["scrip", "trade_date", "high", "low"]),
                                         *self.frames(1, 1, 1, 1)[1:]).empty)
        self.assertTrue(h.classify_probe(*self.frames(0, 0, 100, 90)).empty)


class LatestCommonDay(unittest.TestCase):
    def test_keeps_latest_per_window(self):
        df = pd.DataFrame({"scrip": ["X", "X", "X"],
                           "trade_date": [_d("2026-11-03"), _d("2026-11-05"), _d("2026-10-01")],
                           "verdict": ["split", "ok", "ok"]})
        w = [(None, _d("2026-11-01"), _d("2026-11-07")), (None, _d("2026-09-28"), _d("2026-10-04"))]
        out = h.latest_common_day(df, w)
        self.assertEqual(sorted(out["trade_date"]), [_d("2026-10-01"), _d("2026-11-05")])


class ClassifyOrphans(unittest.TestCase):
    def setUp(self):
        self.mapping = pd.DataFrame({"scrip": ["AAA", "NEWNAME"], "security_id": ["1", " 2 "],
                                     "exc_seg": ["NSE_EQ"] * 2})
        self.stored = pd.DataFrame({
            "scrip": ["AAA", "OLDNAME", "GONE", "NIFTY"],
            "security_id": [1, 2, 3, 13],      # daily stores INTEGER
            "n_rows": [10, 20, 30, 40],
            "first_date": [_d("2024-01-01")] * 4, "last_date": [_d("2026-11-14")] * 4})

    def test_classify(self):
        o = h.classify_orphans(self.stored, self.mapping, protect=["NIFTY"])
        got = {r.scrip: (r.kind, r.new_scrip) for r in o.itertuples()}
        self.assertEqual(got["OLDNAME"], ("renamed", "NEWNAME"))
        self.assertEqual(got["GONE"][0], "dropped")
        self.assertTrue(pd.isna(got["GONE"][1]))
        self.assertNotIn("AAA", got)
        self.assertNotIn("NIFTY", got)          # protected

    def test_unprotected_index_shows_as_dropped(self):
        o = h.classify_orphans(self.stored, self.mapping)
        self.assertIn("NIFTY", set(o["scrip"]))

    def test_nothing_orphaned(self):
        s = self.stored[self.stored["scrip"] == "AAA"]
        self.assertTrue(h.classify_orphans(s, self.mapping).empty)


class FakeJob:
    def __init__(self, df=None, n=0):
        self._df, self.num_dml_affected_rows = df, n

    def result(self):
        return self

    def to_dataframe(self):
        return self._df if self._df is not None else pd.DataFrame()


class FakeClient:
    """Records SQL; answers SELECTs from a router(sql) -> DataFrame."""
    def __init__(self, router, dml_rows=None):
        self.sql, self.router, self.dml_rows = [], router, dml_rows or {}

    def query(self, sql, job_config=None):
        self.sql.append(" ".join(sql.split()))
        head = self.sql[-1].split()[0].upper()
        if head in ("DELETE", "UPDATE", "INSERT"):
            return FakeJob(n=self.dml_rows.get(head, 0))
        return FakeJob(df=self.router(self.sql[-1]))


def cfg():
    return Config(project_id="p", dataset_id="d", daily_table="daily",
                  intraday_table="intra", flag_table="flags")


def is_write(s):
    return s.split()[0].upper() in ("DELETE", "UPDATE", "INSERT", "CREATE", "MERGE")


class SplitProbeRunner(unittest.TestCase):
    def router(self, sql):
        if "FROM `p.d.intra`" in sql:
            return pd.DataFrame({"scrip": ["HEG", "OK"], "trade_date": [_d("2026-10-01")] * 2,
                                 "high": [500.0, 100.0], "low": [450.0, 90.0]})
        if "FROM `p.d.daily`" in sql:
            return pd.DataFrame({"scrip": ["HEG", "OK"], "trade_date": [_d("2026-10-01")] * 2,
                                 "high": [100.0, 100.0], "low": [90.0, 90.0]})
        if "FROM `p.d.flags`" in sql:
            return pd.DataFrame({"scrip": ["PGIL"]})
        raise AssertionError(sql)

    def test_dry_run_reads_only_and_finds_split_plus_cascade(self):
        fake = FakeClient(self.router)
        with mock.patch.object(h, "bq_client", return_value=fake), \
             mock.patch("dhan_pipeline.intraday.run_intraday_reload_for_flagged") as rl:
            rl.return_value = {"dry_run": True}
            out = h.run_intraday_split_probe(
                cfg(), today=_d("2026-11-17"), probe_back_days=(47,),
                cascade_since=_d("2026-11-01"), write_flags_to_bq=True,
                reload=True, scrip_mapping=pd.DataFrame(), dry_run=True)
        self.assertEqual(out["split_scrips"], ["HEG"])
        self.assertEqual(out["flagged"], ["HEG", "PGIL"])
        self.assertFalse(any(is_write(s) for s in fake.sql), fake.sql)
        self.assertTrue(rl.call_args.kwargs["dry_run"])
        # every price-table read is partition-bounded
        for s in fake.sql:
            if "p.d.intra" in s or "p.d.daily" in s:
                self.assertIn("trade_date BETWEEN @lo AND @hi", s)


class ReconcileRunner(unittest.TestCase):
    def router(self, sql):
        if "GROUP BY scrip, security_id" in sql:
            return pd.DataFrame({"scrip": ["AAA", "OLDNAME", "GONE"], "security_id": ["1", "2", "3"],
                                 "n_rows": [5, 6, 7],
                                 "first_date": [_d("2024-01-01")] * 3,
                                 "last_date": [_d("2026-11-14")] * 3})
        if sql.startswith("SELECT COUNT(*)"):
            return pd.DataFrame({"n": [7]})
        if sql.startswith("CREATE"):
            return pd.DataFrame()
        raise AssertionError(sql)

    mapping = pd.DataFrame({"scrip": ["AAA", "NEWNAME"], "security_id": ["1", "2"]})

    def test_dry_run_changes_nothing(self):
        fake = FakeClient(self.router)
        with mock.patch.object(h, "bq_client", return_value=fake):
            out = h.run_reconcile_scrips(cfg(), self.mapping, dry_run=True)
        self.assertFalse(any(is_write(s) for s in fake.sql), fake.sql)
        self.assertEqual(set(out["daily"]["orphans"]["scrip"]), {"OLDNAME", "GONE"})
        self.assertIn("exchange != 'TEMP'", fake.sql[0])

    def test_live_archives_before_delete(self):
        fake = FakeClient(self.router, dml_rows={"INSERT": 7, "DELETE": 7, "UPDATE": 6})
        with mock.patch.object(h, "bq_client", return_value=fake):
            out = h.run_reconcile_scrips(cfg(), self.mapping, tables=("intraday",), dry_run=False)
        writes = [s for s in fake.sql if is_write(s)]
        kinds = [s.split()[0].upper() for s in writes]
        # rename: overlap delete then update; dropped: create, insert, delete
        self.assertEqual(kinds, ["DELETE", "UPDATE", "CREATE", "INSERT", "DELETE"])
        self.assertIn("n.interval_m = o.interval_m", writes[0])
        self.assertIn("`p.d.intra_orphans`", writes[3])
        self.assertTrue(all("trade_date BETWEEN @lo AND @hi" in s for s in writes if not s.startswith("CREATE")))
        self.assertEqual(out["intraday"]["deleted"], 7)

    def test_archive_mismatch_aborts_delete(self):
        fake = FakeClient(self.router, dml_rows={"INSERT": 3, "DELETE": 7, "UPDATE": 6})
        with mock.patch.object(h, "bq_client", return_value=fake):
            out = h.run_reconcile_scrips(cfg(), self.mapping, tables=("daily",), dry_run=False)
        dropped_deletes = [s for s in fake.sql if s.startswith("DELETE FROM `p.d.daily` WHERE scrip IN")]
        self.assertEqual(dropped_deletes, [])
        self.assertEqual(out["daily"]["deleted"], 0)


if __name__ == "__main__":
    unittest.main()
