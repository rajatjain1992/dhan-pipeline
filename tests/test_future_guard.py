"""#997: no Dhan call for dates after today; today itself stays covered. No network.

Run: python -m unittest tests.test_future_guard
"""
import unittest
from datetime import date, datetime
from unittest import mock

from dhan_pipeline import intraday as it
from dhan_pipeline.future_guard import (IST, FutureDateError, clip_inclusive_end, clip_to_date)

TODAY = date(2026, 11, 19)
NOW = datetime(2026, 11, 19, 10, 0, tzinfo=IST)


class FutureGuard(unittest.TestCase):
    def test_clip_to_date_covers_today_and_stops_future(self):
        self.assertEqual(clip_to_date("2026-11-01", "2026-12-31", now=NOW), "2026-11-20")
        self.assertEqual(clip_to_date("2026-11-01", "2026-11-19", now=NOW), "2026-11-19")
        self.assertEqual(clip_to_date("2026-11-19", "2026-11-20", now=NOW), "2026-11-20")
        with self.assertRaises(FutureDateError):
            clip_to_date("2026-11-20", "2026-11-25", now=NOW)

    def test_clip_inclusive_end(self):
        self.assertEqual(clip_inclusive_end("2026-11-01", "2026-12-31", now=NOW), "2026-11-19")
        with self.assertRaises(FutureDateError):
            clip_inclusive_end("2026-11-20", "2026-11-25", now=NOW)

    def test_build_windows_stops_at_today_and_covers_it(self):
        with mock.patch.object(it, "today_ist", lambda now=None: TODAY):
            w = it.build_windows("2026-11-01", 50, 6, 7)
        self.assertEqual(w[-1][1], "2026-11-20")          # exclusive toDate covers today
        self.assertTrue(all(f <= "2026-11-19" for f, _ in w))

    def test_contiguous_windows_clip_future_end(self):
        with mock.patch("dhan_pipeline.future_guard.today_ist", lambda now=None: TODAY):
            w = it.build_contiguous_windows("2026-11-10", "2026-12-31", 5)
        self.assertEqual(w[-1][1], "2026-11-20")


if __name__ == "__main__":
    unittest.main()
