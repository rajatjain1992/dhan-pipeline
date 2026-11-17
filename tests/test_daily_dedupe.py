"""#980.3: duplicate days from Dhan keep the adjusted (lower) row. No BigQuery, no Dhan.

Run: python -m unittest tests.test_daily_dedupe
"""
import unittest
from datetime import date

import pandas as pd

from dhan_pipeline.bq import keep_adjusted


def rows(*spec):
    return pd.DataFrame([{"scrip": "JSWDULUX", "exchange": "NSE_EQ", "security_id": 1467,
                          "trade_date": d, "open": c, "high": c, "low": c, "close": c,
                          "volume": 18135.0} for d, c in spec])


class KeepAdjusted(unittest.TestCase):
    def test_lower_row_kept_whatever_the_order(self):
        d1, d2, d3 = date(2025, 10, 13), date(2025, 10, 14), date(2026, 5, 4)
        for df in (rows((d1, 333.88), (d1, 3338.8), (d2, 3353.5), (d2, 335.35), (d3, 299.3)),
                   rows((d1, 3338.8), (d1, 333.88), (d2, 335.35), (d2, 3353.5), (d3, 299.3))):
            out = keep_adjusted(df)
            self.assertEqual(len(out), 3)
            self.assertEqual(sorted(out["close"]), [299.3, 333.88, 335.35])

    def test_no_duplicates_untouched(self):
        df = rows((date(2026, 5, 4), 299.3), (date(2026, 5, 5), 301.0))
        self.assertIs(keep_adjusted(df), df)

    def test_other_key_columns_not_merged(self):
        df = rows((date(2026, 5, 4), 299.3), (date(2026, 5, 4), 3000.0))
        df.loc[1, "exchange"] = "TEMP"                     # TEMP rows are a different key
        self.assertEqual(len(keep_adjusted(df)), 2)
        self.assertEqual(len(keep_adjusted(df, key=["trade_date"])), 1)


if __name__ == "__main__":
    unittest.main()
