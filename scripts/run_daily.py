"""Local run TEMPLATE for the daily Dhan -> BigQuery pipeline.

This is a *file*, not part of the reusable package: it holds the variables.
Copy it, fill in your values (or read them from env/secrets), and run.
The repeatable logic all lives in the `dhan_pipeline` package.
"""
import os

from dhan_pipeline import Config, run_daily, recent_window

# ===== VARIABLES — edit these =====
FROM_DATE, TO_DATE = recent_window(days=4)   # or e.g. ('2026-07-27', '2026-07-28')

cfg = Config(
    dhan_client_id       = os.getenv("DHAN_CLIENT_ID", ""),
    dhan_access_token    = os.getenv("DHAN_ACCESS_TOKEN", ""),
    service_account_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", ""),

    project_id       = "rajat-trade",
    dataset_id       = "stock_data_set",
    daily_table      = "stock_daily_prices_dhan",
    staging_table    = "stock_daily_prices_dhan_staging",
    flag_table       = "corporate_action_flags",
    instrument_table = "instrument_list",

    sheet_key          = "1aoEgOhQkAAv8b2NqAWtZUYXG41rOal77i0XasevyNtE",
    list_worksheet     = "my_list",
    negative_worksheet = "Negative List",
)


if __name__ == "__main__":
    run_daily(cfg, FROM_DATE, TO_DATE)
