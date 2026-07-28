"""Central configuration — the one place to change "standard parts" for every script.

Values default from environment variables so nothing sensitive is hardcoded.
Override any field explicitly when constructing Config(...).
"""
from dataclasses import dataclass
import os


@dataclass
class Config:
    # ---- GCP / BigQuery ----
    project_id: str = os.getenv("DHAN_GCP_PROJECT", "rajat-trade")
    dataset_id: str = os.getenv("DHAN_BQ_DATASET", "stock_data_set")
    daily_table: str = os.getenv("DHAN_DAILY_TABLE", "stock_daily_prices_dhan")
    staging_table: str = os.getenv("DHAN_STAGING_TABLE", "stock_daily_prices_dhan_staging")
    flag_table: str = os.getenv("DHAN_FLAG_TABLE", "corporate_action_flags")
    instrument_table: str = os.getenv("DHAN_INSTRUMENT_TABLE", "instrument_list")

    # ---- Google Sheet (scrip master list) ----
    sheet_key: str = os.getenv("DHAN_SHEET_KEY", "1aoEgOhQkAAv8b2NqAWtZUYXG41rOal77i0XasevyNtE")
    list_worksheet: str = os.getenv("DHAN_LIST_WS", "my_list")
    negative_worksheet: str = os.getenv("DHAN_NEGATIVE_WS", "Negative List")

    # ---- Dhan API ----
    dhan_client_id: str = os.getenv("DHAN_CLIENT_ID", "")
    dhan_access_token: str = os.getenv("DHAN_ACCESS_TOKEN", "")
    api_url: str = "https://api.dhan.co/v2/charts/historical"

    # ---- Auth ----
    # Path to service-account JSON. Empty -> fall back to Application Default Credentials.
    service_account_file: str = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

    # ---- Fetch tuning ----
    batch_size: int = int(os.getenv("DHAN_BATCH_SIZE", "5"))
    max_retries: int = int(os.getenv("DHAN_MAX_RETRIES", "1"))
    batch_pause_s: float = 0.8  # sleep between batches to respect rate limits

    # ---- Split / corporate-action detection ----
    # Round O/H/L/C to this many decimals before the exact-equality compare.
    # NSE tick = 0.05, so 2 dp is effectively exact while ignoring float-repr noise.
    price_decimals: int = 2
    # User choice: upload the last day for ALL fetched scrips, and just flag
    # mismatched ones. Set False to instead SKIP uploading mismatched scrips.
    upload_mismatched: bool = True

    # ---- Table references (derived) ----
    @property
    def daily_ref(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.daily_table}"

    @property
    def staging_ref(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.staging_table}"

    @property
    def flag_ref(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.flag_table}"

    @property
    def instrument_ref(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.instrument_table}"
