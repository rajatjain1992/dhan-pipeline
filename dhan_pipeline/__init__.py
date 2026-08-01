"""dhan_pipeline — reusable Dhan -> BigQuery OHLCV pipeline.

Standard, shared building blocks so every fetch script (daily, intraday 1m/15m,
options) is a thin caller instead of a copy-pasted notebook.

Typical use:

    from dhan_pipeline import Config, build_clients, load_scrip_mapping
    from dhan_pipeline.daily import run_daily

    cfg = Config(dhan_client_id="...", dhan_access_token="...")
    run_daily(cfg)
"""
from .config import Config
from .auth import build_clients, bq_client, gspread_client, dhan_client, get_credentials
from .scrips import load_scrip_mapping, subset
from .fetch import fetch_ohlcv, generate_row_id
from .daily import run_daily, recent_window

__all__ = [
    "Config",
    "build_clients",
    "bq_client",
    "gspread_client",
    "dhan_client",
    "get_credentials",
    "load_scrip_mapping",
    "subset",
    "fetch_ohlcv",
    "generate_row_id",
    "run_daily",
    "recent_window",
]
