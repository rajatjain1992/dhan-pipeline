"""Thin CLI runner for the daily Dhan -> BigQuery pipeline.

Usage:
    export DHAN_CLIENT_ID=...       # or set in your shell / .env
    export DHAN_ACCESS_TOKEN=...
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account.json
    python scripts/run_daily.py
"""
from dhan_pipeline import Config
from dhan_pipeline.daily import run_daily


def main():
    cfg = Config()  # all defaults come from env vars (see config.py)
    if not (cfg.dhan_client_id and cfg.dhan_access_token):
        raise SystemExit("Set DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN env vars.")
    run_daily(cfg)


if __name__ == "__main__":
    main()
