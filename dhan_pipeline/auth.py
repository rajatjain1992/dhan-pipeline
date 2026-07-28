"""Authentication / client factories — the shared 'plumbing' for every script.

Works both on Colab (pass a service-account file mounted from Drive) and locally
(fall back to Application Default Credentials when no file is given).
"""
from google.oauth2 import service_account
from google.cloud import bigquery
import gspread

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery",
]


def get_credentials(cfg):
    """Service-account credentials if a file is configured, else None (ADC)."""
    if cfg.service_account_file:
        return service_account.Credentials.from_service_account_file(
            cfg.service_account_file, scopes=SCOPES
        )
    return None


def bq_client(cfg):
    creds = get_credentials(cfg)
    if creds is not None:
        return bigquery.Client(project=cfg.project_id, credentials=creds)
    return bigquery.Client(project=cfg.project_id)


def gspread_client(cfg):
    creds = get_credentials(cfg)
    if creds is None:
        raise RuntimeError(
            "gspread needs credentials. Set cfg.service_account_file "
            "(GOOGLE_APPLICATION_CREDENTIALS) to a service-account JSON."
        )
    return gspread.authorize(creds)


def dhan_client(cfg):
    """Optional: dhanhq SDK client. The async fetcher uses the REST API directly,
    so this is only needed if you want the SDK for order/quote calls."""
    from dhanhq import DhanContext, dhanhq as _dhan
    if not (cfg.dhan_client_id and cfg.dhan_access_token):
        raise RuntimeError("Set cfg.dhan_client_id and cfg.dhan_access_token.")
    return _dhan(DhanContext(cfg.dhan_client_id, cfg.dhan_access_token))


def build_clients(cfg):
    """Convenience: return (bigquery_client, gspread_client)."""
    return bq_client(cfg), gspread_client(cfg)
