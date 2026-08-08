"""Dhan scrip/instrument master -> BigQuery.

Downloads Dhan's full instrument list CSV, filters to the segments/instrument
types you trade, and fully refreshes the BigQuery table (WRITE_TRUNCATE — this
is a master list, not a time series, so each run replaces it wholesale).

Repeatable *process* only. Every value (project, dataset, table, filter lists)
is supplied by the caller via `cfg` and the arguments to `run_instrument_master`.
"""
import numpy as np
import pandas as pd
import requests

# ---- Process constants ----
SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
HEADERS = {"User-Agent": "Mozilla/5.0"}

COLUMNS_TO_KEEP = [
    "EXCH_ID", "SEGMENT", "SECURITY_ID", "ISIN", "INSTRUMENT",
    "UNDERLYING_SECURITY_ID", "UNDERLYING_SYMBOL", "SYMBOL_NAME",
    "DISPLAY_NAME", "INSTRUMENT_TYPE", "SERIES", "LOT_SIZE",
    "SM_EXPIRY_DATE", "STRIKE_PRICE", "OPTION_TYPE", "TICK_SIZE",
    "EXPIRY_FLAG", "ASM_GSM_FLAG", "ASM_GSM_CATEGORY",
    "BUY_SELL_INDICATOR", "MTF_LEVERAGE",
]
DEFAULT_SEGMENTS = ("D", "E", "I")
DEFAULT_INSTRUMENTS = ("FUTSTK", "FUTIDX", "EQUITY", "INDEX", "OPTIDX")
DEFAULT_INSTRUMENT_TYPES = ("FUT", "FUTIDX", "FUTSTK", "ES", "ETF",
                            "InvITU", "MF", "REIT", "IDX", "INDEX", "OP")


def _schema():
    from google.cloud import bigquery
    return [
        bigquery.SchemaField("EXCH_ID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("SEGMENT", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("SECURITY_ID", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("ISIN", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("INSTRUMENT", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("UNDERLYING_SECURITY_ID", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("UNDERLYING_SYMBOL", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("SYMBOL_NAME", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("DISPLAY_NAME", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("INSTRUMENT_TYPE", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("SERIES", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("LOT_SIZE", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("SM_EXPIRY_DATE", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("STRIKE_PRICE", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("OPTION_TYPE", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("TICK_SIZE", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("EXPIRY_FLAG", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ASM_GSM_FLAG", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ASM_GSM_CATEGORY", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("BUY_SELL_INDICATOR", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("MTF_LEVERAGE", "FLOAT64", mode="NULLABLE"),
    ]


def download_scrip_master(csv_path, url=SCRIP_MASTER_URL):
    """Stream Dhan's scrip master CSV to `csv_path`. Returns the path."""
    with requests.get(url, headers=HEADERS, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(csv_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return csv_path


def load_and_filter(csv_path, segments=DEFAULT_SEGMENTS,
                    instruments=DEFAULT_INSTRUMENTS,
                    instrument_types=DEFAULT_INSTRUMENT_TYPES):
    """Read the downloaded CSV, keep the standard columns, filter to the
    segments/instrument types you trade, and clean types for BigQuery."""
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} rows from scrip master.")

    df["SM_EXPIRY_DATE"] = pd.to_datetime(df["SM_EXPIRY_DATE"], errors="coerce")

    filtered = df[COLUMNS_TO_KEEP]
    filtered = filtered[filtered["SEGMENT"].isin(segments)]
    filtered = filtered[filtered["INSTRUMENT"].isin(instruments)]
    filtered = filtered[filtered["INSTRUMENT_TYPE"].isin(instrument_types)]
    print(f"Filtered to {len(filtered)} rows.")

    print("\nEXCH_ID counts (all):")
    print(df["EXCH_ID"].value_counts())
    print("\nEXCH_ID counts (filtered):")
    print(filtered["EXCH_ID"].value_counts())

    filtered = filtered.copy()
    filtered["ASM_GSM_CATEGORY"] = (
        filtered["ASM_GSM_CATEGORY"].replace({np.nan: None}).astype(str)
    )
    return filtered


def ensure_table(bq, table_ref):
    from google.cloud import bigquery
    try:
        bq.get_table(table_ref)
        print(f"Table {table_ref} already exists.")
    except Exception:
        bq.create_table(bigquery.Table(table_ref, schema=_schema()))
        print(f"Table {table_ref} created.")


def run_instrument_master(cfg, csv_path="scrip_master.csv", url=SCRIP_MASTER_URL,
                          segments=DEFAULT_SEGMENTS, instruments=DEFAULT_INSTRUMENTS,
                          instrument_types=DEFAULT_INSTRUMENT_TYPES):
    """Download Dhan's scrip master, filter it, and fully refresh cfg.instrument_ref.

    A WRITE_TRUNCATE load replaces the whole table in one atomic step -- no
    separate DELETE is needed (your original script's manual delete before the
    truncate load was redundant with what the load already does).

    Args:
        cfg: Config with project_id, dataset_id, instrument_table.
        csv_path: local path to save/read the downloaded CSV.
        url: scrip master URL (rarely needs overriding).
        segments, instruments, instrument_types: filter lists.
    """
    from google.cloud import bigquery
    from .auth import bq_client

    cfg.require("project_id", "dataset_id", "instrument_table")
    table_ref = cfg.instrument_ref

    download_scrip_master(csv_path, url=url)
    filtered = load_and_filter(csv_path, segments, instruments, instrument_types)

    bq = bq_client(cfg)
    ensure_table(bq, table_ref)

    job = bq.load_table_from_dataframe(
        filtered, table_ref,
        job_config=bigquery.LoadJobConfig(schema=_schema(), write_disposition="WRITE_TRUNCATE"),
    )
    job.result()
    print(f"Loaded {len(filtered)} rows into {table_ref}.")
    return {"loaded": len(filtered), "table": table_ref}
