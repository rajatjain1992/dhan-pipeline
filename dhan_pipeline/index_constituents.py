"""NSE index constituents CSV -> BigQuery (replace-by-snapshot).

Repeatable *process* only. Table/project come from `cfg`; the CSV path and
snapshot date come from the calling file.

The CSV is the `index_constituents_*.csv` produced by nse_index_weightage.py
(columns: index, Company Name, Industry, Symbol, Series, ISIN Code).
Re-running for the same snapshot_date replaces only the index_names present in
the CSV for that date, so it never duplicates and never touches other dates.
"""
import pandas as pd

RENAME = {
    "index": "index_name", "Company Name": "company_name", "Industry": "industry",
    "Symbol": "symbol", "Series": "series", "ISIN Code": "isin_code",
}
COLS = ["index_name", "snapshot_date", "company_name", "industry", "symbol", "series", "isin_code"]


def load_csv(csv_path, snapshot_date):
    df = pd.read_csv(csv_path).rename(columns=RENAME)
    df["snapshot_date"] = pd.to_datetime(snapshot_date).date()
    for c in COLS:
        if c not in df.columns:
            raise ValueError(f"CSV missing column for '{c}'")
    return df[COLS].drop_duplicates(subset=["index_name", "symbol"])


def run_index_constituents(cfg, csv_path, snapshot_date, dry_run=True):
    """snapshot_date: 'YYYY-MM-DD'. dry_run=True only reports what would happen
    (no BigQuery call); pass dry_run=False to delete-then-load."""
    from google.cloud import bigquery
    from .auth import bq_client

    cfg.require("project_id", "dataset_id", "index_constituents_table")
    table_id = cfg.index_constituents_ref
    df = load_csv(csv_path, snapshot_date)
    names = sorted(df["index_name"].unique())
    print(f"{len(df)} rows, {len(names)} indices, snapshot {snapshot_date} -> {table_id}")
    if dry_run:
        print("dry_run=True: nothing sent to BigQuery.")
        return {"rows": len(df), "indices": len(names), "table": table_id, "loaded": 0}

    client = bq_client(cfg)
    try:
        client.get_table(table_id)
        client.query(
            f"DELETE FROM `{table_id}` WHERE snapshot_date = @d AND index_name IN UNNEST(@n)",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("d", "DATE", str(snapshot_date)),
                bigquery.ArrayQueryParameter("n", "STRING", names),
            ]),
        ).result()
    except Exception as e:
        if "Not found" not in str(e):
            raise
    client.load_table_from_dataframe(
        df, table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True),
    ).result()
    print(f"Loaded {len(df)} rows into {table_id}")
    return {"rows": len(df), "indices": len(names), "table": table_id, "loaded": len(df)}
