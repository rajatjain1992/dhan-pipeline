"""BigQuery helpers: schema, upsert-via-staging, reads, and flag writing."""
import numpy as np
import pandas as pd
from google.cloud import bigquery

DAILY_SCHEMA = [
    bigquery.SchemaField("scrip", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("exchange", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("security_id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("trade_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("open", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("high", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("low", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("close", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("volume", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("row_id", "STRING", mode="REQUIRED"),
]

FLAG_SCHEMA = [
    bigquery.SchemaField("run_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("scrip", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("security_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("check_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("reason", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("field", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("dhan_value", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("bq_value", "FLOAT64", mode="NULLABLE"),
]


def ensure_table(client, table_ref, schema):
    try:
        client.get_table(table_ref)
    except Exception:
        client.create_table(bigquery.Table(table_ref, schema=schema))


def read_daily(cfg, client, scrips, from_date, to_date):
    """Read stored daily rows for the given scrips + date range."""
    scrips = list(scrips)
    if not scrips:
        return pd.DataFrame(columns=["scrip", "trade_date", "open", "high", "low", "close", "volume"])
    query = f"""
        SELECT scrip, trade_date, open, high, low, close, volume
        FROM `{cfg.daily_ref}`
        WHERE trade_date BETWEEN @from_date AND @to_date
          AND scrip IN UNNEST(@scrips)
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("from_date", "DATE", from_date),
        bigquery.ScalarQueryParameter("to_date", "DATE", to_date),
        bigquery.ArrayQueryParameter("scrips", "STRING", scrips),
    ])
    return client.query(query, job_config=job_config).to_dataframe()


def upsert_daily(cfg, client, df):
    """Upsert `df` into the main daily table via a staging table + MERGE on row_id."""
    if df.empty:
        return 0

    ensure_table(client, cfg.daily_ref, DAILY_SCHEMA)

    df = df.copy()
    df["security_id"] = df["security_id"].astype(np.int64)
    df = df.drop_duplicates(subset=["scrip", "exchange", "security_id", "trade_date", "row_id"])

    client.load_table_from_dataframe(
        df, cfg.staging_ref,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    ).result()

    merge_query = f"""
    MERGE `{cfg.daily_ref}` AS target
    USING `{cfg.staging_ref}` AS source
    ON target.row_id = source.row_id
    WHEN MATCHED THEN UPDATE SET
        target.open = source.open,
        target.high = source.high,
        target.low = source.low,
        target.close = source.close,
        target.volume = source.volume
    WHEN NOT MATCHED THEN
        INSERT (scrip, exchange, security_id, trade_date, open, high, low, close, volume, row_id)
        VALUES (source.scrip, source.exchange, source.security_id, source.trade_date,
                source.open, source.high, source.low, source.close, source.volume, source.row_id)
    """
    client.query(merge_query).result()
    client.delete_table(cfg.staging_ref, not_found_ok=True)
    return len(df)


def write_flags(cfg, client, flags_df):
    """Append corporate-action flags to the flag table."""
    if flags_df.empty:
        return 0
    ensure_table(client, cfg.flag_ref, FLAG_SCHEMA)
    client.load_table_from_dataframe(
        flags_df, cfg.flag_ref,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    ).result()
    return len(flags_df)
