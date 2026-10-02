"""Zerodha tax P&L 'Tradewise Exits' (F&O section only) -> BigQuery.

Incremental + re-upload safe: rows from the table's last loaded exit day onward
are deleted, then the file's rows from that day onward are appended with
fiscal_year / trade_id / row_id. Older rows are never touched.

Repeatable *process* only: project/dataset/table come from `cfg`
(cfg.fno_exits_table); the xlsx path or bytes comes from the caller.
"""
import pandas as pd
from google.cloud import bigquery

from .auth import bq_client

COLS = {
    "Symbol": "symbol", "Entry Date": "entry_date", "Exit Date": "exit_date",
    "Quantity": "quantity", "Buy Value": "buy_value", "Sell Value": "sell_value",
    "Profit": "profit", "Turnover": "turnover", "Brokerage": "brokerage",
    "Exchange Transaction Charges": "exchange_txn_charges", "IPFT": "ipft",
    "SEBI Charges": "sebi_charges", "CGST": "cgst", "SGST": "sgst",
    "IGST": "igst", "Stamp Duty": "stamp_duty", "STT": "stt",
}
INT_COLS = ["quantity", "cgst", "sgst"]


def read_fno(path):
    import io, openpyxl
    if isinstance(path, (bytes, bytearray)):
        path = io.BytesIO(path)
    wb = openpyxl.load_workbook(path, data_only=True)
    name = next(s for s in wb.sheetnames if s.startswith("Tradewise Exits"))
    rows = [tuple(r) + (None,) * (24 - len(r)) for r in wb[name].iter_rows(values_only=True)]
    start = next(i for i, r in enumerate(rows) if r[1] == "F&O" and r[2] is None)
    hdr = next(i for i in range(start, len(rows)) if rows[i][1] == "Symbol")
    header = [c for c in rows[hdr][1:] if c is not None]
    data = []
    for r in rows[hdr + 1:]:
        if r[1] is None:
            if any(v is not None for v in r):
                break
            continue
        if r[2] is None:  # next section title (Currency / Commodity)
            break
        data.append(r[1:1 + len(header)])
    df = pd.DataFrame(data, columns=header).rename(columns=COLS)
    df["entry_date"] = pd.to_datetime(df["entry_date"])
    df["exit_date"] = pd.to_datetime(df["exit_date"])
    for c in INT_COLS:
        assert (df[c] % 1 == 0).all(), f"{c} has non-integer values"
        df[c] = df[c].astype("int64")
    for c in COLS.values():
        if c not in INT_COLS + ["symbol", "entry_date", "exit_date"]:
            df[c] = df[c].astype(float)
    y = df["exit_date"].dt.year - (df["exit_date"].dt.month < 4)
    df["fiscal_year"] = "fy" + (y % 100).astype(str).str.zfill(2) + ((y + 1) % 100).astype(str).str.zfill(2)
    return df


def assign_trade_id(df, start_id):
    df = df.sort_values(["symbol", "entry_date", "exit_date"]).reset_index(drop=True)
    ids, nid, cur_sym, run_max = [], start_id - 1, None, None
    for sym, en, ex in zip(df["symbol"], df["entry_date"], df["exit_date"]):
        if sym != cur_sym or en >= run_max:
            nid += 1
            cur_sym, run_max = sym, ex
        else:
            run_max = max(run_max, ex)
        ids.append(nid)
    df["trade_id"] = ids
    return df



def run_taxpnl_fno(cfg, source, dry_run=False):
    """source: path to the taxpnl xlsx, or its raw bytes (e.g. from files.upload())."""
    cfg.require("project_id", "dataset_id", "fno_exits_table")
    df = read_fno(source)
    print(len(df), "F&O rows;", df["exit_date"].min(), "..", df["exit_date"].max(),
          "| profit", round(df["profit"].sum(), 2))
    if dry_run:
        return df
    client = bq_client(cfg)
    table = cfg.fno_exits_ref
    last = next(iter(client.query(f"SELECT MAX(exit_date) m FROM `{table}`").result())).m
    lo = max(df["exit_date"].min(), pd.Timestamp(last).normalize()) if last else df["exit_date"].min()
    df = df[df["exit_date"] >= lo]
    P = bigquery.ScalarQueryParameter
    client.query(
        f"DELETE FROM `{table}` WHERE exit_date >= @lo",
        job_config=bigquery.QueryJobConfig(query_parameters=[P("lo", "DATETIME", lo.to_pydatetime())]),
    ).result()
    mx = next(iter(client.query(
        f"SELECT IFNULL(MAX(trade_id),0) t, IFNULL(MAX(row_id),0) r FROM `{table}`").result()))
    df = assign_trade_id(df, mx.t + 1).sort_values("exit_date", kind="stable").reset_index(drop=True)
    df["row_id"] = range(mx.r + 1, mx.r + 1 + len(df))
    schema = client.get_table(table).schema
    client.load_table_from_dataframe(
        df[[f.name for f in schema]], table,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", schema=schema),
    ).result()
    print(f"deleted exit_date >= {lo}; loaded {len(df)} rows -> {table}")
    return df
