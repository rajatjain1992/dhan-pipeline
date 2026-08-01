# dhan-pipeline

Reusable **Dhan → BigQuery** OHLCV pipeline with built-in **corporate-action (split) detection**.

The old flow was one long Colab notebook with the same boilerplate (auth, async
fetcher, BigQuery upsert, row-id, scrip loader) copy-pasted into every script.
This package pulls all the "standard parts" into one installable module so each
script — daily, intraday 1m/15m, options — is a thin caller.

## Install (Colab or local)

```bash
pip install "git+https://github.com/rajatjain1992/dhan-pipeline.git"
```

## The split: functions in git, variables in your file

**This package contains only functions / repeatable processes — no values.**
Every variable (dates, Google Sheet key, project, dataset, table names,
credentials, tokens) lives in *your* calling file (the Colab notebook or a run
script) and is passed in via `Config(...)`. Nothing project-specific is ever
committed to the package.

`Config` is just a schema with a few process-constants (`api_url`, `batch_size`,
`price_decimals`, ...). You fill the rest:

```python
from dhan_pipeline import Config, run_daily, recent_window

FROM_DATE, TO_DATE = recent_window(days=4)   # or hardcode ('2026-07-27', '2026-07-28')

cfg = Config(
    dhan_client_id="...", dhan_access_token="...",
    service_account_file="/path/to/service_account.json",
    project_id="rajat-trade", dataset_id="stock_data_set",
    daily_table="stock_daily_prices_dhan",
    staging_table="stock_daily_prices_dhan_staging",
    flag_table="corporate_action_flags", instrument_table="instrument_list",
    sheet_key="<google-sheet-key>", list_worksheet="my_list",
    negative_worksheet="Negative List",
)

result = run_daily(cfg, FROM_DATE, TO_DATE)   # fetch window -> split-check -> upsert
result["flags"]                                # scrips flagged for a suspected split
```

Ready-made *files* (edit and use, not part of the reusable logic):
- `notebooks/daily_pipeline.ipynb` — thin Colab notebook, all values in one cell
- `scripts/run_daily.py` — local run template

## How split detection works

1. Every run fetches the **last 2 trading days** (window auto-widens across
   weekends/holidays).
2. The **most-recent day** is the new data to upload.
3. The **2nd-last day** already lives in BigQuery. If a split/bonus happened,
   Dhan silently re-adjusts history, so Dhan's 2nd-last-day OHLC no longer
   equals what we stored.
4. Prices are rounded to `price_decimals` (default `2` = NSE tick) then compared
   for **exact equality**. Any difference → the scrip is **flagged**.
5. Flagged scrips are written to `corporate_action_flags.csv` **and** appended to
   the `corporate_action_flags` BigQuery table (long format: one row per field).
6. By default (`upload_mismatched=True`) the last day is still uploaded for every
   scrip; set `cfg.upload_mismatched=False` to skip flagged scrips instead.

When a scrip is flagged, re-fetch its **full history** and overwrite so the whole
series is on the post-split basis.

## Package layout

```
dhan_pipeline/
  config.py      # Config schema (no values) + process constants
  auth.py        # BigQuery / gspread / dhan client factories
  scrips.py      # load scrip list from Google Sheets, subset helpers
  fetch.py       # async Dhan historical OHLCV fetcher (the reusable core)
  bq.py          # schema, staging+MERGE upsert, reads, flag writer
  splitcheck.py  # corporate-action detection (2nd-last-day compare)
  daily.py       # run_daily(): the whole flow in one call
scripts/run_daily.py          # thin CLI entrypoint
notebooks/daily_pipeline.ipynb# thin Colab notebook
```
