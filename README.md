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

## Configure (env vars — nothing hardcoded)

| Var | Meaning |
|-----|---------|
| `DHAN_CLIENT_ID` / `DHAN_ACCESS_TOKEN` | Dhan API creds |
| `GOOGLE_APPLICATION_CREDENTIALS` | path to service-account JSON (Drive path on Colab) |
| `DHAN_GCP_PROJECT` / `DHAN_BQ_DATASET` | BigQuery target (default `rajat-trade` / `stock_data_set`) |
| `DHAN_SHEET_KEY` | Google Sheet with the scrip list |

Any field can also be passed directly: `Config(dhan_client_id="...", ...)`.

## Run the daily pipeline

```python
from dhan_pipeline import Config
from dhan_pipeline.daily import run_daily

cfg = Config()          # reads env vars
result = run_daily(cfg) # fetch 2 days -> split-check -> upsert last day
result["flags"]         # DataFrame of scrips flagged for a suspected split
```

Or from the shell: `python scripts/run_daily.py`

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
  config.py      # single source of truth for every setting (env-driven)
  auth.py        # BigQuery / gspread / dhan client factories
  scrips.py      # load scrip list from Google Sheets, subset helpers
  fetch.py       # async Dhan historical OHLCV fetcher (the reusable core)
  bq.py          # schema, staging+MERGE upsert, reads, flag writer
  splitcheck.py  # corporate-action detection (2nd-last-day compare)
  daily.py       # run_daily(): the whole flow in one call
scripts/run_daily.py          # thin CLI entrypoint
notebooks/daily_pipeline.ipynb# thin Colab notebook
```
