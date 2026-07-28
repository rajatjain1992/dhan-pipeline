"""Async Dhan historical OHLCV fetcher — the reusable core used by every script.

Refactored from the proven notebook logic. Returns a tidy DataFrame plus the
list of scrips that failed, so callers stay thin.

DataFrame columns: scrip, exchange, security_id, trade_date, open, high, low,
close, volume, row_id
"""
import asyncio
import hashlib
import time

import aiohttp
import nest_asyncio
import pandas as pd

try:
    from tqdm.auto import tqdm
except Exception:  # tqdm optional
    def tqdm(x, **k):
        return x

OUT_COLS = ["scrip", "exchange", "security_id", "trade_date",
            "open", "high", "low", "close", "volume", "row_id"]


def generate_row_id(row):
    key = f"{row['scrip']}{row['exchange']}{row['security_id']}{row['trade_date']}"
    return hashlib.sha256(key.encode()).hexdigest()


async def _fetch_one(session, cfg, row, from_date, to_date, failed, retries=0):
    payload = {
        "securityId": str(row["security_id"]),
        "exchangeSegment": row["exc_seg"],
        "instrument": row["instrument_type"],
        "expiryCode": 0,
        "oi": False,
        "fromDate": from_date,
        "toDate": to_date,
    }
    headers = {
        "access-token": cfg.dhan_access_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    scrip_name = row["scrip"]

    try:
        async with session.post(cfg.api_url, json=payload, headers=headers) as resp:
            status = resp.status

            if status == 200:
                result = await resp.json()
                needed = ["open", "high", "low", "close", "volume", "timestamp"]
                if not all(k in result for k in needed) or len(result.get("open", [])) == 0:
                    return None

                df = pd.DataFrame({
                    "timestamp": result["timestamp"],
                    "open": result["open"],
                    "high": result["high"],
                    "low": result["low"],
                    "close": result["close"],
                    "volume": result["volume"],
                })
                if df.empty:
                    return None

                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
                df["trade_date"] = df["timestamp"].dt.tz_convert("Asia/Kolkata").dt.date
                df["scrip"] = scrip_name
                df["exchange"] = row["exc_seg"]
                df["security_id"] = str(row["security_id"])
                df["row_id"] = df.apply(generate_row_id, axis=1)
                return df[OUT_COLS]

            if status == 429:  # rate limited
                await asyncio.sleep(5)
                return await _fetch_one(session, cfg, row, from_date, to_date, failed, retries)

            # 400 / 404 / other -> permanent failure for this scrip
            failed.append(scrip_name)
            return None

    except Exception:
        if retries < cfg.max_retries:
            await asyncio.sleep(2)
            return await _fetch_one(session, cfg, row, from_date, to_date, failed, retries + 1)
        failed.append(scrip_name)
        return None


async def _fetch_batch(cfg, batch_df, from_date, to_date, failed):
    out = []
    async with aiohttp.ClientSession() as session:
        tasks = [
            _fetch_one(session, cfg, row, from_date, to_date, failed)
            for _, row in batch_df.iterrows()
        ]
        for res in await asyncio.gather(*tasks, return_exceptions=True):
            if not isinstance(res, Exception) and res is not None:
                out.append(res)
    return out


def fetch_ohlcv(cfg, scrip_mapping, from_date, to_date):
    """Fetch OHLCV for every scrip in `scrip_mapping` between the two dates.

    Returns (data_df, failed_scrips).
    """
    nest_asyncio.apply()
    failed = []
    all_data = []

    loop = asyncio.get_event_loop()
    for i in tqdm(range(0, len(scrip_mapping), cfg.batch_size), desc="Fetching"):
        batch = scrip_mapping.iloc[i:i + cfg.batch_size]
        all_data.extend(loop.run_until_complete(
            _fetch_batch(cfg, batch, from_date, to_date, failed)
        ))
        time.sleep(cfg.batch_pause_s)

    data = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame(columns=OUT_COLS)
    return data, failed
