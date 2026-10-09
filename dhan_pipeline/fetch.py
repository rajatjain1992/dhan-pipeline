"""Async Dhan historical OHLCV fetcher — the reusable core used by every script.

Refactored from the proven notebook logic. Returns a tidy DataFrame plus the
list of scrips that failed, so callers stay thin.

DataFrame columns: scrip, exchange, security_id, trade_date, open, high, low,
close, volume
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
            "open", "high", "low", "close", "volume"]


def generate_row_id(row):
    key = f"{row['scrip']}{row['exchange']}{row['security_id']}{row['trade_date']}"
    return hashlib.sha256(key.encode()).hexdigest()


class DhanTokenError(RuntimeError):
    """Dhan rejected the access token (expired / invalid). Run is aborted."""


class DhanNoDataError(RuntimeError):
    """The RELIANCE probe came back empty/failed: Dhan is not returning data."""


def _new_stats():
    return {"ok": 0, "empty": 0, "error": 0, "token": 0,
            "token_msg": None, "empty_scrips": []}


def _is_token_error(status, text):
    t = (text or "").lower()
    return status in (401, 403) or "dh-901" in t or "invalid_authentication" in t         or "token" in t


class AsyncRateLimiter:
    """Caps dispatch to at most `rate_per_sec` requests/sec, shared across every
    concurrent task in a fetch_ohlcv() run (not just within one batch) -- this
    is what actually keeps Dhan from returning 429 in the first place."""

    def __init__(self, rate_per_sec):
        self.min_interval = (1.0 / rate_per_sec) if rate_per_sec > 0 else 0
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def wait(self):
        async with self._lock:
            now = time.monotonic()
            remaining = self.min_interval - (now - self._last)
            if remaining > 0:
                await asyncio.sleep(remaining)
            self._last = time.monotonic()


async def _fetch_one(session, cfg, row, from_date, to_date, failed, rate_limiter,
                     retries=0, rl_retries=0, stats=None):
    stats = stats if stats is not None else _new_stats()
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

    await rate_limiter.wait()
    try:
        timeout = aiohttp.ClientTimeout(total=cfg.fetch_timeout_s)
        async with session.post(cfg.api_url, json=payload, headers=headers,
                                timeout=timeout) as resp:
            status = resp.status

            if status == 200:
                result = await resp.json()
                needed = ["open", "high", "low", "close", "volume", "timestamp"]
                if not all(k in result for k in needed) or len(result.get("open", [])) == 0:
                    stats["empty"] += 1
                    stats["empty_scrips"].append(scrip_name)
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
                    stats["empty"] += 1
                    stats["empty_scrips"].append(scrip_name)
                    return None

                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
                df["trade_date"] = df["timestamp"].dt.tz_convert("Asia/Kolkata").dt.date
                df["scrip"] = scrip_name
                df["exchange"] = row["exc_seg"]
                df["security_id"] = str(row["security_id"])
                stats["ok"] += 1
                return df[OUT_COLS]

            if status == 429:  # rate limited
                if rl_retries >= cfg.max_rate_limit_retries:
                    # Bounded: without this cap, sustained rate-limiting makes
                    # every concurrent task in the batch retry forever, and
                    # asyncio.gather() never returns -- the whole run hangs
                    # with no error and no progress.
                    failed.append(scrip_name)
                    stats["error"] += 1
                    return None
                backoff = min(60, 5 * (2 ** rl_retries))  # 5s, 10s, 20s, 40s, 60s...
                await asyncio.sleep(backoff)
                return await _fetch_one(session, cfg, row, from_date, to_date, failed,
                                        rate_limiter, retries, rl_retries + 1, stats)

            # 400 / 404 / other -> permanent failure for this scrip
            text = await resp.text()
            if _is_token_error(status, text):
                stats["token"] += 1
                stats["token_msg"] = f"HTTP {status}: {text[:200]}"
            failed.append(scrip_name)
            stats["error"] += 1
            return None

    except Exception:
        if retries < cfg.max_retries:
            await asyncio.sleep(2)
            return await _fetch_one(session, cfg, row, from_date, to_date, failed,
                                    rate_limiter, retries + 1, rl_retries, stats)
        failed.append(scrip_name)
        stats["error"] += 1
        return None


async def _fetch_batch(cfg, batch_df, from_date, to_date, failed, rate_limiter, stats=None):
    out = []
    async with aiohttp.ClientSession() as session:
        tasks = [
            _fetch_one(session, cfg, row, from_date, to_date, failed, rate_limiter,
                       stats=stats)
            for _, row in batch_df.iterrows()
        ]
        for res in await asyncio.gather(*tasks, return_exceptions=True):
            if not isinstance(res, Exception) and res is not None:
                out.append(res)
    return out


def _raise_token(stats):
    bar = "!" * 64
    raise DhanTokenError(
        f"\n{bar}\n!! DHAN TOKEN ERROR: {stats['token']} request(s) rejected the "
        f"access token.\n!! {stats['token_msg']}\n!! Generate a new token, update "
        f"cfg.dhan_access_token and rerun. Nothing was uploaded.\n{bar}")


def probe_dhan(cfg, scrip_mapping, from_date, to_date, probe="RELIANCE"):
    """Fetch one liquid scrip first. Fails fast on a bad token or a dead feed,
    instead of after the full batch has run and returned nothing."""
    m = scrip_mapping[scrip_mapping["scrip"] == probe]
    if m.empty:
        return  # subset run (e.g. flagged scrips only): no liquid probe available
    stats, failed = _new_stats(), []
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_fetch_batch(
        cfg, m, from_date, to_date, failed, AsyncRateLimiter(cfg.requests_per_sec), stats))
    if stats["token"]:
        _raise_token(stats)
    if stats["ok"] == 0:
        raise DhanNoDataError(
            f"\n!! PROBE FAILED: {probe} returned no data for {from_date} -> {to_date} "
            f"({stats['empty']} empty, {stats['error']} error). Dhan is not serving "
            f"data for this window; aborting before the full batch.")
    print(f"Probe OK: {probe} returned data for {from_date} -> {to_date}.")


def fetch_ohlcv(cfg, scrip_mapping, from_date, to_date, desc="Fetching", probe="RELIANCE"):
    """Fetch OHLCV for every scrip in `scrip_mapping` between the two dates.

    Returns (data_df, failed_scrips). `failed` = request errors only; scrips
    Dhan answered with no rows are counted as "empty" (see the summary line).
    Raises DhanTokenError on a rejected token and DhanNoDataError if the
    RELIANCE probe is empty. The probe only runs when RELIANCE is in the
    mapping; pass probe=None to skip it.
    """
    nest_asyncio.apply()
    failed = []
    all_data = []
    stats = _new_stats()
    rate_limiter = AsyncRateLimiter(cfg.requests_per_sec)

    loop = asyncio.get_event_loop()
    if probe and len(scrip_mapping):
        probe_dhan(cfg, scrip_mapping, from_date, to_date, probe)

    bar = tqdm(range(0, len(scrip_mapping), cfg.batch_size), desc=desc)
    for i in bar:
        batch = scrip_mapping.iloc[i:i + cfg.batch_size]
        all_data.extend(loop.run_until_complete(
            _fetch_batch(cfg, batch, from_date, to_date, failed, rate_limiter, stats)
        ))
        if hasattr(bar, "set_postfix"):
            bar.set_postfix(ok=stats["ok"], empty=stats["empty"], err=stats["error"],
                            token=stats["token"])
        if stats["token"]:
            _raise_token(stats)
        time.sleep(cfg.batch_pause_s)

    n_ok, n_empty, n_err = stats["ok"], stats["empty"], stats["error"]
    print(f"\nAPI: {n_ok} ok / {n_empty + n_err} not loaded "
          f"({n_empty} empty, {n_err} error, {stats['token']} token)")
    if n_empty:
        shown = stats["empty_scrips"][:30]
        print(f"  empty (no rows from Dhan): {shown}"
              + (f" ... +{n_empty - 30} more" if n_empty > 30 else ""))

    data = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame(columns=OUT_COLS)
    return data, failed
