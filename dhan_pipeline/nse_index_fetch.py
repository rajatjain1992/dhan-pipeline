"""Fetch NSE index constituents from niftyindices.com -> DataFrame.

Repeatable *process* only. Source-site constants (URL pattern, slug map) are
process constants like BASE_URL in bhavcopy.py; the caller may pass its own
`indices` dict to override. Columns returned match what
index_constituents.run_index_constituents loads.

Both niftyindices.com endpoints return HTTP 200 with an HTML error page for a
bad slug, so content is validated ("Company Name" header), not the status code.
Sustained rapid requests trigger redirect loops/502s, so failures are retried
once per index after a longer pause.
"""
import io
import time

import pandas as pd
import requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
}
CONSTITUENT_URL = "https://niftyindices.com/IndexConstituent/ind_{slug}list.csv"

INDICES: dict[str, str] = {
    "NIFTY50": "nifty50",
    "NIFTYNEXT50": "niftynext50",
    "NIFTY100": "nifty100",
    "NIFTY200": "nifty200",
    "NIFTY500": "nifty500",
    "NIFTYTOTALMARKET": "niftytotalmarket_",
    "NIFTYMIDCAP50": "niftymidcap50",
    "NIFTYMIDCAP100": "niftymidcap100",
    "NIFTYMIDCAP150": "niftymidcap150",
    "NIFTYSMALLCAP50": "niftysmallcap50",
    "NIFTYSMALLCAP100": "niftysmallcap100",
    "NIFTYSMALLCAP250": "niftysmallcap250",
    "NIFTYLARGEMIDCAP250": "niftylargemidcap250",
    "NIFTYMIDSMALLCAP400": "niftymidsmallcap400",
    "BANKNIFTY": "niftybank",
    "NIFTYAUTO": "niftyauto",
    "NIFTYFINSERVICE": "niftyfinance",
    "NIFTYFMCG": "niftyfmcg",
    "NIFTYHEALTHCARE": "niftyhealthcare",
    "NIFTYIT": "niftyit",
    "NIFTYMEDIA": "niftymedia",
    "NIFTYMETAL": "niftymetal",
    "NIFTYPHARMA": "niftypharma",
    "NIFTYPVTBANK": "nifty_privatebank",
    "NIFTYPSUBANK": "niftypsubank",
    "NIFTYREALTY": "niftyrealty",
    "NIFTYCONSRDURBL": "niftyconsumerdurables",
    "NIFTYOILGAS": "niftyoilgas",
    "NIFTYCPSE": "niftycpse",
    "NIFTYCONSUMPTION": "niftyconsumption",
    "NIFTYCOMMODITIES": "niftycommodities",
    "NIFTYINFRA": "niftyinfra",
    "NIFTYPSE": "niftypse",
    "NIFTYMNC": "niftymnc",
    "NIFTYDIVOPPS50": "niftydivopp50",
    "NIFTYSERVICE": "niftyservice",
    # sector indices added 2026-08-02
    "NIFTYCEMENT": "niftycement_",
    "NIFTYCHEMICALS": "niftychemicals_",
    "NIFTYENERGY": "niftyenergy",
    "NIFTYCAPITALMKT": "niftycapitalmarkets_",
    "NIFTYCOREHOUSING": "niftycorehousing_",
    # midcap/smallcap variants added 2026-08-02
    "NIFTYMIDCAP150QLTY50": "niftymidcap150quality50",
    "NIFTYMIDCAP150MOM50": "niftymidcap150momentum50_",
    "NIFTYSMALLCAP500": "niftysmallcap500_",
    "NIFTYMICROCAP250": "niftymicrocap250_",
    "NIFTYMIDSMALLCAP400MOMQLTY100": "niftymidsmallcap400momentumquality100_",
    "NIFTYTOTALMKTMOMQLTY50": "niftytotalmarketmomentumquality50_",
    "NIFTYSMALLCAP250MOMQLTY100": "niftysmallcap250momentumquality100_",
    "NIFTYMIDCAPSELECT": "niftymidcapselect_",
    "NIFTYMIDSMALLCAP400_5050": "niftymidsmallcap4005050_",
}

# Live NSE indices NOT covered above (checked 2026-08-02) -- no discoverable
# constituent CSV at niftyindices.com's /IndexConstituent/ path under any
# guessed slug, so they're left out rather than silently wrong:
#   India-theme: NIFTY INDIA DEFENCE, DIGITAL, INTERNET, MANUFACTURING,
#     NEW AGE CONSUMPTION, RAILWAYS PSU, TOURISM, INFRASTRUCTURE & LOGISTICS,
#     FPI 150, NON-CYCLICAL CONSUMER
#   Sector: NIFTY HOUSING, NIFTY EV & NEW AGE AUTOMOTIVE
#   Midcap/smallcap: NIFTY MIDCAP LIQUID 15, NIFTY SMALLCAP250 QUALITY 50
#   Niche: NIFTY SME EMERGE, NIFTY IPO, NIFTY MOBILITY,
#     NIFTY TRANSPORTATION & LOGISTICS, NIFTY RURAL, NIFTY WAVES,
#     NIFTY MIDSMALL FINANCIAL SERVICES/HEALTHCARE/INDIA CONSUMPTION/IT & TELECOM
#   Non-equity: INDIA VIX, all G-Sec/Bharat Bond indices
#   Strategy/factor/smart-beta (~50): Alpha/Quality/Low-Vol/Momentum/Equal
#     Weight/ESG variants of Nifty50/100/200/500, PR/TR leverage-inverse,
#     USD/Shariah, Top-N Equal Weight, etc. -- out of scope, separate ask.

# niftyindices.com uses a DIFFERENT (inconsistently underscored) slug for the
# factsheet PDF than for the constituent CSV. Only include an entry here once
# verified to actually return a PDF (content-type application/pdf) - indices
# missing from this map still get their full constituent list, just no
# top-10-weight / sector-weight breakdown (the factsheet URL couldn't be
# reliably guessed for them).


def fetch_constituents(slug, session):
    resp = session.get(CONSTITUENT_URL.format(slug=slug), headers=HEADERS, timeout=15)
    resp.raise_for_status()
    if not resp.text.lstrip().startswith("Company Name"):
        raise ValueError(f"unexpected constituent CSV for slug={slug} (bad slug?)")
    df = pd.read_csv(io.StringIO(resp.text))
    df.columns = [c.strip() for c in df.columns]
    return df


def fetch_index_constituents(indices=None, delay=0.6, retry_delay=3.0):
    """Return (DataFrame in index_constituents CSV layout, {failed_index: error})."""
    indices = indices or INDICES
    frames, failed = [], {}
    with requests.Session() as s:
        for name, slug in indices.items():
            for attempt, d in enumerate((delay, retry_delay)):
                try:
                    df = fetch_constituents(slug, s)
                    df.insert(0, "index", name)
                    frames.append(df)
                    failed.pop(name, None)
                    print(f"ok  {name}: {len(df)}")
                    break
                except Exception as e:
                    failed[name] = str(e)[:120]
                    time.sleep(retry_delay if attempt == 0 else 0)
            time.sleep(delay)
    for n, e in failed.items():
        print(f"FAIL {n}: {e}")
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return out, failed
