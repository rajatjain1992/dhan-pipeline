"""Load the scrip master list from Google Sheets and derive subsets."""
import pandas as pd


def _ws_to_df(ws):
    rows = ws.get_all_values()
    return pd.DataFrame.from_records(rows[1:], columns=rows[0])


def load_scrip_mapping(cfg, gc):
    """Return the active scrip mapping (my_list minus Negative List).

    Expected columns include: scrip, security_id, exc_seg, instrument_type,
    plus tag columns like `intraday`, `random_fetch`, etc.
    """
    src = gc.open_by_key(cfg.sheet_key)

    scrip_mapping = _ws_to_df(src.worksheet(cfg.list_worksheet))
    negative = _ws_to_df(src.worksheet(cfg.negative_worksheet))

    negative_scrips = negative["scrip"].tolist()
    scrip_mapping = scrip_mapping[~scrip_mapping["scrip"].isin(negative_scrips)]
    return scrip_mapping.reset_index(drop=True)


def subset(scrip_mapping, column, values):
    """Filter the mapping where `column` is in `values` (list)."""
    return scrip_mapping[scrip_mapping[column].isin(values)].reset_index(drop=True)
