"""Config *schema* only — no business values live here.

Git holds the repeatable structure; every actual value (project, dataset, table
names, sheet key, credentials, tokens, dates) is supplied by the CALLING FILE
(your notebook / run script), never committed to this package.

The only defaults kept here are true *process constants* — things that are part
of the pipeline itself, not your setup (the Dhan endpoint, batch tuning, the
tick-rounding used by the split check). Override them if you ever need to.
"""
from dataclasses import dataclass


@dataclass
class Config:
    # ---- Project-specific values: MUST be set by the calling file ----
    dhan_client_id: str = ""
    dhan_access_token: str = ""
    service_account_file: str = ""      # path to service-account JSON (empty -> ADC)
    project_id: str = ""
    dataset_id: str = ""
    daily_table: str = ""
    staging_table: str = ""
    flag_table: str = ""
    instrument_table: str = ""
    bhav_table: str = ""
    mcap_table: str = ""
    option_table: str = ""
    sheet_key: str = ""
    list_worksheet: str = ""
    negative_worksheet: str = ""

    # ---- Process constants (part of the repeatable pipeline) ----
    api_url: str = "https://api.dhan.co/v2/charts/historical"
    batch_size: int = 5
    max_retries: int = 1
    batch_pause_s: float = 0.8
    price_decimals: int = 2          # round O/H/L/C to tick before exact compare
    upload_mismatched: bool = True   # upload flagged scrips anyway (vs skip them)

    def require(self, *fields):
        """Raise if any named field is still empty — call from your run file."""
        missing = [f for f in fields if not getattr(self, f)]
        if missing:
            raise ValueError(f"Config is missing required values: {missing}")

    # ---- Table references (derived) ----
    @property
    def daily_ref(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.daily_table}"

    @property
    def staging_ref(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.staging_table}"

    @property
    def flag_ref(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.flag_table}"

    @property
    def instrument_ref(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.instrument_table}"

    @property
    def bhav_ref(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.bhav_table}"

    @property
    def mcap_ref(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.mcap_table}"

    @property
    def option_ref(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.option_table}"
