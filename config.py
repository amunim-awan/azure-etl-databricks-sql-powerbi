from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv(override=False)


@dataclass(frozen=True)
class AppConfig:
    run_date: str
    log_level: str

    # ADLS / Blob
    adls_account_url: str
    adls_container: str

    # Key Vault
    keyvault_url: str | None

    # Azure SQL
    azuresql_server: str
    azuresql_database: str


def get_config() -> AppConfig:
    run_date = os.getenv("RUN_DATE") or ""
    if not run_date:
        # default to today's date in YYYY-MM-DD
        import datetime as _dt
        run_date = _dt.date.today().isoformat()

    return AppConfig(
        run_date=run_date,
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        adls_account_url=os.environ["ADLS_ACCOUNT_URL"],
        adls_container=os.environ.get("ADLS_CONTAINER", "carwash-datalake"),
        keyvault_url=os.getenv("AZURE_KEYVAULT_URL"),
        azuresql_server=os.environ.get("AZURESQL_SERVER", ""),
        azuresql_database=os.environ.get("AZURESQL_DATABASE", ""),
    )
