from __future__ import annotations

import os
import yaml

from src.config import get_config
from src.logging_utils import setup_logging
from src.secrets import SecretProvider
from src.adls import ADLSClient

from src.connectors.rest_api import RestApiClient, PagePagination, IncrementalConfig, iter_paginated, to_jsonl
from src.connectors.quickbooks import QuickBooksClient, QuickBooksAuthConfig


log = setup_logging("extract")


def extract_superoperator(cfg, secrets: SecretProvider, adls: ADLSClient, spec: dict) -> None:
    base_url = os.environ[spec["base_url_env"]]

    api_key_secret_name = os.environ[spec["auth"]["api_key_secret_env"]]
    api_key = secrets.get_secret(api_key_secret_name)

    header_name = spec["auth"]["header_name"]
    header_template = spec["auth"]["header_template"]
    headers = {header_name: header_template.format(api_key=api_key)}

    client = RestApiClient(base_url=base_url, headers=headers)

    for ep in spec["endpoints"]:
        name = ep["name"]
        path = ep["path"]
        pag = PagePagination(**ep.get("pagination", {}))
        inc_cfg = None
        if "incremental" in ep:
            inc = ep["incremental"]
            inc_cfg = IncrementalConfig(param=inc["param"], from_days_ago=int(inc.get("from_days_ago", 7)))

        log.info("Extracting Superoperator endpoint=%s path=%s", name, path)
        records = iter_paginated(client, path, pag, inc_cfg)
        jsonl = to_jsonl(records)

        blob_path = f"bronze/superoperator/{name}/run_date={cfg.run_date}/data.jsonl"
        adls.upload_text(cfg.adls_container, blob_path, jsonl)
        log.info("Wrote %s", blob_path)


def extract_quickbooks(cfg, secrets: SecretProvider, adls: ADLSClient, spec: dict) -> None:
    company_id = os.environ[spec["auth"]["company_id_env"]]
    env = os.getenv(spec["auth"]["env_env"], "production")

    client_id = secrets.get_secret(os.environ[spec["auth"]["client_id_secret_env"]])
    client_secret = secrets.get_secret(os.environ[spec["auth"]["client_secret_secret_env"]])
    refresh_token = secrets.get_secret(os.environ[spec["auth"]["refresh_token_secret_env"]])

    auth = QuickBooksAuthConfig(
        token_url=spec["auth"]["token_url"],
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
    )
    qb = QuickBooksClient(auth=auth, company_id=company_id, env=env)

    for ep in spec["endpoints"]:
        name = ep["name"]
        query = ep["query"]
        log.info("Extracting QuickBooks endpoint=%s", name)
        data = qb.query(query)
        blob_path = f"bronze/quickbooks/{name}/run_date={cfg.run_date}/data.json"
        import json
        adls.upload_text(cfg.adls_container, blob_path, json.dumps(data, ensure_ascii=False, indent=2))
        log.info("Wrote %s", blob_path)


def main() -> None:
    cfg = get_config()
    secrets = SecretProvider(cfg.keyvault_url)
    adls = ADLSClient(cfg.adls_account_url)

    spec_path = os.path.join(os.path.dirname(__file__), "..", "configs", "endpoints.yml")
    with open(spec_path, "r", encoding="utf-8") as f:
        spec = yaml.safe_load(f)

    if "superoperator" in spec:
        extract_superoperator(cfg, secrets, adls, spec["superoperator"])

    if "quickbooks" in spec:
        extract_quickbooks(cfg, secrets, adls, spec["quickbooks"])

    log.info("Extraction complete for run_date=%s", cfg.run_date)


if __name__ == "__main__":
    main()
