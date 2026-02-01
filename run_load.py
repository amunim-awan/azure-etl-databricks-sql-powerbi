from __future__ import annotations

import os
import yaml
import pandas as pd
from sqlalchemy import create_engine, text

from src.config import get_config
from src.logging_utils import setup_logging
from src.secrets import SecretProvider
from src.adls import ADLSClient

log = setup_logging("load")


def _sqlalchemy_conn_str(server: str, database: str, username: str, password: str) -> str:
    # ODBC driver name may vary by environment
    driver = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
    params = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    from urllib.parse import quote_plus
    return f"mssql+pyodbc:///?odbc_connect={quote_plus(params)}"


def load_parquet_from_adls(adls: ADLSClient, container: str, prefix: str) -> pd.DataFrame:
    """
    Loads parquet files under a prefix from ADLS (Blob) into a pandas DataFrame.
    This is OK for small/medium tables. For large tables, use ADF Copy Activity or Spark JDBC writes.
    """
    blob_names = adls.list_blobs(container, prefix)
    parquet_blobs = [b for b in blob_names if b.endswith(".parquet")]

    if not parquet_blobs:
        raise FileNotFoundError(f"No parquet files found under {prefix}")

    import pyarrow.parquet as pq
    import pyarrow as pa

    tables = []
    for blob in parquet_blobs:
        data = adls.client.get_blob_client(container=container, blob=blob).download_blob().readall()
        table = pq.read_table(pa.BufferReader(data))
        tables.append(table)

    merged = pa.concat_tables(tables)
    return merged.to_pandas()


def upsert_dataframe(engine, df: pd.DataFrame, table_name: str, key_cols: list[str]) -> None:
    """
    Simple upsert pattern:
    - stage into temp table
    - merge into target
    Requires key columns to exist.

    Note: for production scale, consider:
      - ADF Copy to staging + SQL MERGE
      - Spark JDBC write to staging + MERGE
    """
    if df.empty:
        log.warning("Skipping %s because dataframe is empty", table_name)
        return

    tmp = f"tmp_{table_name}"
    df.to_sql(tmp, engine, if_exists="replace", index=False)

    # Build MERGE statement
    on_clause = " AND ".join([f"t.{c}=s.{c}" for c in key_cols])
    update_cols = [c for c in df.columns if c not in key_cols]
    set_clause = ", ".join([f"t.{c}=s.{c}" for c in update_cols])
    insert_cols = ", ".join(df.columns)
    insert_vals = ", ".join([f"s.{c}" for c in df.columns])

    merge_sql = f"""
    MERGE INTO {table_name} AS t
    USING {tmp} AS s
      ON {on_clause}
    WHEN MATCHED THEN
      UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_cols}) VALUES ({insert_vals});
    """

    with engine.begin() as conn:
        conn.execute(text(merge_sql))
        conn.execute(text(f"DROP TABLE {tmp};"))

    log.info("Upserted %s rows into %s", len(df), table_name)


def main() -> None:
    cfg = get_config()
    secrets = SecretProvider(cfg.keyvault_url)
    adls = ADLSClient(cfg.adls_account_url)

    # SQL creds from Key Vault
    username = secrets.get_secret(os.environ.get("AZURESQL_USERNAME_SECRET_NAME", ""))
    password = secrets.get_secret(os.environ.get("AZURESQL_PASSWORD_SECRET_NAME", ""))

    conn_str = _sqlalchemy_conn_str(cfg.azuresql_server, cfg.azuresql_database, username, password)
    engine = create_engine(conn_str, fast_executemany=True)

    # Example loading: gold dim_customers + fact_payments
    # Adjust key columns based on your real schema
    load_plan = [
        {
            "table": "dbo.dim_customers",
            "prefix": f"gold/core/dim_customers/run_date={cfg.run_date}/",
            "keys": ["id"]
        },
        {
            "table": "dbo.fact_payments",
            "prefix": f"gold/finance/fact_payments/run_date={cfg.run_date}/",
            "keys": ["payment_id"] if True else ["id"]
        },
    ]

    for item in load_plan:
        df = load_parquet_from_adls(adls, cfg.adls_container, item["prefix"])
        upsert_dataframe(engine, df, item["table"], item["keys"])

    log.info("Load complete for run_date=%s", cfg.run_date)


if __name__ == "__main__":
    main()
