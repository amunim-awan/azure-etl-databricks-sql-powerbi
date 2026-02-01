"""
PySpark transformations for bronze -> silver -> gold.

Typical Databricks usage:
  spark-submit pipelines/run_transform.py --run-date 2026-02-01

This script assumes bronze JSONL/JSON files are already in ADLS/Blob.
"""
from __future__ import annotations

import argparse
import os
from typing import Dict, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, lit, current_timestamp

from src.logging_utils import setup_logging

log = setup_logging("transform")


def read_bronze_jsonl(spark: SparkSession, adls_abfss_prefix: str, source: str, endpoint: str, run_date: str) -> DataFrame:
    path = f"{adls_abfss_prefix}/bronze/{source}/{endpoint}/run_date={run_date}/data.jsonl"
    # For JSONL, Spark can read as json lines
    return spark.read.json(path)


def write_parquet(df: DataFrame, adls_abfss_prefix: str, layer: str, domain: str, table: str, run_date: str) -> None:
    out = f"{adls_abfss_prefix}/{layer}/{domain}/{table}/run_date={run_date}/"
    (
        df.write
        .mode("overwrite")
        .parquet(out)
    )
    log.info("Wrote %s", out)


def clean_customers(bronze: DataFrame) -> DataFrame:
    """
    Example cleaning:
    - Deduplicate by id (if present)
    - Cast typical fields
    - Standardize names/emails
    """
    df = bronze

    # Common patterns, adjust to your real schema
    if "id" in df.columns:
        df = df.dropDuplicates(["id"])

    for c in ("created_at", "updated_at"):
        if c in df.columns:
            df = df.withColumn(c, to_timestamp(col(c)))

    if "email" in df.columns:
        df = df.withColumn("email", col("email").cast("string"))

    df = df.withColumn("etl_loaded_at", current_timestamp())
    return df


def clean_payments(bronze: DataFrame) -> DataFrame:
    df = bronze
    if "payment_id" in df.columns:
        df = df.dropDuplicates(["payment_id"])
    if "amount" in df.columns:
        df = df.withColumn("amount", col("amount").cast("double"))
    for c in ("created_at", "paid_at"):
        if c in df.columns:
            df = df.withColumn(c, to_timestamp(col(c)))
    df = df.withColumn("etl_loaded_at", current_timestamp())
    return df


def gold_facts(customers_silver: DataFrame, payments_silver: DataFrame) -> Dict[str, DataFrame]:
    """
    Example gold tables:
    - dim_customers
    - fact_payments
    Add more as your model evolves.
    """
    dim_customers = customers_silver.select(
        *[c for c in customers_silver.columns if c not in ("etl_loaded_at",)]
    )

    fact_payments = payments_silver

    return {
        "dim_customers": dim_customers,
        "fact_payments": fact_payments,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--abfss-prefix", required=True, help="Example: abfss://container@account.dfs.core.windows.net")
    args = parser.parse_args()

    run_date = args.run_date
    prefix = args.abfss_prefix.rstrip("/")

    spark = SparkSession.builder.appName("superoperator-etl-transform").getOrCreate()

    # Bronze -> Silver
    customers_bronze = read_bronze_jsonl(spark, prefix, "superoperator", "customers", run_date)
    payments_bronze = read_bronze_jsonl(spark, prefix, "superoperator", "payments", run_date)

    customers_silver = clean_customers(customers_bronze)
    payments_silver = clean_payments(payments_bronze)

    write_parquet(customers_silver, prefix, "silver", "core", "customers", run_date)
    write_parquet(payments_silver, prefix, "silver", "finance", "payments", run_date)

    # Silver -> Gold (curated)
    gold_tables = gold_facts(customers_silver, payments_silver)
    for table_name, df in gold_tables.items():
        domain = "core" if table_name.startswith("dim_") else "finance"
        write_parquet(df, prefix, "gold", domain, table_name, run_date)

    log.info("Transform complete for run_date=%s", run_date)


if __name__ == "__main__":
    main()
