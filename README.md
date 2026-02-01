# Superoperator Car Wash ETL + BI on Azure

## What this repo is
This repository is a Python-based ETL scaffold that mirrors a real production build for a car wash business where operational and financial data lived in two main sources: Superoperator and QuickBooks Online.

The goal is to:
- pull data from REST APIs,
- land raw data in an Azure data lake (bronze),
- clean + normalize it with Spark (silver),
- publish curated analytics tables (gold),
- and load those tables into Azure SQL for Power BI reporting.

This repo is designed so you can run the same logic locally for testing, then move execution to Azure (ADF + Databricks) for daily scheduled ETL.

---

## Architecture

### Data sources
- Superoperator (core operations data)
- QuickBooks Online (financial data)

### Azure flow (production pattern)
1. Extract: Azure Data Factory pulls from REST APIs into ADLS Gen2 (bronze layer)
2. Transform: Azure Databricks reads from bronze, cleans + normalizes into silver, then writes curated gold tables back to ADLS Gen2
3. Load: ADF Copy Activity moves gold tables into Azure SQL Server (data warehouse)
4. BI: Power BI connects to Azure SQL Server for reporting and dashboards

### Security and testing
- Microsoft Entra ID for access control and secure authentication
- Azure Key Vault for secrets (API keys, DB credentials)
- Postman used to validate endpoints and responses during development

---

## Folder structure

- `configs/`
  - `endpoints.yml`: add endpoints, pagination rules, and incremental settings.
- `pipelines/`
  - `run_extract.py`: pulls Superoperator + QuickBooks data and lands it in bronze.
  - `run_transform.py`: PySpark bronze → silver → gold transforms (Databricks-ready).
  - `run_load.py`: loads gold parquet into Azure SQL (small/medium tables).
  - `run_all_local.py`: convenience runner for local debugging.
- `src/`
  - `adls.py`: upload/download utilities for ADLS Gen2.
  - `secrets.py`: Key Vault secret provider with env fallback.
  - `connectors/`: REST + QuickBooks connectors
  - `qc/`: lightweight data quality checks
- `tests/`: small unit tests for QC utilities

---

## Data lake layout (Bronze / Silver / Gold)

This repo follows the same 3-layer structure:
- bronze: raw API dumps (kept as-is)
- silver: cleaned and normalized tables
- gold: analytics-ready tables (facts/dimensions)

Example paths:
- `bronze/superoperator/payments/run_date=YYYY-MM-DD/data.jsonl`
- `silver/finance/payments/run_date=YYYY-MM-DD/part-*.parquet`
- `gold/finance/fact_payments/run_date=YYYY-MM-DD/part-*.parquet`

---

## How to run (local dev)

### 1) Setup environment
1. Create a Python venv
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
