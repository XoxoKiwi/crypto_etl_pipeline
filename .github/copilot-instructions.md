# Crypto ETL Pipeline - AI Agent Instructions

## Architecture Overview
This is an ETL pipeline for cryptocurrency data from CoinGecko API to Snowflake. Supports both standalone Python execution (`main.py`) and Airflow orchestration (`dags/crypto_pipeline_v2.py`).

**Data Flow:**
- Extract: Fetch from CoinGecko API with retry logic → save to `data/raw_crypto.json`
- Transform: Validate schema/data quality with pandas → output `data/processed_crypto/cleaned_data.csv`
- Load: Idempotent MERGE into Snowflake using temp staging tables

**Key Components:**
- `src/extract.py`: API extraction with exponential backoff (Retry strategy for 429/5xx errors)
- `src/transform.py`: Data contracts (required columns: id, symbol, current_price, market_cap; quality checks)
- `src/load.py`: Snowflake loading with MERGE for deduplication
- `src/logger_util.py`: Pipeline observability logging to PIPELINE_LOGS table

## Critical Workflows
- **Airflow Setup:** `docker-compose up` launches standalone Airflow. DAG runs hourly, uses SnowflakeOperator for loading.
- **Standalone Run:** `python main.py` (requires .env with SNOWFLAKE_USER/PASS/ACCOUNT)
- **Database Init:** Execute `database_setup.sql` in Snowflake to create CRYPTO_DB.PUBLIC.COIN_DATA and PIPELINE_LOGS tables
- **Analysis:** Use `analysis_queries.sql` for dashboard queries (latest data, health checks, aggregations)

## Project Conventions
- **Idempotency:** Always use MERGE statements for loads to prevent duplicates (see `src/load.py` and DAG)
- **Staging Pattern:** Load data into temp tables first, then MERGE (avoids partial failures)
- **Error Handling:** Exponential backoff in API calls; data contracts in transforms; comprehensive logging
- **Secrets:** Use python-dotenv (.env file) for Snowflake credentials
- **Data Quality:** Fill NaN with 0 before Snowflake loads (handles API nulls)
- **Logging:** Every pipeline run logs status/message/rows to PIPELINE_LOGS table

## Integration Points
- **CoinGecko API:** Rate-limited; fetch top coins by market cap
- **Snowflake:** Warehouse=COMPUTE_WH (or CRYPTO_WH in DAG), Database=CRYPTO_DB, Schema=PUBLIC
- **Airflow:** Uses XCom for passing SQL from Python task to SnowflakeOperator

Reference: `README.md` for setup overview, `requirements.txt` for dependencies.