🪙 Modular Crypto-to-Snowflake ETL Pipeline

- Scheduled Batch Orchestration & Market Intelligence

This project demonstrates a high-quality Data Engineering solution for orchestrating cryptocurrency market data into Snowflake using Apache Airflow. It is built with a focus on idempotency, modular service architecture, and comprehensive auditability.


🏗️ Engineering Design Patterns

- Orchestration Layer Automated batch execution is managed via Apache Airflow (Docker-hosted), utilizing a PythonOperator with catchup=False to ensure deterministic scheduling and optimized resource consumption.

- Modular Service Architecture The ETL logic is decoupled into dedicated extract, transform, and load modules. This separation of concerns facilitates isolated Integration Testing and long-term code maintainability.

- Idempotent Load Strategy To ensure data reliability, the pipeline implements the Snowflake MERGE pattern. This "upsert" logic prevents record duplication by matching on a composite key of Asset Name + Captured At (UTC).

- Resiliency & Retries Built-in Airflow Retry Policies (2 retries, 5-minute delay) handle transient API rate limits or database connectivity events without manual intervention.


🧠 Data Model & Intelligence

- The pipeline enforces a strict 6-column data contract: asset, price_usd, captured_at_utc, momentum_pct, total_growth_pct, and last_updated_utc.

- CRYPTO_PRICES (Source of Truth) Standardized via UTC Normalization to ensure global timestamp consistency for multi-region financial analysis.

- ASSET_METRICS (Analytical Layer) Calculates Run-Gap Momentum using SQL LAG() functions. This logic is relative to the last available observation, making the metrics resilient to schedule gaps or system downtime.

- PIPELINE_LOGS (Audit Layer) A centralized logging system in Snowflake that captures STATUS, ROWS_PROCESSED, and full error tracebacks for every batch execution.


📂 Repository Structure

- dags/: Contains the Airflow DAG definition and task dependency graphs.

- src/: The core ETL engine including API ingestion (Pandas), transformation logic, and Snowflake loading.

- database_setup.sql: Data Definition Language (DDL) for the persistent warehouse layer and audit tables.

- analysis_queries.sql: Advanced SQL logic used to derive momentum and asset growth KPIs.

- .gitignore: Strictly enforced to prevent the leakage of .env credentials and local session files.


🛡️ Operational Standards

- Observability Every pipeline run is indexed in Snowflake for rapid debugging and health monitoring.

- Secrets Management Zero-leakage policy via environment-based credential handling (.env).

- Data Integrity Pre-load validation checks ensure that only records matching the 6-column contract reach the production warehouse.