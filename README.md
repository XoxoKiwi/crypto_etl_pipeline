🪙 Crypto-to-Snowflake ETL Pipeline:

A modular Python-based ETL pipeline that ingests real-time market data from the CoinGecko API into a Snowflake Cloud Data Warehouse.


🏗️ Architecture & Features:

- Orchestration: Managed by a central main.py to ensure sequential execution and error propagation.

- Security: Zero-trust credential management using .env and .gitignore to prevent secret leaks.

- Data Quality: Integrated "Gatekeeper" layer in transform.py to validate schemas and drop null values before ingestion.

- Resilience: Implemented raise_for_status() to handle API failures gracefully.


📂 Structure:

- src/: Decoupled logic for Extract, Transform, and Load.

- data/: Local staging for raw JSON and processed CSV (Git-ignored).

- .env: Encrypted-style local credential storage.


🚀 Execution:

- Configure Snowflake credentials in .env.

- Install dependencies: pip install -r requirements.txt.

- Run: python main.py
