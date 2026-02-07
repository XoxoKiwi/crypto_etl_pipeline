import snowflake.connector
import os
from datetime import datetime
from dotenv import load_dotenv

def log_pipeline_run(status, message, rows_processed=0):
    load_dotenv()
    
    try:
        # Using os.getenv with defaults to ensure stability
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASS'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'CRYPTO_WH'),
            database=os.getenv('SNOWFLAKE_DATABASE', 'CRYPTO_DB'),
            schema=os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
        )
        cursor = conn.cursor()

        # Insert Log using the schema defined in your SQL setup
        sql = "INSERT INTO PIPELINE_LOGS (RUN_TIMESTAMP, STATUS, MESSAGE, ROWS_PROCESSED) VALUES (%s, %s, %s, %s)"
        cursor.execute(sql, (datetime.now(), status, message, rows_processed))
        
        conn.commit()
        cursor.close()
        conn.close()
        print(f"📊 Snowflake Audit Updated: {status}")

    except Exception as e:
        # Non-critical failure: don't crash the ETL if logging fails
        print(f"⚠️ Snowflake Logging Failed: {e}")