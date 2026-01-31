import snowflake.connector
import os
from datetime import datetime
from dotenv import load_dotenv

def log_pipeline_run(status, message, rows_processed=0):
    load_dotenv()
    
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASS'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse='COMPUTE_WH',
            database='CRYPTO_DB',
            schema='PUBLIC'
        )
        cursor = conn.cursor()

        # Create the log table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PIPELINE_LOGS (
                run_timestamp TIMESTAMP_NTZ,
                status STRING,
                message STRING,
                rows_processed INTEGER
            )
        """)

        # Insert the log entry
        sql = "INSERT INTO PIPELINE_LOGS VALUES (%s, %s, %s, %s)"
        cursor.execute(sql, (datetime.now(), status, message, rows_processed))
        
        conn.commit()
        cursor.close()
        conn.close()
        print(f"📊 Pipeline Log Updated: {status}")

    except Exception as e:
        print(f"⚠️ Logging Failed: {e}")