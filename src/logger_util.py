import snowflake.connector
import os
from datetime import datetime
from dotenv import load_dotenv

def log_pipeline_run(status, message, rows_processed=0):
    load_dotenv()
    
    try:
        # 1. CONNECT (Using the same credentials as your Load script)
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASS'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse='CRYPTO_WH',
            database='CRYPTO_DB',
            schema='PUBLIC'
        )
        cursor = conn.cursor()

        # 2. ENSURE LOG TABLE EXISTS (The "Audit Trail")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PIPELINE_LOGS (
                run_timestamp TIMESTAMP_NTZ,
                status STRING,
                message STRING,
                rows_processed INTEGER
            )
        """)

        # 3. INSERT LOG (The "Heartbeat")
        sql = "INSERT INTO PIPELINE_LOGS VALUES (%s, %s, %s, %s)"
        cursor.execute(sql, (datetime.now(), status, message, rows_processed))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"📊 Snowflake Audit Updated: {status}")

    except Exception as e:
        # If the database is unreachable, we don't want the whole pipeline to crash
        print(f"⚠️ Snowflake Logging Failed: {e}")

if __name__ == "__main__":
    log_pipeline_run('SUCCESS', 'Manual test', 50)