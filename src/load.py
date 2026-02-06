import snowflake.connector
import pandas as pd
import os
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Loader")

def load_data():
    load_dotenv()
    logger.info("🚀 Starting Snowflake Load...")
    
    try:
        # 1. LOAD LOCAL DATA
        df = pd.read_csv("data/processed_crypto/cleaned_data.csv")
        
        # 2. CONNECT (Using your useful .env credentials)
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASS'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse='CRYPTO_WH', 
            database='CRYPTO_DB',
            schema='PUBLIC'
        )
        cursor = conn.cursor()

        # 3. CREATE STAGE (The "Lobby")
        # Transient = Cost Effective
        # UPDATED: Matching the 6 columns in your CSV
        cursor.execute("""
            CREATE OR REPLACE TRANSIENT TABLE COIN_DATA_STAGE (
                ID STRING, 
                SYMBOL STRING, 
                CURRENT_PRICE NUMBER(38,8),
                MARKET_CAP NUMBER(38,2), 
                TOTAL_VOLUME NUMBER(38,2),
                INGESTION_TIME TIMESTAMP_NTZ
            )
        """)
        
        # 4. BATCH INSERT (Security with %s)
        # UPDATED: Changed from 7 to 6 placeholders to match your CSV columns
        insert_sql = "INSERT INTO COIN_DATA_STAGE VALUES (%s, %s, %s, %s, %s, %s)"
        data_to_load = [tuple(x) for x in df.to_numpy()]
        cursor.executemany(insert_sql, data_to_load)

        # 5. THE MERGE (History-Tracking Logic)
        # UPDATED: Removed price_change_24h to align with your current pipeline output
        merge_sql = """
        MERGE INTO COIN_DATA AS target
        USING COIN_DATA_STAGE AS source
        ON target.ID = source.ID AND target.INGESTION_TIME = source.INGESTION_TIME
        WHEN NOT MATCHED THEN
            INSERT (ID, SYMBOL, CURRENT_PRICE, MARKET_CAP, TOTAL_VOLUME, INGESTION_TIME)
            VALUES (source.ID, source.SYMBOL, source.CURRENT_PRICE, source.MARKET_CAP, 
                    source.TOTAL_VOLUME, source.INGESTION_TIME);
        """
        cursor.execute(merge_sql)
        conn.commit()
        
        logger.info(f"✅ Success! {len(df)} rows loaded into History.")
        cursor.close()
        conn.close()
        return len(df) # Returning count for the main/airflow orchestrator
        
    except Exception as e:
        logger.error(f"❌ Loading Failed: {e}")
        raise

if __name__ == "__main__":
    load_data()