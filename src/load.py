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
        df = pd.read_csv("data/processed_crypto/cleaned_data.csv")
        
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASS'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse='COMPUTE_WH', 
            database='CRYPTO_DB',
            schema='PUBLIC'
        )
        cursor = conn.cursor()

        # Step 1: Create a staging table with the new COIN_NAME column
        cursor.execute("""
            CREATE OR REPLACE TRANSIENT TABLE COIN_DATA_STAGE (
                COIN_NAME STRING, 
                SYMBOL STRING, 
                CURRENT_PRICE NUMBER(38,8),
                MARKET_CAP NUMBER(38,2), 
                TOTAL_VOLUME NUMBER(38,2),
                INGESTION_TIME TIMESTAMP_NTZ
            )
        """)
        
        # Step 2: Batch Insert the 6 columns from your CSV
        insert_sql = "INSERT INTO COIN_DATA_STAGE VALUES (%s, %s, %s, %s, %s, %s)"
        data_to_load = [tuple(x) for x in df.to_numpy()]
        cursor.executemany(insert_sql, data_to_load)

        # Step 3: Merge into the main table (Matches on Name + Time)
        merge_sql = """
        MERGE INTO COIN_DATA AS target
        USING COIN_DATA_STAGE AS source
        ON target.COIN_NAME = source.COIN_NAME AND target.INGESTION_TIME = source.INGESTION_TIME
        WHEN NOT MATCHED THEN
            INSERT (COIN_NAME, SYMBOL, CURRENT_PRICE, MARKET_CAP, TOTAL_VOLUME, INGESTION_TIME)
            VALUES (source.COIN_NAME, source.SYMBOL, source.CURRENT_PRICE, source.MARKET_CAP, 
                    source.TOTAL_VOLUME, source.INGESTION_TIME);
        """
        cursor.execute(merge_sql)
        conn.commit()
        
        logger.info(f"✅ Success! {len(df)} rows loaded into Snowflake.")
        cursor.close()
        conn.close()
        return len(df)
        
    except Exception as e:
        logger.error(f"❌ Loading Failed: {e}")
        raise