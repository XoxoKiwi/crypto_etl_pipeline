import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv

def load_data():
    load_dotenv()
    
    if not os.path.exists("data/processed_crypto/cleaned_data.csv"):
        raise FileNotFoundError("Cleaned CSV not found for loading!")

    df = pd.read_csv("data/processed_crypto/cleaned_data.csv")
    
    # Fully dynamic connection
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASS'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    cursor = conn.cursor()

    # 1. Staging (Transient = Cost Effective)
    cursor.execute("""
        CREATE OR REPLACE TRANSIENT TABLE COIN_DATA_STAGE (
            COIN_NAME STRING, SYMBOL STRING, CURRENT_PRICE NUMBER(38,8),
            MARKET_CAP NUMBER(38,2), TOTAL_VOLUME NUMBER(38,2), INGESTION_TIME TIMESTAMP_NTZ
        )
    """)
    
    # 2. Batch Ingest
    cols = "INSERT INTO COIN_DATA_STAGE VALUES (%s, %s, %s, %s, %s, %s)"
    cursor.executemany(cols, [tuple(x) for x in df.to_numpy()])

    # 3. The Idempotent Merge
    cursor.execute("""
        MERGE INTO COIN_DATA AS t USING COIN_DATA_STAGE AS s
        ON t.COIN_NAME = s.COIN_NAME AND t.INGESTION_TIME = s.INGESTION_TIME
        WHEN MATCHED THEN UPDATE SET 
            t.MARKET_CAP = s.MARKET_CAP, 
            t.TOTAL_VOLUME = s.TOTAL_VOLUME
        WHEN NOT MATCHED THEN INSERT (COIN_NAME, SYMBOL, CURRENT_PRICE, MARKET_CAP, TOTAL_VOLUME, INGESTION_TIME)
        VALUES (s.COIN_NAME, s.SYMBOL, s.CURRENT_PRICE, s.MARKET_CAP, s.TOTAL_VOLUME, s.INGESTION_TIME)
    """)
    
    conn.commit()
    cursor.close()
    conn.close()
    return len(df)