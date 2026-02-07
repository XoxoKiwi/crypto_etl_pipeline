import snowflake.connector, pandas as pd, os
from dotenv import load_dotenv

def load_data():
    load_dotenv()
    # Read the data from your transformation step
    df = pd.read_csv("data/processed_crypto/cleaned_data.csv")
    
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'), password=os.getenv('SNOWFLAKE_PASS'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'), warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'), # Added a default just in case
        database=os.getenv('SNOWFLAKE_DATABASE', 'CRYPTO_DB'), schema=os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
    )
    cursor = conn.cursor()

    # 1. Create Stage (Must match the column names in your SQL setup script)
    cursor.execute("""
        CREATE OR REPLACE TRANSIENT TABLE COIN_DATA_STAGE (
            COIN_NAME STRING, SYMBOL STRING, CURRENT_PRICE NUMBER(38,8),
            MARKET_CAP NUMBER(38,2), TOTAL_VOLUME NUMBER(38,2), INGESTION_TIME TIMESTAMP_NTZ
        )
    """)
    
    # 2. Batch Insert (Order must match the Stage table above)
    cols = "INSERT INTO COIN_DATA_STAGE VALUES (%s, %s, %s, %s, %s, %s)"
    cursor.executemany(cols, [tuple(x) for x in df.to_numpy()])

    # 3. The "Sync-Matched" Merge (Using your exact database column names)
    # This prevents the 'invalid identifier' error by matching Snowflake.
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
    print(f"✅ Successfully Loaded {len(df)} rows.")

if __name__ == "__main__":
    load_data()