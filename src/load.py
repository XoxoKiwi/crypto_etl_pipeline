import snowflake.connector, pandas as pd, os
from dotenv import load_dotenv

def load_data():
    load_dotenv()
    df = pd.read_csv("data/processed_crypto/cleaned_data.csv")
    
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'), password=os.getenv('SNOWFLAKE_PASS'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'), warehouse='COMPUTE_WH', 
        database='CRYPTO_DB', schema='PUBLIC'
    )
    cursor = conn.cursor()

    # 1. Create Stage (Transient saves money; exact match to CSV columns)
    cursor.execute("""
        CREATE OR REPLACE TRANSIENT TABLE COIN_DATA_STAGE (
            COIN_ID STRING, SYMBOL STRING, PRICE NUMBER(38,8),
            MARKET_CAP NUMBER(38,2), VOLUME NUMBER(38,2), TIME TIMESTAMP_NTZ
        )
    """)
    
    # 2. Fast Batch Insert
    cols = "INSERT INTO COIN_DATA_STAGE VALUES (%s, %s, %s, %s, %s, %s)"
    cursor.executemany(cols, [tuple(x) for x in df.to_numpy()])

    # 3. The "Smart" Merge (Matches on ID + Time to prevent duplicates)
    cursor.execute("""
        MERGE INTO COIN_DATA AS t USING COIN_DATA_STAGE AS s
        ON t.COIN_ID = s.COIN_ID AND t.TIME = s.TIME
        WHEN MATCHED THEN UPDATE SET t.MARKET_CAP = s.MARKET_CAP, t.VOLUME = s.VOLUME
        WHEN NOT MATCHED THEN INSERT (COIN_ID, SYMBOL, PRICE, MARKET_CAP, VOLUME, TIME)
        VALUES (s.COIN_ID, s.SYMBOL, s.PRICE, s.MARKET_CAP, s.VOLUME, s.TIME)
    """)
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Loaded {len(df)} rows.")

if __name__ == "__main__":
    load_data()