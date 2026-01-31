import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv

def load_data():
    load_dotenv()
    print("🚀 Step 3: Loading to Snowflake (Idempotent MERGE Mode)...")
    
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

        # CREATE STAGING TABLE (Best Practice)
        cursor.execute("CREATE OR REPLACE TEMPORARY TABLE TEMP_COIN_STAGE LIKE COIN_DATA")
        
        # Load data into Temp Table
        for _, row in df.iterrows():
            cursor.execute(
                "INSERT INTO TEMP_COIN_STAGE VALUES (%s, %s, %s, %s, %s)",
                (row['coin_name'], row['symbol'], row['current_price'], row['market_cap'], row['ingestion_time'])
            )

        # THE "MERGE" (Idempotency Logic)
        # Updates existing coins or inserts new ones based on the symbol
        merge_sql = """
        MERGE INTO COIN_DATA AS target
        USING TEMP_COIN_STAGE AS source
        ON target.symbol = source.symbol AND target.ingestion_time = source.ingestion_time
        WHEN NOT MATCHED THEN
            INSERT (coin_name, symbol, current_price, market_cap, ingestion_time)
            VALUES (source.coin_name, source.symbol, source.current_price, source.market_cap, source.ingestion_time);
        """
        cursor.execute(merge_sql)
        
        conn.commit()
        print(f"✅ Success! Idempotent load complete.")
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Loading Error: {e}")
        raise