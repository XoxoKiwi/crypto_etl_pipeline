import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv

def load_data():
    load_dotenv() # Reads the .env file
    print("🚀 Step 3: Loading data to Snowflake (Secure Mode)...")
    
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
        for _, row in df.iterrows():
            sql = "INSERT INTO COIN_DATA VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(sql, (row['coin_name'], row['symbol'], row['current_price'], row['market_cap'], row['ingestion_time']))
        
        conn.commit()
        print(f"✅ Success! {len(df)} rows loaded to Snowflake.")
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Loading Error: {e}")
        raise