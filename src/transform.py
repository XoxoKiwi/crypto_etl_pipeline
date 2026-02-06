import pandas as pd
from datetime import datetime
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Transformer")

def transform_data():
    logger.info("🧹 Starting Transformation...")
    
    try:
        # 1. LOAD
        if not os.path.exists("data/raw_crypto.json"):
            raise FileNotFoundError("Raw data file missing!")
            
        df = pd.read_json("data/raw_crypto.json")
        
        # 2. DATA CONTRACT (Essential Feature)
        # We focus on the 5 core columns from the API + 1 timestamp we add later
        required_cols = ['id', 'symbol', 'current_price', 'market_cap', 'total_volume']
        if not all(col in df.columns for col in required_cols):
            raise ValueError("Schema Check Failed: API response is missing required columns.")

        # 3. CLEANING & QUALITY
        # Filter out bad data (Negative prices)
        df = df[df['current_price'] > 0]
        
        # Fill missing values so math doesn't break
        df = df.fillna(0)

        # 4. STANDARDIZATION
        # Selecting only the 5 core columns for the transformation
        df_clean = df[required_cols].copy()
        df_clean.columns = ['coin_name', 'symbol', 'current_price', 'market_cap', 'total_volume']
        
        # Standardize symbols to uppercase for clean Snowflake joins
        df_clean['symbol'] = df_clean['symbol'].str.upper()
        
        # Add the 'Batch Timestamp' for Time-Series Analysis (Column #6)
        # This is the "Key" to your growth percentage calculations!
        df_clean['ingestion_time'] = datetime.now()
        
        # 5. SAVE
        os.makedirs('data/processed_crypto', exist_ok=True)
        # Saves exactly 6 columns to the CSV
        df_clean.to_csv("data/processed_crypto/cleaned_data.csv", index=False)
        
        logger.info(f"✅ Success! {len(df_clean)} rows ready for Snowflake.")
        return len(df_clean) # Return count for the orchestrator
        
    except Exception as e:
        logger.error(f"❌ Transformation Error: {e}")
        raise

if __name__ == "__main__":
    transform_data()