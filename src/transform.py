import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Transformer")

def transform_data():
    try:
        if not os.path.exists("data/raw_crypto.json"):
            raise FileNotFoundError("Raw data file missing!")
            
        df = pd.read_json("data/raw_crypto.json")
        
        # We now track 6 columns (adding name and captured_at)
        required_cols = ['name', 'symbol', 'current_price', 'market_cap', 'total_volume', 'captured_at']
        
        # Quality check and cleaning
        #df = df[df['current_price'] > 0] # Step 1: Filter out bad prices
        #df = df.fillna(0)                # Step 2: Fix any empty holes
        df = df[df['current_price'] > 0].fillna(0)

        # Standardizing names and symbols
        df_clean = df[required_cols].copy()
        df_clean.columns = ['coin_name', 'symbol', 'current_price', 'market_cap', 'total_volume', 'ingestion_time']
        df_clean['symbol'] = df_clean['symbol'].str.upper()
        
        os.makedirs('data/processed_crypto', exist_ok=True)
        df_clean.to_csv("data/processed_crypto/cleaned_data.csv", index=False)
        
        return len(df_clean)
        
    except Exception as e:
        logger.error(f"❌ Transformation Error: {e}")
        raise