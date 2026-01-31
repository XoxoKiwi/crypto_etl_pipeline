import pandas as pd
from datetime import datetime
import os

def transform_data():
    print("🧹 Step 2: Validating Data Contracts...")
    
    try:
        df = pd.read_json("data/raw_crypto.json")
        
        # --- DATA CONTRACT: SCHEMA VALIDATION ---
        required_cols = ['id', 'symbol', 'current_price', 'market_cap']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"Data Contract Failed: Missing columns! Expected {required_cols}")

        # Quality Check: Price must be positive
        if df['current_price'].min() <= 0:
            raise ValueError("Data Quality Failed: Negative price found!")
            
        # Clean & Transform
        df_clean = df[required_cols].copy()
        df_clean.columns = ['coin_name', 'symbol', 'current_price', 'market_cap']
        df_clean['ingestion_time'] = datetime.now()
        
        os.makedirs('data/processed_crypto', exist_ok=True)
        df_clean.to_csv("data/processed_crypto/cleaned_data.csv", index=False)
        print("✅ Success! Data contract verified and cleaned.")
        
    except Exception as e:
        print(f"❌ Transformation Error: {e}")
        raise