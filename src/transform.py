import pandas as pd
from datetime import datetime
import os

def transform_data():
    print("🧹 Step 2: Transforming & Checking Data Quality...")
    
    try:
        df = pd.read_json("data/raw_crypto.json")
        
        # --- IMPROVED DATA QUALITY CHECKS ---
        # 1. Check for negative prices
        if df['current_price'].min() <= 0:
            raise ValueError("Data Quality Failed: Negative price found!")
            
        # 2. Instead of failing on empty values, let's just remove them!
        initial_count = len(df)
        df = df.dropna(subset=['current_price', 'market_cap'])
        final_count = len(df)
        
        if final_count < initial_count:
            print(f"⚠️ Warning: Dropped {initial_count - final_count} rows with missing values.")
        
        # Transformation logic
        df_clean = df[['id', 'symbol', 'current_price', 'market_cap']].copy()
        df_clean.columns = ['coin_name', 'symbol', 'current_price', 'market_cap']
        df_clean['ingestion_time'] = datetime.now()
        
        os.makedirs('data/processed_crypto', exist_ok=True)
        df_clean.to_csv("data/processed_crypto/cleaned_data.csv", index=False)
        print("✅ Success! Data is clean and ready.")
        
    except Exception as e:
        print(f"❌ Transformation Error: {e}")
        raise