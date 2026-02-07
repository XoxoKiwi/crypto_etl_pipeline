import requests
import json
import os
import logging
from datetime import datetime, timezone # Line 5: Added to handle UTC time

# Simple setup: easiest way to see what's happening
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Extractor")

def extract_data():
    logger.info("🚀 Starting Extraction...")
    url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=50&page=1"
    
    try:
        # 1. FETCH (Using a simple session)
        session = requests.Session()
        response = session.get(url, timeout=10)
        response.raise_for_status() 
        data = response.json()
        
        # 2. VALIDATE (The "Volume Guardrail" Feature)
        if not isinstance(data, list) or len(data) < 10:
            raise ValueError(f"Data Quality Failed: Received only {len(data) if isinstance(data, list) else 'nothing'}")

        # 3. ADD TIMESTAMP (Fixing the -478 Error)
        # We loop through each coin and add the exact UTC time right now.
        # This ensures every coin has a "birth time" before it hits Snowflake.
        extraction_time = datetime.now(timezone.utc).isoformat()
        for coin in data:
            coin['captured_at'] = extraction_time # Line 32: Adding time to each coin

        # 4. SAVE (The "Landing Zone")
        os.makedirs('data', exist_ok=True)
        with open('data/raw_crypto.json', 'w') as f:
            json.dump(data, f) # Saving as a simple list, just like before
            
        logger.info(f"✅ Success! {len(data)} coins saved with names and timestamps.")
        return data
        
    except Exception as e:
        logger.error(f"❌ Extraction Failed: {e}")
        raise # Critical: This tells Airflow the task failed!

if __name__ == "__main__":
    extract_data()