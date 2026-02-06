import requests
import json
import os
import logging

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
        # We check if it's a list AND if we got enough coins to be useful
        if not isinstance(data, list) or len(data) < 10:
            raise ValueError(f"Data Quality Failed: Received only {len(data) if isinstance(data, list) else 'nothing'}")

        # 3. SAVE (The "Landing Zone")
        os.makedirs('data', exist_ok=True)
        with open('data/raw_crypto.json', 'w') as f:
            json.dump(data, f)
            
        logger.info(f"✅ Success! {len(data)} coins saved to raw_crypto.json")
        return data
        
    except Exception as e:
        logger.error(f"❌ Extraction Failed: {e}")
        raise # Critical: This tells Airflow the task failed!

if __name__ == "__main__":
    extract_data()