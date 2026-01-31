import requests
import json
import os
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

def extract_data():
    print("🚀 Step 1: Extracting data with Exponential Backoff...")
    url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=3&page=1"
    
    # SETUP RETRY STRATEGY (Exponential Backoff)
    retry_strategy = Retry(
        total=3, # Try 3 times
        backoff_factor=2, # Wait 2s, 4s, 8s between retries
        status_forcelist=[429, 500, 502, 503, 504] # Retry on these specific errors
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)

    try:
        response = session.get(url, timeout=10)
        response.raise_for_status() 
        
        data = response.json()
        os.makedirs('data', exist_ok=True)
        with open('data/raw_crypto.json', 'w') as f:
            json.dump(data, f)
        print("✅ Success! Raw data saved.")
        
    except Exception as e:
        print(f"❌ Extraction Error: {e}")
        raise