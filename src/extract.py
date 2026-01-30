import requests
import json
import os

def extract_data():
    print("🚀 Step 1: Extracting data from CoinGecko API...")
    url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=3&page=1"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status() # This is the "Pro" error check
        
        data = response.json()
        os.makedirs('data', exist_ok=True)
        with open('data/raw_crypto.json', 'w') as f:
            json.dump(data, f)
        print("✅ Success! Raw data saved.")
        
    except Exception as e:
        print(f"❌ Extraction Error: {e}")
        raise # Tells the 'brain' (main.py) to stop everything