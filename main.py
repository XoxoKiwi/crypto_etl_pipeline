import sys
from src.extract import extract_data
from src.transform import transform_data
from src.load import load_data

def run_pipeline():
    print("--- 🏁 STARTING CRYPTO ETL PIPELINE ---")
    
    try:
        # Step 1: Extract
        extract_data()
        
        # Step 2: Transform
        transform_data()
        
        # Step 3: Load
        load_data()
        
        print("\n🏆 SUCCESS: All steps completed. Check Snowflake!")
        
    except Exception as e:
        print(f"\n🛑 PIPELINE FAILED: {e}")
        sys.exit(1) # Stops the script properly

if __name__ == "__main__":
    run_pipeline()