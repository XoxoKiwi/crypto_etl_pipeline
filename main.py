import sys
from src.extract import extract_data
from src.transform import transform_data
from src.load import load_data
from src.logger_util import log_pipeline_run # New Import!

def run_pipeline():
    print("--- 🏁 STARTING CRYPTO ETL PIPELINE ---")
    
    try:
        # Step 1: Extract
        extract_data()
        
        # Step 2: Transform
        transform_data()
        
        # Step 3: Load
        load_data()
        
        # 🎉 SUCCESS LOGGING
        message = "Pipeline completed successfully."
        print(f"\n🏆 {message}")
        log_pipeline_run("SUCCESS", message, rows_processed=3)
        
    except Exception as e:
        # 🛑 FAILURE LOGGING
        error_msg = str(e)
        print(f"\n🛑 PIPELINE FAILED: {error_msg}")
        log_pipeline_run("FAILED", error_msg, rows_processed=0)
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()