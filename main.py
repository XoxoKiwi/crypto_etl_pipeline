import sys
from src.extract import extract_data
from src.transform import transform_data
from src.load import load_data
from src.logger_util import log_pipeline_run

def run_pipeline():
    print("🚀 Local Pipeline Run Initiated...")
    
    try:
        # Step 1: EXTRACT
        extract_data()
        
        # Step 2: TRANSFORM
        # We assume 50 rows for the manual log, or you can pass the actual count
        transform_data()
        
        # Step 3: LOAD
        load_data()
        
        # SUCCESS
        success_msg = "Manual Batch Completed Successfully."
        log_pipeline_run("SUCCESS", success_msg, rows_processed=50)
        print(f"✅ {success_msg}")
        
    except Exception as e:
        # FAILURE
        error_msg = f"Local Run Error: {str(e)}"
        print(f"❌ {error_msg}")
        log_pipeline_run("FAILED", error_msg, rows_processed=0)
        sys.exit(1) # Tells the terminal the run failed

if __name__ == "__main__":
    run_pipeline()