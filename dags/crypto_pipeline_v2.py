from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import logging

# 1. PATH SETUP (Ensuring Airflow sees your scripts)
dag_path = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.join(os.path.dirname(dag_path), 'src')
sys.path.append(src_path)

# 2. MODULAR IMPORTS
from extract import extract_data
from transform import transform_data
from load import load_data
from logger_util import log_pipeline_run

logger = logging.getLogger("airflow.task")

# 3. THE ORCHESTRATION LOGIC
def run_modular_etl():
    try:
        logger.info("🚀 Batch Started...")
        
        extract_data()
        row_count = transform_data()
        load_data()
        
        # Log success to Snowflake
        log_pipeline_run(status='SUCCESS', message='Batch complete', rows_processed=row_count)
        
    except Exception as e:
        # Log failure to Snowflake so you see it in your analysis queries
        error_msg = f"Pipeline Failed: {str(e)}"
        log_pipeline_run(status='FAILED', message=error_msg)
        raise # Critical: This makes the Airflow UI turn RED so you know to fix it

# 4. DAG CONFIGURATION
default_args = {
    'owner': 'heena',
    'retries': 2, 
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'crypto_modular_pipeline_v2',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=datetime(2026, 2, 1),
    catchup=False  # Saves Snowflake credits and PC memory
) as dag:

    execute_pipeline = PythonOperator(
        task_id='execute_etl_flow',
        python_callable=run_modular_etl
    )