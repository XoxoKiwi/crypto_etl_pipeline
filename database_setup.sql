-- 1. Create the project database
CREATE DATABASE IF NOT EXISTS CRYPTO_DB;

-- 2. Switch to the public schema
USE SCHEMA CRYPTO_DB.PUBLIC;

-- 3. Create the main data table
-- Using IF NOT EXISTS ensures your historical data is safe!
CREATE TABLE IF NOT EXISTS COIN_DATA (
    coin_name STRING,
    symbol STRING,
    current_price FLOAT,
    market_cap FLOAT,
    ingestion_time TIMESTAMP_NTZ
);

-- 4. Create the log table for Observability
-- This is where logger_util.py will record every run's status
CREATE TABLE IF NOT EXISTS PIPELINE_LOGS (
    run_timestamp TIMESTAMP_NTZ,
    status STRING,
    message STRING,
    rows_processed INTEGER
);