-- ========================================================
-- 1. COMPUTE LAYER: THE ENGINE
-- ========================================================
-- We use XSMALL to save credits. 
-- AUTO_SUSPEND = 60 ensures it turns off 1 min after the job finishes.
CREATE WAREHOUSE IF NOT EXISTS CRYPTO_WH 
WITH 
    WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE 
    COMMENT = 'Warehouse for Crypto ETL Pipeline - Cost Optimized';

-- ========================================================
-- 2. STORAGE LAYER: DATABASE & SCHEMA
-- ========================================================
CREATE DATABASE IF NOT EXISTS CRYPTO_DB;

USE DATABASE CRYPTO_DB;
USE SCHEMA PUBLIC;

-- ========================================================
-- 3. THE VAULT: MAIN DATA TABLE (TIME-SERIES)
-- ========================================================
-- This table stores every version of the price over time.
CREATE TABLE IF NOT EXISTS COIN_DATA (
    ID STRING,                   -- e.g., 'bitcoin'
    SYMBOL STRING,               -- e.g., 'BTC'
    CURRENT_PRICE NUMBER(38, 8), -- High precision for small coins
    MARKET_CAP NUMBER(38, 2),    
    TOTAL_VOLUME NUMBER(38, 2),  
    PRICE_CHANGE_24H FLOAT,      
    INGESTION_TIME TIMESTAMP_NTZ,
    
    -- COMPOSITE KEY: Prevents duplicate rows for the same coin at the same time.
    -- This is what allows your Window Functions (LAG, FIRST_VALUE) to work.
    CONSTRAINT pk_coin_time PRIMARY KEY (ID, INGESTION_TIME)
);

-- ========================================================
-- 4. THE QUARANTINE: DEAD LETTER TABLE
-- ========================================================
-- If data fails your Python quality checks, we move it here instead of deleting it.
CREATE TABLE IF NOT EXISTS COIN_DATA_ERRORS (
    ID STRING,
    SYMBOL STRING,
    CURRENT_PRICE FLOAT,
    ERROR_REASON STRING,         -- e.g., "Zero price detected"
    FAILED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ========================================================
-- 5. OBSERVABILITY: PIPELINE AUDIT LOGS
-- ========================================================
-- This table is updated by your logger_util.py script.
CREATE TABLE IF NOT EXISTS PIPELINE_LOGS (
    RUN_TIMESTAMP TIMESTAMP_NTZ,
    STATUS STRING,                -- 'SUCCESS' or 'FAILED'
    MESSAGE STRING,               -- Error details or summary
    ROWS_PROCESSED INTEGER
);

-- ========================================================
-- 6. VERIFICATION
-- ========================================================
SHOW TABLES;
DESC TABLE COIN_DATA;