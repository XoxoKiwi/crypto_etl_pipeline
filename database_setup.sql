-- ========================================================
-- 1. COMPUTE LAYER: THE ENGINE
-- ========================================================
-- Using XSMALL to keep costs near zero for a personal project.
-- AUTO_SUSPEND = 60 ensures it turns off 1 minute after the DAG finishes.
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
-- 3. THE VAULT: PRODUCTION DATA TABLE (GOLD LAYER)
-- ========================================================
-- This table stores the final, clean, time-series data.
CREATE TABLE IF NOT EXISTS COIN_DATA (
    COIN_NAME STRING,            -- e.g., 'Bitcoin'
    SYMBOL STRING,               -- e.g., 'BTC'
    CURRENT_PRICE NUMBER(38, 8), 
    MARKET_CAP NUMBER(38, 2),    
    TOTAL_VOLUME NUMBER(38, 2),  
    INGESTION_TIME TIMESTAMP_NTZ,
    
    -- Composite Primary Key ensures data integrity for time-series analysis
    CONSTRAINT pk_coin_time PRIMARY KEY (COIN_NAME, INGESTION_TIME)
);

-- ========================================================
-- 4. THE STAGING AREA: (BRONZE/SILVER LAYER)
-- ========================================================
-- Transient tables do not have Fail-safe storage, saving on costs.
-- This is where the Python 'load.py' script dumps data before the MERGE.
CREATE OR REPLACE TRANSIENT TABLE COIN_DATA_STAGE (
    COIN_NAME STRING, 
    SYMBOL STRING, 
    CURRENT_PRICE NUMBER(38,8),
    MARKET_CAP NUMBER(38,2), 
    TOTAL_VOLUME NUMBER(38,2), 
    INGESTION_TIME TIMESTAMP_NTZ
);

-- ========================================================
-- 5. OBSERVABILITY: PIPELINE AUDIT LOGS
-- ========================================================
-- This table is fueled by your 'logger_util.py'
CREATE TABLE IF NOT EXISTS PIPELINE_LOGS (
    RUN_TIMESTAMP TIMESTAMP_NTZ,
    STATUS STRING,                -- 'SUCCESS' or 'FAILED'
    MESSAGE STRING,               -- Error details or 'Batch complete'
    ROWS_PROCESSED INTEGER
);

-- Quick Check: Verify the setup
SHOW WAREHOUSES LIKE 'CRYPTO_WH';
DESC TABLE COIN_DATA;