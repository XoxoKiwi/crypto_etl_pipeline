-- Check the latest data ingested
SELECT * FROM CRYPTO_DB.PUBLIC.COIN_DATA 
ORDER BY ingestion_time DESC;

-- Check pipeline health and logs
SELECT * FROM CRYPTO_DB.PUBLIC.PIPELINE_LOGS 
ORDER BY run_timestamp DESC;

-- Business Question: What is the total market cap of our tracked coins?
SELECT SUM(market_cap) as total_market_value 
FROM CRYPTO_DB.PUBLIC.COIN_DATA;

-- DASHBOARD LOGIC (Gold Layer)
-- This query aggregates data for the time-series chart
-- to visualize price trends over time.
SELECT 
    COIN_NAME, 
    CURRENT_PRICE, 
    INGESTION_TIME 
FROM CRYPTO_DB.PUBLIC.COIN_DATA
ORDER BY INGESTION_TIME ASC;