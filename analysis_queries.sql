--What it tells you:-
--Success/Fail status: A permanent record of every run.
--Row Counts: Proof that 50 rows were moved.
SELECT * FROM PIPELINE_LOGS ORDER BY RUN_TIMESTAMP DESC;

-- 1. THE HEARTBEAT (Fixes the -478 Error)
-- TO_TIMESTAMP_NTZ ensures we compare the API time to your current time fairly.
SELECT 
    MAX(INGESTION_TIME) as LAST_REFRESH_UTC,
    DATEDIFF('minute', MAX(INGESTION_TIME), SYSDATE()) as MINUTES_AGO
FROM COIN_DATA;

-- 2. THE ALPHA SCANNER (The Decision Table)
WITH market_stats AS (
    SELECT 
        COIN_NAME, -- Full name (e.g., Bitcoin)
        SYMBOL,
        CURRENT_PRICE,
        -- LAG: Looks at the previous row to find the old price.
        LAG(CURRENT_PRICE) OVER (PARTITION BY SYMBOL ORDER BY INGESTION_TIME ASC) as prev_price,
        -- FIRST_VALUE: Looks at the very first row ever recorded.
        FIRST_VALUE(CURRENT_PRICE) OVER (PARTITION BY SYMBOL ORDER BY INGESTION_TIME ASC) as start_price,
        INGESTION_TIME
    FROM COIN_DATA
)
SELECT 
    COIN_NAME,
    SYMBOL,
    CURRENT_PRICE AS PRICE_USD,
    -- NULLIF prevents the "Division by Zero" crash if a price is missing.
    ROUND(((CURRENT_PRICE - prev_price) / NULLIF(prev_price, 0)) * 100, 2) AS MOMENTUM_PCT,
    ROUND(((CURRENT_PRICE - start_price) / NULLIF(start_price, 0)) * 100, 2) AS TOTAL_GROWTH_PCT,
    INGESTION_TIME AS EVENT_TIME_UTC
FROM market_stats
QUALIFY ROW_NUMBER() OVER (PARTITION BY SYMBOL ORDER BY INGESTION_TIME DESC) = 1
ORDER BY MOMENTUM_PCT DESC;

-- 3. IDEMPOTENCY AUDIT (Proves No Duplicates)
SELECT COIN_NAME, INGESTION_TIME, COUNT(*)
FROM COIN_DATA
GROUP BY 1, 2
HAVING COUNT(*) > 1;



SELECT COUNT(*) FROM COIN_DATA;

SELECT 
    INGESTION_TIME, 
    COUNT(*) as rows_in_batch
FROM COIN_DATA
GROUP BY 1
ORDER BY 1 DESC;


