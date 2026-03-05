
-- ============================================================
-- Weather Analytics — Snowflake SQL
-- Database: USER_DB_FERRET 
-- Role: TRAINING_ROLE
-- ============================================================

USE ROLE TRAINING_ROLE;
USE WAREHOUSE FERRET_QUERY_WH;
USE DATABASE USER_DB_FERRET;
USE SCHEMA RAW;

-- ============================================================
-- SECTION 1 — SETUP
-- ============================================================

-- Create a separate schema to hold ML views and forecast tables
CREATE SCHEMA IF NOT EXISTS ADHOC;


-- Main weather history table — one row per city per day
CREATE OR REPLACE TABLE CITY_WEATHER (
    CITY STRING,
    LATITUDE FLOAT,
    LONGITUDE FLOAT,
    DATE DATE,
    TEMP_MAX FLOAT,
    TEMP_MIN FLOAT,
    TEMP_MEAN FLOAT,
    PRECIPITATION_MM FLOAT,
    WIND_SPEED_MAX_KMH FLOAT,
    WEATHER_CODE INTEGER,
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (CITY, DATE)
);


CREATE OR REPLACE SNOWFLAKE.ML.FORECAST test_model(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'RAW.CITY_WEATHER'),
    TIMESTAMP_COLNAME => 'DATE',
    TARGET_COLNAME => 'TEMP_MAX'
);

CALL test_model!SHOW_EVALUATION_METRICS();

CALL test_model!FORECAST(
    FORECASTING_PERIODS => 7,
    CONFIG_OBJECT => {'prediction_interval': 0.95}
);

-- ============================================================
-- SECTION 2 — DATA VALIDATION
-- ============================================================

-- Confirm table columns and data types loaded correctly
DESC TABLE RAW.CITY_WEATHER;


-- Check how many total rows exist in the history table
SELECT COUNT(*) FROM RAW.CITY_WEATHER;


-- Count how many days of data exist per city
SELECT
    CITY,
    MIN(DATE)  AS first_date,
    MAX(DATE)  AS last_date,
    COUNT(*)   AS total_records
FROM RAW.CITY_WEATHER
GROUP BY CITY;


-- Check for duplicate records — any result here means a data quality issue
SELECT CITY, DATE, COUNT(*)
FROM RAW.CITY_WEATHER
GROUP BY CITY, DATE
HAVING COUNT(*) > 1;

-- Show the 10 most recently loaded rows to confirm the pipeline ran
SELECT CITY, DATE, LOAD_TS
FROM RAW.CITY_WEATHER
ORDER BY LOAD_TS DESC
LIMIT 10;

-- Show rows loaded after a specific date to verify a particular pipeline run
SELECT * FROM RAW.CITY_WEATHER
WHERE LOAD_TS > '2026-03-04'
ORDER BY CITY, DATE DESC;

-- ============================================================
-- SECTION 3 — EXPLORATORY QUERIES
-- ============================================================

-- View the last 5 days of weather for a specific city
SELECT
    CITY, DATE, TEMP_MAX, TEMP_MIN, TEMP_MEAN,
    PRECIPITATION_MM, WIND_SPEED_MAX_KMH, WEATHER_CODE
FROM RAW.CITY_WEATHER
WHERE CITY = 'Newport Beach'
ORDER BY DATE DESC
LIMIT 5;


-- Translate numeric WMO weather codes into plain English descriptions
SELECT *,
    CASE WEATHER_CODE
        WHEN 0  THEN 'Clear sky'
        WHEN 1  THEN 'Mainly clear'
        WHEN 2  THEN 'Partly cloudy'
        WHEN 3  THEN 'Overcast'
        WHEN 45 THEN 'Fog'
        WHEN 48 THEN 'Rime fog'
        WHEN 51 THEN 'Light drizzle'
        WHEN 53 THEN 'Moderate drizzle'
        WHEN 55 THEN 'Dense drizzle'
        WHEN 61 THEN 'Slight rain'
        WHEN 63 THEN 'Moderate rain'
        WHEN 65 THEN 'Heavy rain'
        WHEN 71 THEN 'Slight snow'
        WHEN 73 THEN 'Moderate snow'
        WHEN 75 THEN 'Heavy snow'
        WHEN 80 THEN 'Rain showers'
        WHEN 81 THEN 'Moderate rain showers'
        WHEN 82 THEN 'Violent rain showers'
        WHEN 95 THEN 'Thunderstorm'
        WHEN 96 THEN 'Thunderstorm with hail'
        WHEN 99 THEN 'Severe thunderstorm'
        ELSE 'Unknown'
    END AS WEATHER_DESCRIPTION
FROM RAW.CITY_WEATHER;



-- ============================================================
-- SECTION 4 — ADVANCED ANALYTICS
-- ============================================================

-- Compute 7-day and 14-day rolling average temperatures to smooth daily noise
SELECT
    CITY,
    DATE,
    TEMP_MAX,
    ROUND(AVG(TEMP_MAX) OVER (
        PARTITION BY CITY ORDER BY DATE
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_7day_avg,
    ROUND(AVG(TEMP_MAX) OVER (
        PARTITION BY CITY ORDER BY DATE
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_14day_avg,
    ROUND(TEMP_MAX - AVG(TEMP_MAX) OVER (
        PARTITION BY CITY ORDER BY DATE
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS deviation_from_7day_trend
FROM RAW.CITY_WEATHER
ORDER BY CITY, DATE;




-- Flag days where temperature deviated more than 2 standard deviations from the city mean
SELECT
    CITY,
    DATE,
    TEMP_MAX,
    ROUND(AVG(TEMP_MAX)    OVER (PARTITION BY CITY), 2) AS city_avg,
    ROUND(STDDEV(TEMP_MAX) OVER (PARTITION BY CITY), 2) AS city_stddev,
    ROUND(TEMP_MAX - AVG(TEMP_MAX) OVER (PARTITION BY CITY), 2) AS deviation,
    CASE
        WHEN TEMP_MAX > AVG(TEMP_MAX) OVER (PARTITION BY CITY)
                      + 2 * STDDEV(TEMP_MAX) OVER (PARTITION BY CITY)
             THEN 'Heat Anomaly'
        WHEN TEMP_MAX < AVG(TEMP_MAX) OVER (PARTITION BY CITY)
                      - 2 * STDDEV(TEMP_MAX) OVER (PARTITION BY CITY)
             THEN 'Cold Anomaly'
        ELSE 'Normal'
    END AS anomaly_flag
FROM RAW.CITY_WEATHER;


-- Assign each day a temperature percentile and quartile rank within its city
SELECT
    CITY,
    DATE,
    TEMP_MAX,
    TEMP_MIN,
    ROUND(PERCENT_RANK() OVER (PARTITION BY CITY ORDER BY TEMP_MAX), 3) AS temp_percentile,
    NTILE(4)             OVER (PARTITION BY CITY ORDER BY TEMP_MAX)     AS temp_quartile,
    CASE NTILE(4) OVER (PARTITION BY CITY ORDER BY TEMP_MAX)
        WHEN 1 THEN 'Cold Quarter'
        WHEN 2 THEN 'Below Average'
        WHEN 3 THEN 'Above Average'
        WHEN 4 THEN 'Hot Quarter'
    END AS temp_category
FROM RAW.CITY_WEATHER
ORDER BY CITY, DATE;


-- Find the longest consecutive rainy day streaks per city using island-gap logic
WITH daily_rain AS (
    SELECT
        CITY,
        DATE,
        PRECIPITATION_MM,
        CASE WHEN PRECIPITATION_MM > 0 THEN 1 ELSE 0 END AS is_rainy,
        ROW_NUMBER() OVER (PARTITION BY CITY ORDER BY DATE) -
        SUM(CASE WHEN PRECIPITATION_MM > 0 THEN 1 ELSE 0 END)
            OVER (PARTITION BY CITY ORDER BY DATE
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
    FROM RAW.CITY_WEATHER
),
streaks AS (
    SELECT
        CITY,
        MIN(DATE)                       AS streak_start,
        MAX(DATE)                       AS streak_end,
        COUNT(*)                        AS streak_length_days,
        ROUND(SUM(PRECIPITATION_MM), 1) AS total_rain_mm
    FROM daily_rain
    WHERE is_rainy = 1
    GROUP BY CITY, grp
)
SELECT * FROM streaks
ORDER BY streak_length_days DESC
LIMIT 10;


-- Compare daily max temperatures across all four cities side by side
SELECT
    a.DATE,
    a.TEMP_MAX AS miami_temp,
    b.TEMP_MAX AS newport_temp,
    c.TEMP_MAX AS seattle_temp,
    d.TEMP_MAX AS boston_temp,
    ROUND(a.TEMP_MAX - b.TEMP_MAX, 1) AS miami_vs_newport,
    ROUND(a.TEMP_MAX - d.TEMP_MAX, 1) AS miami_vs_boston,
    ROUND(c.TEMP_MAX - d.TEMP_MAX, 1) AS seattle_vs_boston
FROM RAW.CITY_WEATHER a
JOIN RAW.CITY_WEATHER b ON a.DATE = b.DATE AND b.CITY = 'Newport Beach'
JOIN RAW.CITY_WEATHER c ON a.DATE = c.DATE AND c.CITY = 'Seattle'
JOIN RAW.CITY_WEATHER d ON a.DATE = d.DATE AND d.CITY = 'Boston'
WHERE a.CITY = 'Miami'
ORDER BY a.DATE DESC;



-- ============================================================
-- SECTION 5 — ML FORECAST RESULTS
-- ============================================================

-- View the clean training data the ML model was trained on
SELECT * FROM ADHOC.CITY_WEATHER_TRAIN_VIEW
ORDER BY DATE DESC;

-- View raw 7-day forecast output straight from the ML model
SELECT * FROM ADHOC.CITY_WEATHER_FORECAST;

-- Show forecast uncertainty — wider bands mean the model is less confident on that day
SELECT
    REPLACE(SERIES, '"', '')            AS CITY,
    TS                                  AS FORECAST_DATE,
    ROUND(FORECAST, 2)                  AS predicted_temp,
    ROUND(LOWER_BOUND, 2)               AS lower_95,
    ROUND(UPPER_BOUND, 2)               AS upper_95,
    ROUND(UPPER_BOUND - LOWER_BOUND, 2) AS confidence_band_width,
    RANK() OVER (
        PARTITION BY REPLACE(SERIES, '"', '')
        ORDER BY (UPPER_BOUND - LOWER_BOUND) DESC
    ) AS uncertainty_rank
FROM ADHOC.CITY_WEATHER_FORECAST
ORDER BY CITY, FORECAST_DATE;


-- View the final unified table combining historical actuals and 7-day forecast
SELECT * FROM ANALYTICS.CITY_WEATHER_FINAL
ORDER BY CITY, DATE DESC;

-- ============================================================
-- SECTION 6 — MODEL EVALUATION METRICS
-- ============================================================

-- View all stored evaluation metric runs across every pipeline execution
SELECT * FROM ANALYTICS.CITY_WEATHER_MODEL_METRICS
ORDER BY SERIES, ERROR_METRIC;

-- Show only the most recent run's metrics — one clean result per city per metric
SELECT
    SERIES     AS CITY,
    ERROR_METRIC,
    METRIC_VALUE,
    RUN_TS
FROM ANALYTICS.CITY_WEATHER_MODEL_METRICS
WHERE RUN_TS = (SELECT MAX(RUN_TS) FROM ANALYTICS.CITY_WEATHER_MODEL_METRICS)
ORDER BY SERIES, ERROR_METRIC;










