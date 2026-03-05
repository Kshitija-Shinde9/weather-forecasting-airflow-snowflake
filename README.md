# Weather Prediction Analytics Pipeline
### Powered by Snowflake ML · Apache Airflow · Open-Meteo API

<p align="center">
  <img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white"/>
  <img src="https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white"/>
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
  <img src="https://img.shields.io/badge/Open--Meteo-00C7B7?style=for-the-badge&logo=cloud&logoColor=white"/>
</p>

---

## Table of Contents

- [Overview](#-overview)
- [System Architecture](#-system-architecture)
- [Cities Tracked](#-cities-tracked)
- [Project Structure](#-project-structure)
- [Pipeline 1 — Weather ETL DAG](#-pipeline-1--weather-etl-dag)
- [Pipeline 2 — Train & Predict DAG](#-pipeline-2--trainpredict-dag)
- [Snowflake Schema & Tables](#-snowflake-schema--tables)
- [Airflow Setup](#️-airflow-setup)
- [Screenshots](#-screenshots)
- [Key SQL Queries](#-key-sql-queries)
- [Results & Forecast Output](#-results--forecast-output)
- [Lessons Learned](#-lessons-learned)

---

## Overview

This project builds a **fully automated end-to-end weather analytics and forecasting system** using:

- **Open-Meteo API** — free, open-source weather data with 60 days of historical observations
- **Apache Airflow** — orchestrates and schedules ETL and ML pipelines daily
- **Snowflake** — cloud data warehouse that stores raw data and runs ML forecasting natively via `SNOWFLAKE.ML.FORECAST`

The system ingests historical weather data for **4 US cities**, loads it into Snowflake daily, trains a time-series ML model, and generates **7-day temperature forecasts** — all fully automated.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        APACHE AIRFLOW                           │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  DAG 1: WeatherData_ETL  (Daily @ 02:30 UTC)             │  │
│  │                                                          │  │
│  │  Open-Meteo API  →  Extract  →  Transform  →  Load       │  │
│  │  (4 cities, parallel tasks)        ↓                     │  │
│  │                              Snowflake RAW.CITY_WEATHER   │  │
│  └──────────────────────────────────────────────────────────┘  │
│                              ↓                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  DAG 2: TrainPredict      (Daily @ 03:30 UTC)            │  │
│  │                                                          │  │
│  │  RAW.CITY_WEATHER  →  Train Forecast Model               │  │
│  │                              ↓                           │  │
│  │                       Predict 7 Days                     │  │
│  │                              ↓                           │  │
│  │              ANALYTICS.CITY_WEATHER_FINAL                │  │
│  │          (Historical UNION Forecast Results)             │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Cities Tracked

| City | Latitude | Longitude | State |
|------|----------|-----------|-------|
| Miami | 25.7617 | -80.1918 | Florida |
| Newport Beach | 33.6189 | -117.9289 | California |
| Seattle | 47.6062 | -122.3321 | Washington |
| Boston | 42.3601 | -71.0589 | Massachusetts |

---

## Project Structure

```
weather-prediction-analytics/
│
├── README.md
├── weather_etl_pipeline.py       # Airflow DAG 1 — ETL Pipeline
├── weather_prediction.py         # Airflow DAG 2 — Train & Predict
└── snowflake.sql                 # All Snowflake DDL & queries
```

---

## Pipeline 1 — Weather ETL DAG

**DAG ID:** `WeatherData_ETL`  
**Schedule:** `30 2 * * *` (Daily at 02:30 UTC)  
**File:** `weather_etl_pipeline.py`

### How It Works

The ETL pipeline runs **4 parallel pipelines** (one per city), each consisting of 3 tasks:

```
extract  →  transform  →  load
```

### Task Breakdown

#### `extract(latitude, longitude)`
- Calls the **Open-Meteo Forecast API**
- Fetches **past 60 days** of daily weather data
- Collects: `temp_max`, `temp_min`, `temp_mean`, `precipitation`, `wind_speed`, `weather_code`

```python
params = {
    "latitude": latitude,
    "longitude": longitude,
    "past_days": 60,
    "forecast_days": 0,
    "daily": ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
              "precipitation_sum", "windspeed_10m_max", "weathercode"],
    "timezone": "America/Los_Angeles"
}
```

#### `transform(raw_data, latitude, longitude, city)`
- Flattens the nested API JSON response
- Converts data into a list of tuples, one per day
- Returns clean records ready for loading

#### `load(records, target_table)`
- Uses a **MERGE (UPSERT)** strategy via a temp staging table
- Prevents duplicate records on re-runs
- Wrapped in **SQL transaction** with `try/except/rollback` for data integrity

```sql
MERGE INTO CITY_WEATHER t
USING CITY_WEATHER_STAGE s
ON t.CITY = s.CITY AND t.DATE = s.DATE
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

### Airflow Variables Used

City configurations are stored as an **Airflow Variable** (JSON) — no hardcoded values:

```json
[
  {"city": "Miami",         "lat": 25.7617,  "lon": -80.1918},
  {"city": "Newport Beach", "lat": 33.6189,  "lon": -117.9289},
  {"city": "Seattle",       "lat": 47.6062,  "lon": -122.3321},
  {"city": "Boston",        "lat": 42.3601,  "lon": -71.0589}
]
```

> Set via **Admin → Variables → `weather_cities`** in the Airflow UI

---

## Pipeline 2 — TrainPredict DAG

**DAG ID:** `TrainPredict`  
**Schedule:** `30 3 * * *` (Daily at 03:30 UTC — runs 1 hour after ETL)  
**File:** `weather_prediction.py`

### How It Works

```
RAW.CITY_WEATHER  →  [train]  →  [predict]  →  ANALYTICS.CITY_WEATHER_FINAL
```

#### Task 1 `train()`

1. Creates a **clean training view** (`ADHOC.CITY_WEATHER_TRAIN_VIEW`) with non-null `TEMP_MAX`
2. Trains a native **Snowflake ML Forecast Model** using `SNOWFLAKE.ML.FORECAST`
3. Saves evaluation metrics to `ANALYTICS.CITY_WEATHER_MODEL_METRICS`

```sql
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST ANALYTICS.CITY_WEATHER_FORECAST_MODEL (
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'ADHOC.CITY_WEATHER_TRAIN_VIEW'),
    SERIES_COLNAME => 'CITY',
    TIMESTAMP_COLNAME => 'DATE',
    TARGET_COLNAME => 'TEMP_MAX',
    CONFIG_OBJECT => {'ON_ERROR': 'SKIP'}
);
```

#### Task 2 `predict()`

1. Runs the trained model to generate **7-day forecasts** with **95% prediction intervals**
2. Captures results using `RESULT_SCAN(LAST_QUERY_ID())`
3. Stores forecast in `ADHOC.CITY_WEATHER_FORECAST`
4. Creates the **final union table** combining historical actuals + forecast predictions:

```sql
-- Historical data
SELECT CITY, DATE, TEMP_MAX AS ACTUAL, NULL AS FORECAST, ...
FROM RAW.CITY_WEATHER

UNION ALL

-- ML Forecast
SELECT CITY, TS AS DATE, NULL AS ACTUAL, FORECAST, LOWER_BOUND, UPPER_BOUND
FROM ADHOC.CITY_WEATHER_FORECAST
```

---

## Snowflake Schema & Tables

### Database & Schema Layout

```
USER_DB_FERRET
├── RAW
│   └── CITY_WEATHER              ← Historical weather data (ETL target)
├── ADHOC
│   ├── CITY_WEATHER_TRAIN_VIEW   ← Clean view for ML training
│   └── CITY_WEATHER_FORECAST     ← Raw ML forecast output
└── ANALYTICS
    ├── CITY_WEATHER_FINAL        ← Historical + Forecast (final table)
    └── CITY_WEATHER_MODEL_METRICS ← Model evaluation metrics
```

### `RAW.CITY_WEATHER` — Main History Table

| Column | Type | Description |
|--------|------|-------------|
| `CITY` | STRING | City name (part of PK) |
| `LATITUDE` | FLOAT | Geographic latitude |
| `LONGITUDE` | FLOAT | Geographic longitude |
| `DATE` | DATE | Observation date (part of PK) |
| `TEMP_MAX` | FLOAT | Daily max temperature (°C) |
| `TEMP_MIN` | FLOAT | Daily min temperature (°C) |
| `TEMP_MEAN` | FLOAT | Daily mean temperature (°C) |
| `PRECIPITATION_MM` | FLOAT | Total precipitation (mm) |
| `WIND_SPEED_MAX_KMH` | FLOAT | Max wind speed (km/h) |
| `WEATHER_CODE` | INTEGER | WMO weather interpretation code |
| `LOAD_TS` | TIMESTAMP_NTZ | Auto-set on insert (audit column) |

> **Primary Key:** `(CITY, DATE)` — ensures one record per city per day

### `ANALYTICS.CITY_WEATHER_FINAL` — Final Output Table

| Column | Type | Description |
|--------|------|-------------|
| `CITY` | STRING | City name |
| `DATE` | DATE | Historical or forecast date |
| `ACTUAL` | FLOAT | Historical TEMP_MAX (NULL for forecast rows) |
| `FORECAST` | FLOAT | Predicted TEMP_MAX (NULL for historical rows) |
| `LOWER_BOUND` | FLOAT | 95% prediction interval lower bound |
| `UPPER_BOUND` | FLOAT | 95% prediction interval upper bound |

---

## Airflow Setup

### Prerequisites

- Apache Airflow 2.10+
- `apache-airflow-providers-snowflake`
- Snowflake account with `TRAINING_ROLE` and ML features enabled

### Connections

Configure in **Admin → Connections:**

| Conn ID | Type | Description |
|---------|------|-------------|
| `snowflake_conn` | Snowflake | Points to `USER_DB_FERRET`, schema `RAW` |

### Variables

Configure in **Admin → Variables:**

| Key | Type | Description |
|-----|------|-------------|
| `weather_cities` | JSON | List of city objects with `city`, `lat`, `lon` |

### DAG Execution Order

```
02:30 UTC  →  WeatherData_ETL   (fetches + loads 60-day weather for 4 cities)
03:30 UTC  →  TrainPredict      (trains ML model + generates 7-day forecast)
```

---

## Screenshots

> **Place your screenshots in a `/screenshots` folder in this repo and update the paths below.**

### Airflow DAGs Overview
![Airflow DAGs List](screenshots/airflow_dags_overview.png)
*Two active DAGs: `WeatherData_ETL` (12 tasks) and `TrainPredict` (2 tasks), both running successfully*

### WeatherData_ETL — Graph View
![ETL DAG Graph](screenshots/airflow_etl_graph.png)
*4 parallel ETL pipelines (extract → transform → load) for each city, all showing success*

### TrainPredict — Graph View
![ML DAG Graph](screenshots/airflow_trainpredict_graph.png)
*Sequential ML pipeline: `train` → `predict`, both tasks completed successfully*

---

## Key SQL Queries

### Check Latest Loaded Data
```sql
SELECT * FROM RAW.CITY_WEATHER
WHERE LOAD_TS > '2026-03-04'
ORDER BY CITY, DATE DESC;
```

### City Record Summary
```sql
SELECT
    CITY,
    MIN(DATE) AS first_date,
    MAX(DATE) AS last_date,
    COUNT(*) AS total_records
FROM RAW.CITY_WEATHER
GROUP BY CITY;
```

### Weather with Human-Readable Descriptions
```sql
SELECT *,
    CASE WEATHER_CODE
        WHEN 0  THEN 'Clear sky'
        WHEN 1  THEN 'Mainly clear'
        WHEN 2  THEN 'Partly cloudy'
        WHEN 3  THEN 'Overcast'
        WHEN 45 THEN 'Fog'
        WHEN 61 THEN 'Slight rain'
        WHEN 63 THEN 'Moderate rain'
        WHEN 95 THEN 'Thunderstorm'
        ELSE 'Unknown'
    END AS WEATHER_DESCRIPTION
FROM RAW.CITY_WEATHER;
```

### View Forecast Results
```sql
SELECT * FROM ANALYTICS.CITY_WEATHER_FINAL
ORDER BY CITY, DATE DESC;
```

### View Model Evaluation Metrics
```sql
SELECT * FROM ANALYTICS.CITY_WEATHER_MODEL_METRICS;
```

---

## Results & Forecast Output

The `ANALYTICS.CITY_WEATHER_FINAL` table contains a **unified view** of:

-  **Historical actuals** — 60 days of real weather observations
-  **7-day forecast** — ML-predicted max temperature with 95% confidence intervals

### Sample Output Structure

```
CITY          | DATE       | ACTUAL | FORECAST | LOWER_BOUND | UPPER_BOUND
--------------|------------|--------|----------|-------------|------------
Boston        | 2026-03-05 | 8.2    | NULL     | NULL        | NULL
Boston        | 2026-03-06 | NULL   | 9.1      | 6.8         | 11.4
Newport Beach | 2026-03-05 | 21.4   | NULL     | NULL        | NULL
Newport Beach | 2026-03-06 | NULL   | 22.0     | 19.5        | 24.5
...
```

---

## Lessons Learned

1. **Snowflake ML is powerful out-of-the-box** — `SNOWFLAKE.ML.FORECAST` requires minimal setup compared to external ML frameworks, and handles multi-series forecasting with a single `SERIES_COLNAME` parameter.

2. **UPSERT over INSERT** — Using `MERGE` with a staging table makes the pipeline idempotent. Re-running won't create duplicates.

3. **Airflow Variables > hardcoding** — Storing city config in Airflow Variables means adding a new city requires zero code changes.

4. **DAG scheduling sequencing** — Scheduling `TrainPredict` one hour after `WeatherData_ETL` ensures fresh data is always available before training begins.

5. **SQL transactions protect data integrity** — Wrapping all Snowflake operations in `BEGIN/COMMIT/ROLLBACK` inside `try/except` blocks prevents partial writes on failure.

---

## Authors

Built as part of **DATA226 — Building a Weather Prediction Analytics using Snowflake & Airflow**

---

<p align="center">
  <i>Built with ❤️ using Apache Airflow, Snowflake ML, and Open-Meteo</i>
</p>
