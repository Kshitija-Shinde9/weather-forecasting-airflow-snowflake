# Weather Prediction Analytics Pipeline
### Powered by Snowflake ML · Apache Airflow · Open-Meteo API
---

## Table of Contents
- [Overview](#overview)
- [System Architecture](#system-architecture)
- [Cities Tracked](#cities-tracked)
- [Project Structure](#project-structure)
- [Pipeline 1 — Weather ETL DAG](#pipeline-1----weather-etl-dag)
- [Pipeline 2 — Train & Predict DAG](#pipeline-2----trainpredict-dag)
- [Snowflake Schema & Tables](#snowflake-schema--tables)
- [Airflow Setup](#airflow-setup)
- [Key SQL Queries](#key-sql-queries)
- [Results & Forecast Output](#results--forecast-output)
- [Lessons Learned](#lessons-learned)

---

## Overview

This project builds a **fully automated end-to-end weather analytics and forecasting system**:

It ingests 60 days of real historical weather data from four US cities, stores it in Snowflake, and uses Snowflake's native machine learning engine to generate a 7-day temperature forecast — all running on a fully automated daily schedule through Apache Airflow.

Two separate pipelines handle the two stages of work. The first pipeline runs at **02:30 UTC** and handles data collection, transformation, and storage. The second runs one hour later at **03:30 UTC**, trains the forecasting model on fresh data, generates predictions, and assembles the final output table that places historical actuals and future forecasts side by side.

---

## System Architecture

<img width="1400" height="960" alt="image" src="https://github.com/user-attachments/assets/3d7f6126-92dd-46da-a11c-cdae93218193" />

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
weather-forecasting-airflow-snowflake/
│
├── README.md
├── dags/
│   ├── weather_etl_pipeline.py        # Airflow DAG 1 — ETL Pipeline
│   └── weather_prediction.py          # Airflow DAG 2 — Train & Predict
└── sql/
    └── snowflake.sql                  # All Snowflake DDL & queries
```
---

## Pipeline 1 — Weather ETL DAG

<img width="1710" height="953" alt="image" src="https://github.com/user-attachments/assets/fab23574-6fe8-4f7a-bc3e-90981f550692" />

**DAG ID:** `WeatherData_ETL`  
**Schedule:** `30 2 * * *` (Daily at 02:30 UTC)  
**File:** `weather_etl_pipeline.py`

### How It Works

The ETL pipeline runs **4 parallel pipelines** (one per city), each consisting of 3 tasks:

```
extract  -->  transform  -->  load
```

### Task Breakdown

#### 1 `extract(latitude, longitude)`
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

#### 2 `transform(raw_data, latitude, longitude, city)`
- Flattens the nested API JSON response
- Converts data into a list of tuples, one per day
- Returns clean records ready for loading

#### 3 `load(records, target_table)`
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

> Set via **Admin --> Variables --> `weather_cities`** in the Airflow UI

---

## Pipeline 2 — TrainPredict DAG

**DAG ID:** `TrainPredict`  
**Schedule:** `30 3 * * *` (Daily at 03:30 UTC — runs 1 hour after ETL)  
**File:** `weather_prediction.py`

<img width="1710" height="953" alt="image" src="https://github.com/user-attachments/assets/c7ee9e6f-2229-4413-82a1-32cbccd0bbb1" />

### How It Works

```
RAW.CITY_WEATHER  -->  [train]  -->  [predict]  -->  ANALYTICS.CITY_WEATHER_FINAL
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
│   └── CITY_WEATHER              <-- Historical weather data (ETL target)
├── ADHOC
│   ├── CITY_WEATHER_TRAIN_VIEW   <--  Clean view for ML training
│   └── CITY_WEATHER_FORECAST     <--  Raw ML forecast output
└── ANALYTICS
    ├── CITY_WEATHER_FINAL        <--  Historical + Forecast (final table)
    └── CITY_WEATHER_MODEL_METRICS <--  Model evaluation metrics
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

<img width="1281" height="639" alt="image" src="https://github.com/user-attachments/assets/0c906d4f-46ce-492a-95cf-38acb26bd7e3" />


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
<img width="1288" height="820" alt="image" src="https://github.com/user-attachments/assets/daf73e0e-7bac-4507-a38d-6bc9fab4fbbb" />

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

<img width="1710" height="949" alt="image" src="https://github.com/user-attachments/assets/47ce4c20-2706-4336-9fa2-e6556adc48d4" />


### Variables

Configure in **Admin → Variables:**

| Key | Type | Description |
|-----|------|-------------|
| `weather_cities` | JSON | List of city objects with `city`, `lat`, `lon` |

<img width="1710" height="949" alt="image" src="https://github.com/user-attachments/assets/9f98c63a-f52f-40d4-947d-d55415d8dba1" />

### DAG Execution Order

```
02:30 UTC  →  WeatherData_ETL   (fetches + loads 60-day weather for 4 cities)
03:30 UTC  →  TrainPredict      (trains ML model + generates 7-day forecast)
```
<img width="1710" height="470" alt="image" src="https://github.com/user-attachments/assets/de3209ce-1aef-46fa-a2a2-4e76026753aa" />

---

## Key SQL Queries

### Check Latest Loaded Data
```sql
SELECT * FROM RAW.CITY_WEATHER
WHERE LOAD_TS > '2026-03-04'
ORDER BY CITY, DATE DESC;
```
<img width="1710" height="495" alt="image" src="https://github.com/user-attachments/assets/2fb2dd58-086a-4011-9ba0-41e67f4fbcde" />

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
<img width="1710" height="495" alt="image" src="https://github.com/user-attachments/assets/b999f13e-39d4-4b39-b770-c8a3241acb29" />

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
<img width="1710" height="950" alt="image" src="https://github.com/user-attachments/assets/81dbe578-9d48-485c-bcca-6ed379fece68" />

### View Forecast Results
```sql
SELECT * FROM ANALYTICS.CITY_WEATHER_FINAL
ORDER BY CITY, DATE DESC;
```
<img width="1710" height="950" alt="image" src="https://github.com/user-attachments/assets/fabb3761-cada-4378-97e8-a2dc52f179ec" />

### View Model Evaluation Metrics
```sql
SELECT * FROM ANALYTICS.CITY_WEATHER_MODEL_METRICS;
```
<img width="1288" height="782" alt="image" src="https://github.com/user-attachments/assets/18a9c51c-b824-4bf8-9560-f4583e9f06b0" />

---

## Results & Forecast Output

The `ANALYTICS.CITY_WEATHER_FINAL` table contains a **unified view** of:

- **Historical actuals** — 60 days of real weather observations
- **7-day forecast** — ML-predicted max temperature with 95% confidence intervals
  
<img width="1710" height="950" alt="image" src="https://github.com/user-attachments/assets/fabb3761-cada-4378-97e8-a2dc52f179ec" />

<img width="1707" height="635" alt="image" src="https://github.com/user-attachments/assets/231e3c10-509b-4c9a-a344-f3d3241f0b9f" />


---

## Lessons Learned

1. **Snowflake ML is powerful out-of-the-box** — `SNOWFLAKE.ML.FORECAST` requires minimal setup compared to external ML frameworks, and handles multi-series forecasting with a single `SERIES_COLNAME` parameter.

2. **UPSERT over INSERT** — Using `MERGE` with a staging table makes the pipeline idempotent. Re-running won't create duplicates.

3. **Airflow Variables > hardcoding** — Storing city config in Airflow Variables means adding a new city requires zero code changes.

4. **DAG scheduling sequencing** — Scheduling `TrainPredict` one hour after `WeatherData_ETL` ensures fresh data is always available before training begins.

5. **SQL transactions protect data integrity** — Wrapping all Snowflake operations in `BEGIN/COMMIT/ROLLBACK` inside `try/except` blocks prevents partial writes on failure.

---
