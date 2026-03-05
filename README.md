# Weather Prediction Analytics System
### Automated ETL · Snowflake ML Forecasting · Apache Airflow Orchestration

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache_Airflow-2.10.1-017CEE?style=flat-square&logo=apacheairflow&logoColor=white"/>
  <img src="https://img.shields.io/badge/Snowflake-ML_Forecast-29B5E8?style=flat-square&logo=snowflake&logoColor=white"/>
  <img src="https://img.shields.io/badge/Open--Meteo-Free_API-00C7B7?style=flat-square"/>
  <img src="https://img.shields.io/badge/Status-Active-22c55e?style=flat-square"/>
</p>

<p align="center">
  <img src="screenshots/weather_architecture_original.png" alt="System Architecture" width="100%"/>
</p>

---

## What This Project Does

This system ingests 60 days of real historical weather data from four US cities, stores it in Snowflake, and uses Snowflake's native machine learning engine to generate a 7-day temperature forecast — all running on a fully automated daily schedule through Apache Airflow.

Two separate pipelines handle the two stages of work. The first pipeline runs at **02:30 UTC** and handles data collection, transformation, and storage. The second runs one hour later at **03:30 UTC**, trains the forecasting model on fresh data, generates predictions, and assembles the final output table that places historical actuals and future forecasts side by side.

---

## Repository Contents

```
weather-prediction-analytics/
│
├── weather_etl_pipeline.py        ← Airflow DAG 1: data collection & storage
├── weather_prediction.py          ← Airflow DAG 2: ML training & forecasting
├── snowflake.sql                  ← All DDL, setup queries & analysis SQL
├── screenshots/
│   ├── airflow_dags_overview.png
│   ├── airflow_etl_graph.png
│   ├── airflow_trainpredict_graph.png
│   └── weather_architecture_original.png
└── README.md
```

---

## Cities in Scope

| # | City | State | Latitude | Longitude | Climate |
|---|------|-------|----------|-----------|---------|
| 1 | Miami | Florida | 25.7617° N | 80.1918° W | Tropical |
| 2 | Newport Beach | California | 33.6189° N | 117.9289° W | Mediterranean |
| 3 | Seattle | Washington | 47.6062° N | 122.3321° W | Oceanic |
| 4 | Boston | Massachusetts | 42.3601° N | 71.0589° W | Continental |

---

## Pipeline 1 — WeatherData_ETL

**File:** `weather_etl_pipeline.py`  
**DAG ID:** `WeatherData_ETL`  
**Schedule:** `30 2 * * *` — daily at 02:30 UTC  
**Total Tasks:** 12 (3 tasks × 4 cities, all running in parallel)

### Data Flow

```
Open-Meteo API
      │
      ▼
  extract()  ──Raw JSON──►  transform()  ──records[]──►  load()
  [per city]                [per city]                   [per city]
                                                              │
                                                              ▼
                                                  RAW.CITY_WEATHER (Snowflake)
```

### Task 1 — `extract(latitude, longitude)`

Calls the Open-Meteo `/v1/forecast` endpoint with `past_days=60` and `forecast_days=0`. No API key is required. Returns a raw JSON payload containing daily arrays for six weather variables:

- `temperature_2m_max` — daily high
- `temperature_2m_min` — daily low
- `temperature_2m_mean` — daily average
- `precipitation_sum` — total rainfall in mm
- `windspeed_10m_max` — peak wind speed
- `weathercode` — WMO condition code

Timezone is locked to `America/Los_Angeles` for all four cities to ensure consistent date alignment.

### Task 2 — `transform(raw_data, latitude, longitude, city)`

Unpacks the nested JSON into a flat list of Python tuples — one tuple per calendar day. Each tuple carries the city name, coordinates, date, and all six metric values in insertion order. No external transformation libraries are used; this is pure Python iteration over the API response structure.

### Task 3 — `load(records, target_table)`

Opens a Snowflake connection via `SnowflakeHook`, creates a temporary in-session staging table, bulk-inserts all records using `executemany`, then runs a `MERGE` statement that upserts into the permanent history table. The entire sequence runs inside a `BEGIN / COMMIT` transaction block with a `ROLLBACK` in the `except` handler — a failed run never writes partial data.

```sql
MERGE INTO RAW.CITY_WEATHER t
USING CITY_WEATHER_STAGE s
  ON t.CITY = s.CITY AND t.DATE = s.DATE
WHEN MATCHED THEN
    UPDATE SET TEMP_MAX = s.TEMP_MAX, TEMP_MIN = s.TEMP_MIN, ...
WHEN NOT MATCHED THEN
    INSERT (CITY, DATE, TEMP_MAX, ...) VALUES (s.CITY, s.DATE, s.TEMP_MAX, ...)
```

### Airflow Variable — `weather_cities`

City coordinates are stored as an Airflow Variable (JSON), not hardcoded. Adding a fifth city requires no code changes — update the variable and the DAG builds the new pipeline branch automatically on the next run.

```json
[
  { "city": "Miami",         "lat": 25.7617,  "lon": -80.1918  },
  { "city": "Newport Beach", "lat": 33.6189,  "lon": -117.9289 },
  { "city": "Seattle",       "lat": 47.6062,  "lon": -122.3321 },
  { "city": "Boston",        "lat": 42.3601,  "lon": -71.0589  }
]
```

> **Set via:** Admin → Variables → key: `weather_cities`

---

## Pipeline 2 — TrainPredict

**File:** `weather_prediction.py`  
**DAG ID:** `TrainPredict`  
**Schedule:** `30 3 * * *` — daily at 03:30 UTC  
**Total Tasks:** 2, strictly sequential (`train` → `predict`)

### Task 1 — `train()`

Creates a clean training view `ADHOC.CITY_WEATHER_TRAIN_VIEW` that filters out any rows where `TEMP_MAX` is null, then trains a `SNOWFLAKE.ML.FORECAST` model using `CITY` as the series column and `TEMP_MAX` as the prediction target. The `ON_ERROR: SKIP` configuration allows training to continue even when one city has a gap in data.

```sql
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST ANALYTICS.CITY_WEATHER_FORECAST_MODEL (
    INPUT_DATA        => SYSTEM$REFERENCE('VIEW', 'ADHOC.CITY_WEATHER_TRAIN_VIEW'),
    SERIES_COLNAME    => 'CITY',
    TIMESTAMP_COLNAME => 'DATE',
    TARGET_COLNAME    => 'TEMP_MAX',
    CONFIG_OBJECT     => { 'ON_ERROR': 'SKIP' }
);
```

After training, evaluation metrics are appended (with timestamp) to `ANALYTICS.CITY_WEATHER_MODEL_METRICS` so each run remains independently auditable.

### Task 2 — `predict()`

Calls the trained model to generate seven days of forecasts with a 95% prediction interval. Results are captured using `RESULT_SCAN(LAST_QUERY_ID())` and written to `ADHOC.CITY_WEATHER_FORECAST`. The final step assembles `ANALYTICS.CITY_WEATHER_FINAL` — a unified table where historical and predicted rows coexist:

```sql
SELECT CITY, DATE, TEMP_MAX AS ACTUAL,
       NULL AS FORECAST, NULL AS LOWER_BOUND, NULL AS UPPER_BOUND
FROM RAW.CITY_WEATHER

UNION ALL

SELECT REPLACE(SERIES, '"', '') AS CITY,
       TS AS DATE,
       NULL AS ACTUAL, FORECAST, LOWER_BOUND, UPPER_BOUND
FROM ADHOC.CITY_WEATHER_FORECAST
```

---

## Snowflake Database Structure

**Database:** `USER_DB_FERRET`

```
USER_DB_FERRET
├── RAW
│   └── CITY_WEATHER                     ← ETL destination — daily UPSERT
├── ADHOC
│   ├── CITY_WEATHER_TRAIN_VIEW          ← Clean input for ML model
│   └── CITY_WEATHER_FORECAST            ← Raw 7-day ML output
└── ANALYTICS
    ├── CITY_WEATHER_FINAL               ← Historical actuals + forecast (final output)
    └── CITY_WEATHER_MODEL_METRICS       ← Timestamped evaluation metrics per run
```

### `RAW.CITY_WEATHER` — Primary History Table

| Column | Type | Description |
|--------|------|-------------|
| `CITY` | STRING | City name — part of primary key |
| `LATITUDE` | FLOAT | Geographic latitude |
| `LONGITUDE` | FLOAT | Geographic longitude |
| `DATE` | DATE | Observation date — part of primary key |
| `TEMP_MAX` | FLOAT | Daily high temperature °C |
| `TEMP_MIN` | FLOAT | Daily low temperature °C |
| `TEMP_MEAN` | FLOAT | Daily average temperature °C |
| `PRECIPITATION_MM` | FLOAT | Total rainfall in millimetres |
| `WIND_SPEED_MAX_KMH` | FLOAT | Peak wind speed in km/h |
| `WEATHER_CODE` | INTEGER | WMO weather condition code |
| `LOAD_TS` | TIMESTAMP_NTZ | Auto-populated on insert — used for auditing |

**Primary Key:** `(CITY, DATE)` — enforces exactly one record per city per day regardless of pipeline reruns.

### `ANALYTICS.CITY_WEATHER_FINAL` — Output Table

| Column | Type | Description |
|--------|------|-------------|
| `CITY` | STRING | City name |
| `DATE` | DATE | Historical or future date |
| `ACTUAL` | FLOAT | Observed TEMP_MAX — NULL on forecast rows |
| `FORECAST` | FLOAT | Predicted TEMP_MAX — NULL on historical rows |
| `LOWER_BOUND` | FLOAT | 95% confidence interval lower limit |
| `UPPER_BOUND` | FLOAT | 95% confidence interval upper limit |

---

## Model Evaluation Results

Query to retrieve the latest clean metrics with no duplicates:

```sql
SELECT SERIES AS CITY, ERROR_METRIC, METRIC_VALUE
FROM ANALYTICS.CITY_WEATHER_MODEL_METRICS
WHERE RUN_TS = (SELECT MAX(RUN_TS) FROM ANALYTICS.CITY_WEATHER_MODEL_METRICS)
ORDER BY SERIES, ERROR_METRIC;
```

**Latest run — March 4, 2026 @ 19:31 UTC (28 rows — 4 cities × 7 metrics):**

| City | MAE °C | SMAPE | MDA | MSE | WINKLER |
|------|--------|-------|-----|-----|---------|
| Boston | 3.797 | 1.036 | 0.636 | 20.858 | 88.835 |
| Miami | 5.933 | 0.223 | 0.727 | 46.653 | 118.772 |
| Newport Beach | 5.745 | 0.305 | — | — | — |
| Seattle | — | 0.389 | 0.545 | — | — |

**Reading the metrics:**

`MAE` is the average absolute error in degrees Celsius. Boston at 3.8°C is reasonable for a volatile New England winter with only 60 days of training data.

`MDA` (Mean Directional Accuracy) measures how often the model correctly predicted whether the next day would be warmer or cooler. Any value above 0.5 beats random guessing — Miami at 0.727 means the model called the direction correctly nearly 3 out of 4 days.

`SMAPE` near zero is best. Miami's 0.223 and Seattle's 0.389 indicate low symmetric percentage error across both directions.

`MAPE` for Boston reads 350,244 — this is a known mathematical issue. When actual temperatures pass through or near 0°C in winter, the MAPE formula divides by values very close to zero and produces an extremely large number. This does not reflect poor model quality. For cold-weather cities, use MAE instead.

`COVERAGE_INTERVAL=0.95` values below 1.0 are expected with a 60-day training window. Snowflake ML calibrates its confidence bands much more accurately with 12+ months of history.

---

## Screenshots

### Airflow — Both DAGs Running
![Airflow DAGs Overview](screenshots/airflow_dags_overview.png)
*WeatherData_ETL (12 tasks) and TrainPredict (2 tasks) — both active and on schedule*

### WeatherData_ETL — Task Graph
![ETL Task Graph](screenshots/airflow_etl_graph.png)
*Four parallel extract → transform → load chains, one per city, all green*

### TrainPredict — Task Graph
![ML Task Graph](screenshots/airflow_trainpredict_graph.png)
*Sequential train → predict dependency enforced — both tasks successful*

---

## Useful Queries

**Confirm data loaded for all four cities:**
```sql
SELECT
    CITY,
    MIN(DATE)  AS earliest,
    MAX(DATE)  AS latest,
    COUNT(*)   AS total_days
FROM RAW.CITY_WEATHER
GROUP BY CITY
ORDER BY CITY;
```

**Verify historical and forecast row counts (expect 60 + 7 = 67 per city):**
```sql
SELECT
    CITY,
    COUNT(CASE WHEN ACTUAL   IS NOT NULL THEN 1 END) AS historical_rows,
    COUNT(CASE WHEN FORECAST IS NOT NULL THEN 1 END) AS forecast_rows
FROM ANALYTICS.CITY_WEATHER_FINAL
GROUP BY CITY
ORDER BY CITY;
```

**View the 7-day forecast with confidence bands:**
```sql
SELECT *
FROM ANALYTICS.CITY_WEATHER_FINAL
WHERE FORECAST IS NOT NULL
ORDER BY CITY, DATE;
```

**Weather descriptions alongside raw WMO codes:**
```sql
SELECT
    CITY, DATE, TEMP_MAX, TEMP_MIN,
    CASE WEATHER_CODE
        WHEN 0  THEN 'Clear sky'
        WHEN 1  THEN 'Mainly clear'
        WHEN 2  THEN 'Partly cloudy'
        WHEN 3  THEN 'Overcast'
        WHEN 45 THEN 'Fog'
        WHEN 61 THEN 'Slight rain'
        WHEN 63 THEN 'Moderate rain'
        WHEN 65 THEN 'Heavy rain'
        WHEN 80 THEN 'Rain showers'
        WHEN 95 THEN 'Thunderstorm'
        ELSE 'Code ' || WEATHER_CODE
    END AS CONDITIONS
FROM RAW.CITY_WEATHER
ORDER BY DATE DESC;
```

---

## Setup Guide

### Prerequisites

- Apache Airflow 2.10+ with `apache-airflow-providers-snowflake`
- Snowflake account with `TRAINING_ROLE`, warehouse `FERRET_QUERY_WH`, and ML Preview features enabled
- Python 3.10+

### Step 1 — Run Snowflake DDL

Open `snowflake.sql` in Snowflake Worksheets and execute it top to bottom. This creates the `ADHOC` schema and the `RAW.CITY_WEATHER` table with the correct primary key and audit column.

### Step 2 — Create Airflow Connection

Go to **Admin → Connections** and add:

| Field | Value |
|-------|-------|
| Connection ID | `snowflake_conn` |
| Type | Snowflake |
| Database | `USER_DB_FERRET` |
| Schema | `RAW` |
| Warehouse | `FERRET_QUERY_WH` |
| Role | `TRAINING_ROLE` |

### Step 3 — Create Airflow Variable

Go to **Admin → Variables** and add:

| Key | Value |
|-----|-------|
| `weather_cities` | JSON array from the Pipeline 1 section above |

### Step 4 — Deploy Both DAGs

Copy `weather_etl_pipeline.py` and `weather_prediction.py` into your Airflow `dags/` directory. Both DAGs will register automatically and begin running on their schedules.

---

## Design Decisions

**MERGE over DELETE + INSERT** — the UPSERT strategy makes every pipeline run idempotent. Running it twice on the same day produces the same table state as running it once. A delete-then-insert approach creates a brief window of data absence on every execution.

**One-hour scheduling gap** — the ETL DAG finishes in under four minutes for all four cities combined. The one-hour buffer is deliberate insurance: if any city task retries due to a transient API error, the ML pipeline still finds complete, fresh data when it starts.

**Timestamped metrics over overwrite** — appending each run's metrics with a `RUN_TS` column creates a natural performance audit trail. You can observe whether MAE is improving as the training window grows over time, without any additional logging setup.

**Single multi-series model** — using `SERIES_COLNAME = 'CITY'` trains one model that covers all four cities simultaneously. This is more efficient than four separate model objects and allows Snowflake ML to share statistical signal across series during training.

---

## Lessons Learned

A 60-day training window produces realistic MAE values but underperforms on confidence interval calibration. The `COVERAGE_INTERVAL` metric climbs toward 0.95 as more history accumulates — a 365-day rolling window would significantly improve forecast reliability in production.

The `MAPE` metric is unreliable for cities with temperatures near 0°C in winter and should be excluded from any automated reporting dashboard. `MAE` and `SMAPE` are mathematically stable alternatives that produce meaningful values regardless of the temperature scale or season.

Airflow's `@task` decorator combined with a loop over a JSON variable demonstrates a clean configuration-driven pattern for building pipelines. Adding new cities, changing metrics, or adjusting the lookback window are all one-line configuration changes rather than code edits.

---


<p align="center">
  Apache Airflow &nbsp;·&nbsp; Snowflake ML &nbsp;·&nbsp; Open-Meteo API &nbsp;·&nbsp; Python
</p>
