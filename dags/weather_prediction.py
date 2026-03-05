from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


# ----------------------------------------------------------
# Snowflake Connection Helper
# ----------------------------------------------------------
def return_snowflake_conn(conn_id="snowflake_conn"):
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    conn = hook.get_conn()
    return conn, conn.cursor()


# ----------------------------------------------------------
# DAG CONFIG
# ----------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="TrainPredict",
    start_date=datetime(2026, 3, 1),
    schedule="30 3 * * *",   # runs after ETL DAG
    catchup=False,
    default_args=default_args,
    tags=["ML", "Forecast", "Weather", "Snowflake"],
) as dag:

    train_input_table = "RAW.CITY_WEATHER"
    train_view = "ADHOC.CITY_WEATHER_TRAIN_VIEW"

    model_name = "ANALYTICS.CITY_WEATHER_FORECAST_MODEL"

    forecast_table = "ADHOC.CITY_WEATHER_FORECAST"

    final_table = "ANALYTICS.CITY_WEATHER_FINAL"

    metrics_table = "ANALYTICS.CITY_WEATHER_MODEL_METRICS"


# ----------------------------------------------------------
# TASK 1 — TRAIN MODEL
# ----------------------------------------------------------
    @task
    def train():

        conn, cur = return_snowflake_conn()

        try:

            cur.execute("USE DATABASE USER_DB_FERRET")
            cur.execute("USE SCHEMA RAW")

            cur.execute("BEGIN")

            # -------------------------------------------
            # Create Clean Training View
            # -------------------------------------------
            create_view_sql = f"""
            CREATE OR REPLACE VIEW {train_view} AS
            SELECT
                CITY,
                DATE,
                TEMP_MAX
            FROM {train_input_table}
            WHERE TEMP_MAX IS NOT NULL
            ORDER BY CITY, DATE;
            """

            cur.execute(create_view_sql)

            # -------------------------------------------
            # Train Snowflake Forecast Model
            # -------------------------------------------
            create_model_sql = f"""
            CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {model_name}
            (
                INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
                SERIES_COLNAME => 'CITY',
                TIMESTAMP_COLNAME => 'DATE',
                TARGET_COLNAME => 'TEMP_MAX',
                CONFIG_OBJECT => {{
                    'ON_ERROR': 'SKIP'
                }}
            );
            """

            cur.execute(create_model_sql)

            # -------------------------------------------
            # Store Evaluation Metrics
            # -------------------------------------------
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {metrics_table}
            AS
            SELECT CURRENT_TIMESTAMP() AS RUN_TS,
                   *
            FROM TABLE({model_name}!SHOW_EVALUATION_METRICS())
            LIMIT 0
            """)

            cur.execute(f"""
            INSERT INTO {metrics_table}
            SELECT CURRENT_TIMESTAMP() AS RUN_TS,
                   *
            FROM TABLE({model_name}!SHOW_EVALUATION_METRICS())
            """)

            cur.execute("COMMIT")

        except Exception as e:

            cur.execute("ROLLBACK")
            raise

        finally:

            cur.close()
            conn.close()


# ----------------------------------------------------------
# TASK 2 — GENERATE FORECAST
# ----------------------------------------------------------
    @task
    def predict():

        conn, cur = return_snowflake_conn()

        try:

            # 1️⃣ Run Forecast
            cur.execute(f"""
            CALL {model_name}!FORECAST(
                FORECASTING_PERIODS => 7,
                CONFIG_OBJECT => {{'prediction_interval': 0.95}}
            )
            """)

            # 2️⃣ Get Query ID
            cur.execute("SELECT LAST_QUERY_ID()")
            query_id = cur.fetchone()[0]

            # 3️⃣ Store Forecast Results
            cur.execute(f"""
            CREATE OR REPLACE TABLE {forecast_table} AS
            SELECT *
            FROM TABLE(RESULT_SCAN('{query_id}'))
            """)

            # 4️⃣ Create Final Table (Historical + Forecast)
            cur.execute(f"""
            CREATE OR REPLACE TABLE {final_table} AS

            SELECT
                CITY,
                DATE,
                TEMP_MAX AS ACTUAL,
                NULL AS FORECAST,
                NULL AS LOWER_BOUND,
                NULL AS UPPER_BOUND
            FROM {train_input_table}

            UNION ALL

            SELECT
                REPLACE(SERIES, '"', '') AS CITY,
                TS AS DATE,
                NULL AS ACTUAL,
                FORECAST,
                LOWER_BOUND,
                UPPER_BOUND
            FROM {forecast_table}
            """)

            print("Forecast + Final Table Created Successfully")

        except Exception as e:
            raise

        finally:
            cur.close()
            conn.close()


# ----------------------------------------------------------
# DAG ORDER
# ----------------------------------------------------------
    train_task = train()

    predict_task = predict()

    train_task >> predict_task
