from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta
import requests


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


def return_snowflake_conn(conn_id):
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    conn = hook.get_conn()
    return conn.cursor()


# -------------------------
# EXTRACT
# -------------------------
@task
def extract(latitude, longitude):

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "past_days": 60,
        "forecast_days": 0,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "temperature_2m_mean",
            "precipitation_sum",
            "windspeed_10m_max",
            "weathercode"
        ],
        "timezone": "America/Los_Angeles"
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise RuntimeError(f"API request failed: {response.status_code}")

    return response.json()


# -------------------------
# TRANSFORM
# -------------------------
@task
def transform(raw_data, latitude, longitude, city):

    data = raw_data["daily"]

    records = []

    for i in range(len(data["time"])):

        records.append((
            city,
            latitude,
            longitude,
            data["time"][i],
            data["temperature_2m_max"][i],
            data["temperature_2m_min"][i],
            data["temperature_2m_mean"][i],
            data["precipitation_sum"][i],
            data["windspeed_10m_max"][i],
            data["weathercode"][i]
        ))

    return records


# -------------------------
# LOAD (UPSERT)
# -------------------------
@task
def load(records, target_table):

    cur = return_snowflake_conn("snowflake_conn")

    try:
        cur.execute("USE DATABASE USER_DB_FERRET")
        cur.execute("USE SCHEMA RAW")
        cur.execute("BEGIN")

        cur.execute(f"""
        CREATE TEMP TABLE CITY_WEATHER_STAGE (
            CITY STRING,
            LATITUDE FLOAT,
            LONGITUDE FLOAT,
            DATE DATE,
            TEMP_MAX FLOAT,
            TEMP_MIN FLOAT,
            TEMP_MEAN FLOAT,
            PRECIPITATION_MM FLOAT,
            WIND_SPEED_MAX_KMH FLOAT,
            WEATHER_CODE INTEGER
        )
        """)

        insert_stage = """
        INSERT INTO CITY_WEATHER_STAGE (
            CITY,
            LATITUDE,
            LONGITUDE,
            DATE,
            TEMP_MAX,
            TEMP_MIN,
            TEMP_MEAN,
            PRECIPITATION_MM,
            WIND_SPEED_MAX_KMH,
            WEATHER_CODE
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        cur.executemany(insert_stage, records)

        cur.execute(f"""
        MERGE INTO {target_table} t
        USING CITY_WEATHER_STAGE s
        ON t.CITY = s.CITY AND t.DATE = s.DATE

        WHEN MATCHED THEN UPDATE SET
            LATITUDE = s.LATITUDE,
            LONGITUDE = s.LONGITUDE,
            TEMP_MAX = s.TEMP_MAX,
            TEMP_MIN = s.TEMP_MIN,
            TEMP_MEAN = s.TEMP_MEAN,
            PRECIPITATION_MM = s.PRECIPITATION_MM,
            WIND_SPEED_MAX_KMH = s.WIND_SPEED_MAX_KMH,
            WEATHER_CODE = s.WEATHER_CODE

        WHEN NOT MATCHED THEN INSERT (
            CITY,
            LATITUDE,
            LONGITUDE,
            DATE,
            TEMP_MAX,
            TEMP_MIN,
            TEMP_MEAN,
            PRECIPITATION_MM,
            WIND_SPEED_MAX_KMH,
            WEATHER_CODE
        )
        VALUES (
            s.CITY,
            s.LATITUDE,
            s.LONGITUDE,
            s.DATE,
            s.TEMP_MAX,
            s.TEMP_MIN,
            s.TEMP_MEAN,
            s.PRECIPITATION_MM,
            s.WIND_SPEED_MAX_KMH,
            s.WEATHER_CODE
        )
        """)

        cur.execute("COMMIT")

        print("UPSERT SUCCESS")

    except Exception as e:

        cur.execute("ROLLBACK")
        raise e


# -------------------------
# DAG
# -------------------------
with DAG(
    dag_id="WeatherData_ETL",
    start_date=datetime(2026, 3, 2),
    schedule="30 2 * * *",
    catchup=False,
    tags=["ETL"],
    default_args=default_args
) as dag:

    cities = Variable.get("weather_cities", deserialize_json=True)

    target_table = "RAW.CITY_WEATHER"

    for city in cities:

        raw = extract(city["lat"], city["lon"])

        transformed = transform(
            raw,
            city["lat"],
            city["lon"],
            city["city"]
        )

        load(transformed, target_table)
