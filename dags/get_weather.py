from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests, logging
import pandas as pd

default_args = {
    "owner": "Nanda",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def process_weather(**context):
    conf = context["dag_run"].conf
    date_str = conf["date_str"]
    time_str = conf["time_str"]

    raw_path = (
        f"s3://odssey-landing-data/landing_zone/weather/"
        f"date={date_str}/time={time_str}/"
    )

    city = "Zagreb"
    api_key = "your_openweather_api_key"
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"

    response = requests.get(url)
    response.raise_for_status()

    df = pd.DataFrame([response.json()])
    df.to_csv(f"{raw_path}weather.csv", index=False)

    logging.info("Weather data successfully written to %s", raw_path)

with DAG(
    dag_id="weather_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="Ingest weather data",
) as dag:

    ingest_weather = PythonOperator(
        task_id="ingest_weather",
        python_callable=process_weather,
    )

ingest_weather