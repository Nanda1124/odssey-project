from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests, zipfile, io, logging
import pandas as pd

default_args = {
    "owner": "Nanda",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def process_schedule(**context):
    conf = context["dag_run"].conf
    date_str = conf["date_str"]
    time_str = conf["time_str"]

    raw_path = (
        f"s3://odssey-landing-data/landing_zone/schedule/"
        f"date={date_str}/time={time_str}/"
    )

    url = "https://storage.googleapis.com/storage/v1/b/mdb-latest/o/hr-gradski-parking-gtfs-2910.zip?alt=media"
    response = requests.get(url)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        for file_name in z.namelist():
            with z.open(file_name) as f:
                df = pd.read_csv(f)
                df.to_csv(f"{raw_path}{file_name.replace('txt', 'csv')}", index=False)

    logging.info("Schedule data successfully written to %s", raw_path)

with DAG(
    dag_id="schedule_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="Ingest GTFS schedule data",
) as dag:

    ingest_schedule = PythonOperator(
        task_id="ingest_schedule",
        python_callable=process_schedule,
    )

ingest_schedule