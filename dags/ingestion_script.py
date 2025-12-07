# schedule_ingestion.py

import logging
import requests
import zipfile
import io
import pandas as pd
import os
import shutil
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta

def set_get_logging():
  root = logging.getLogger()
  if root.handlers:
    for handler in root.handlers[:]:
      root.removeHandler(handler)
  logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler()])
  logging.debug('This is a debug message.')
  logging.info('This is an info message.')
  return logging

def save_files(file_name, zip_file, logging):
  with zip_file.open(file_name) as f:
    df_one = pd.read_csv(f)
    logging.info('logging first 5 records.......')
    logging.info(df_one.head())
    file_name = file_name.replace('txt','csv')
    df_one.to_csv('output/'+file_name)
  return

def schedule_data():
  try:
    logging = set_get_logging()
    logging.info('Starting downloading file !')
    response = requests.get('https://transitfeeds.com/p/zet-zagreba-ki-elektri-ni-tramvaj/1007/latest/download')
    logging.info(response.status_code)
    if os.path.exists('output'):
      shutil.rmtree('output')
    os.makedirs('output')
    if response.status_code == 200:
      zip_buffer = io.BytesIO(response.content)
      with zipfile.ZipFile(zip_buffer) as z:
        logging.info('the downloaded files are %s',str(z.namelist()))
        for file_name in z.namelist():
          logging.info(f'started processing of {file_name}')
          save_files(file_name, z, logging)
          logging.info(f'finished processing of {file_name}')
      logging.info('successfully saved files !')


  except Exception as e:
    logging.error(f'Not able to fetch schedule data : {e}')




default_args = {
    'owner': 'Nanda',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'get_schedule_data',
    default_args=default_args,
    description='fetching the latest schedule data'
)

run_etl = PythonOperator(
    task_id='get_latest_schedule_data',
    python_callable= schedule_data,
    dag=dag,
)

run_etl