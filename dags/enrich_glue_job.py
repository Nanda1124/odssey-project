from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import DAG
import boto3
import time
import logging
from datetime import datetime, timedelta

default_args = {
    "owner": "Nanda",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def run_glue_job(**context):
  conf = context['dag_run'].conf
  date_str = conf['date_str']
  time_str = conf['time_str']
  client = boto3.client('glue','us-east-1')
  response = client.start_job_run(
      JobName='odssey process raw',
      Arguments={
          '--date_str': date_str,
          '--time_str': time_str
      }
  )
  job_id = response['JobRunId']
  logging.info(f'response: {response}')
  context['ti'].xcom_push(
      key = 'glue_job_run',
      value = {
          'job_id': job_id
      }
  )

def get_glue_job_status(**context):
  job_id = context['ti'].xcom_pull(task_ids='silver_glue_job', key= 'glue_job_run')['job_id']

  client = boto3.client('glue','us-east-1')
  status = 'RUNNING'
  while status == 'RUNNING':
    response = client.get_job_run(
      JobName='odssey process raw',
      RunId=job_id,
      PredecessorsIncluded=True
    )
    status = response['JobRun']['JobRunState']
    logging.info(f'status: {status}')
    time.sleep(5)




with DAG(
    dag_id="run_glue_job",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="Trigger the Glue job for processing raw layer"
) as dag:
  glue_job = PythonOperator(
      task_id = 'silver_glue_job',
      python_callable = run_glue_job
  )

  glue_job_status = PythonOperator(
      task_id= 'glue_job_status',
      python_callable = get_glue_job_status
  )


glue_job >> glue_job_status