import boto3
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import time

default_args = {
    "owner": "Nanda",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def run_dq_job(**context):
  try:
    conf = context['dag_run'].conf
    date_str = conf['date_str']
    time_str = conf['time_str']
    client = boto3.client('glue', 'us-east-1')
    response = client.start_job_run(
        JobName='odssey_data_checks',
        Arguments={
            '--date_str': date_str,
            '--time_str': time_str
        }
    )
    response_id = response['JobRunId']
    context['ti'].xcom_push(key='dq_glue_job_id', value={'job_id': response_id})
    logging.info(f'Successfully triggered, Response Id: {response_id}')
    return response
  except Exception as e:
    logging.error(f'not able to run the dq job! {e}')

def monitor_dq_job(**context):
  try:      
    client = boto3.client('glue', 'us-east-1')
    response_id = context['ti'].xcom_pull(task_ids= 'dq_glue_job', key='dq_glue_job_id')['job_id']
    status = 'RUNNING'
    while status == 'RUNNING':
      response = client.get_job_run(
        JobName='odssey_data_checks',
        RunId= response_id,
      )
      status = response['JobRun']['JobRunState']
      logging.info(f'Status: {status}')
      time.sleep(5)
    return
  except Exception as e:
    logging.error(f'not able to get status of dq job! {e}')


with DAG(
    dag_id="run_DQ_glue_job",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="Dag to run and monitor the data quality glue job",
) as dag:

    run_DQ_job = PythonOperator(
        task_id="dq_glue_job",
        python_callable= run_dq_job
    )

    monitor_DQ_job = PythonOperator(
        task_id= "monitor_DQ_job",
        python_callable = monitor_dq_job
    )


run_DQ_job >> monitor_DQ_job