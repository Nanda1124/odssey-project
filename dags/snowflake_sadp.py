import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

args = {"owner": "Nanda", "start_date": datetime(2021,3,22,17,15)}

def execute_query(**context):
  hist_load_queries = context['ti'].xcom_pull(task_ids= 'construct_query', key= 'constructed_query')['hist_load_queries_list']
  truncate_queries = context['ti'].xcom_pull(task_ids= 'construct_query', key= 'constructed_query')['truncate_queries_list']
  data_load_queries = context['ti'].xcom_pull(task_ids= 'construct_query', key= 'constructed_query')['load_queries_list']
  hook = SnowflakeHook(snowflake_conn_id='odssey_snowflake_conn')
  hook.run(hist_load_queries)
  hook.run(truncate_queries)
  hook.run(data_load_queries)

def construct_queries(**context):
  hist_load_queries = []
  truncate_queries = []
  data_load_queries = []
  conf = context['dag_run'].conf
  date_str = conf['date_str']
  time_str = conf['time_str']
  files_list = ['trips', 'routes', 'agency', 'calendar', 'calendar_dates', 'stops', 'stop_times']
  for file_name in files_list:
    query = f"COPY INTO {file_name} FROM @s3_enriched_odssey/schedule/date={date_str}/time={time_str}/{file_name}/ MATCH_BY_COLUMN_NAME= CASE_INSENSITIVE;"
    data_load_queries.append(query)
    query = f"INSERT INTO consumption_hist.{file_name}_hist SELECT *, CURRENT_TIMESTAMP() AS valid_upto from consumption.{file_name};"
    hist_load_queries.append(query)
    truncate_query = f"TRUNCATE TABLE consumption.{file_name};"
    truncate_queries.append(truncate_query)

  query = f'COPY INTO weather FROM @s3_enriched_odssey/weather/date={date_str}/time={time_str}/ MATCH_BY_COLUMN_NAME= CASE_INSENSITIVE;'
  data_load_queries.append(query)
  context['ti'].xcom_push(key= 'constructed_query', value = {'load_queries_list': data_load_queries, 'hist_load_queries_list': hist_load_queries, 'truncate_queries_list': truncate_queries})
  return

with DAG(
    dag_id="snowflake_connector", 
    default_args=args, 
    start_date = datetime(2024, 1, 1),
    catchup= False,
    description= 'load data into snowflake with copy command'
) as dag:
  construct_query = PythonOperator(
      task_id = 'construct_query',
      python_callable = construct_queries
  )

  execute_query = PythonOperator(
      task_id= 'execute_query',
      python_callable= execute_query
  )



construct_query >> execute_query