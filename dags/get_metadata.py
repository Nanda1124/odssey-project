from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    "owner": "Nanda",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def generate_metadata(**context):
    run_ts = datetime.utcnow()

    context["ti"].xcom_push(
        key="metadata",
        value={
            "date_str": run_ts.strftime("%Y-%m-%d"),
            "time_str": run_ts.strftime("%H-%M"),
        },
    )

with DAG(
    dag_id="metadata_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="Generate runtime metadata and trigger downstream DAGs",
) as dag:

    get_metadata = PythonOperator(
        task_id="get_metadata",
        python_callable=generate_metadata,
    )

    trigger_schedule = TriggerDagRunOperator(
        task_id="trigger_schedule_dag",
        trigger_dag_id="schedule_dag",
        conf={
            "date_str": "{{ ti.xcom_pull(task_ids='get_metadata', key='metadata')['date_str'] }}",
            "time_str": "{{ ti.xcom_pull(task_ids='get_metadata', key='metadata')['time_str'] }}",
        },
        wait_for_completion=True,        # This pauses this task until the external DAG finishes
        poke_interval=30,                # Check status every 30 seconds
        reset_dag_run=True,              # Allows re-running this if you clear it
        failed_states=["failed"]         # Fail this task if the external DAG fails
    )

    trigger_weather = TriggerDagRunOperator(
        task_id="trigger_weather_dag",
        trigger_dag_id="weather_dag",
        conf={
            "date_str": "{{ ti.xcom_pull(task_ids='get_metadata', key='metadata')['date_str'] }}",
            "time_str": "{{ ti.xcom_pull(task_ids='get_metadata', key='metadata')['time_str'] }}",
        },
        wait_for_completion=True,        # This pauses this task until the external DAG finishes
        poke_interval=30,                # Check status every 30 seconds
        reset_dag_run=True,              # Allows re-running this if you clear it
        failed_states=["failed"]         # Fail this task if the external DAG fails
    )

    trigger_glue_job = TriggerDagRunOperator(
	    task_id="trigger_glue_job_dag",
	    trigger_dag_id="run_glue_job",
	    conf={
            "date_str": "{{ ti.xcom_pull(task_ids='get_metadata', key='metadata')['date_str'] }}",
            "time_str": "{{ ti.xcom_pull(task_ids='get_metadata', key='metadata')['time_str'] }}",
        },
      wait_for_completion=True,        # This pauses this task until the external DAG finishes
      poke_interval=30,                # Check status every 30 seconds
      reset_dag_run=True,              # Allows re-running this if you clear it
      failed_states=["failed"]         # Fail this task if the external DAG fails
    )

    trigger_DQ_glue_job = TriggerDagRunOperator(
        task_id= "trigger_DQ_glue_dag",
        trigger_dag_id = "run_DQ_glue_job",
        conf={
            "date_str": "{{ ti.xcom_pull(task_ids='get_metadata', key='metadata')['date_str'] }}",
            "time_str": "{{ ti.xcom_pull(task_ids='get_metadata', key='metadata')['time_str'] }}",
        },
        wait_for_completion= True,
        poke_interval= 30,
        reset_dag_run= True,
        failed_states= ["failed"]
    )

get_metadata >> [trigger_schedule, trigger_weather] >> trigger_glue_job >> trigger_DQ_glue_job