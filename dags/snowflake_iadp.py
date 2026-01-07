import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

args = {"owner": "Nanda", "start_date": datetime(2021,3,22,17,15)}

def load_iadp_tables(**context):
  iadp_load_queries = [
      """
      INSERT INTO odssey_curated.curated.dim_routes
        SELECT
        md5(CONCAT(r.route_id, a.agency_id))::STRING AS dim_route_id,
        r.route_id INT,
        r.route_number INT,
        r.route_name STRING,
        r.route_type INT,
        r.route_color STRING,
        r.route_color_code STRING,
        r.route_sort_order INT,
        a.agency_name STRING,
        a.agency_url STRING,
        a.agency_timezone STRING,
        a.agency_lang STRING,
        a.agency_phone STRING,
        a.agency_id INT,
        a.loaded_date DATETIME
    FROM odssey_enriched.consumption.routes r
    LEFT JOIN odssey_enriched.consumption.agency a
    ON r.agency_id = a.agency_id;""",
    """
    INSERT INTO odssey_curated.curated.fact_trips
    SELECT
        md5(CONCAT(trip_id,route_id, service_id)) AS fact_trips_id,
        trip_id INTEGER,
        route_id INTEGER,
        service_id INTEGER,
        direction_id INTEGER,
        shape_id INTEGER,
        trip_headsign STRING,
        block_id STRING,
        loaded_date DATETIME
    FROM odssey_enriched.consumption.trips;""",
    """
    INSERT INTO odssey_curated.curated.dim_calendar
    SELECT
        md5(concat(c.service_id, cd.exception_date)) AS dim_calendar_id,
        c.service_id INT,
        c.monday INT,
        c.tuesday INT,
        c.wednesday INT,
        c.thursday INT,
        c.friday INT,
        c.saturday INT,
        c.sunday INT,
        c.start_date DATE,
        c.end_date DATE,
        cd.exception_date DATE,
        cd.exception_type INT,
        cd.loaded_date DATETIME
    FROM odssey_enriched.consumption.calendar c
    LEFT JOIN odssey_enriched.consumption.calendar_dates cd
    ON c.service_id = cd.service_id;""",
    """
    INSERT INTO odssey_curated.curated.dim_stops
    SELECT
        md5(concat(s.stop_id, st.arrival_time, st.trip_id))::STRING AS dim_stops_id,
        s.stop_id INT,
        s.stop_name STRING,
        s.stop_lat STRING,
        s.stop_lon STRING,
        st.trip_id INT,
        st.arrival_time STRING,
        st.departure_time STRING,
        st.stop_sequence INT,
        st.time_point INT,
        st.loaded_date DATETIME
    FROM odssey_enriched.consumption.stops s
    LEFT JOIN odssey_enriched.consumption.stop_times st
    ON s.stop_id = st.stop_id;"""
  ]
  truncate_queries = [
      "TRUNCATE TABLE odssey_curated.curated.dim_calendar;",
      "TRUNCATE TABLE odssey_curated.curated.dim_routes;",
      "TRUNCATE TABLE odssey_curated.curated.dim_stops;",
      "TRUNCATE TABLE odssey_curated.curated.fact_trips;"
  ]
  hook = SnowflakeHook(snowflake_conn_id='odssey_snowflake_conn')
  hook.run(truncate_queries)
  hook.run(iadp_load_queries)

with DAG(
    dag_id="iadp_load_data_snowflake",
    default_args=args,
) as dag:
  execute_query = PythonOperator(
      task_id= 'execute_query',
      python_callable= load_iadp_tables
  )



execute_query