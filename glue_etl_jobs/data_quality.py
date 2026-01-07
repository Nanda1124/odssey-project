import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import boto3
import json

s3_client = boto3.client('s3', 'us-east-1')

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'date_str', 'time_str'])


def data_quality_check(spark, file_name, file_metadata, date_str, time_str):
  dq_res = {}
  pk_list = file_metadata['pk']
  # creating data frame
#   path = f'/content/silver/{date_str}/{time_str}/{file_name}'
  path = f's3://odssey-enriched/schedule/date={date_str}/time={time_str}/{file_name}'
  df = spark.read.parquet(path)
  # row count
  dq_res['row_count'] = df.count()
  # PK unique count
  pk_unique_count = df.select(*pk_list).distinct().count()
  if pk_unique_count == dq_res['row_count']:
    dq_res['pk_unique'] = 'Pass'
  else:
    dq_res['pk_unique'] = 'Fail'
    # raise Exception(f'PK Unique constraint Failed ! {file_name}')
  # schema validation
  given_columns_count = len(file_metadata['required_columns'])
  columns_diff = len(list(df.columns))-given_columns_count
  if columns_diff == 0:
    dq_res['schema'] = 'Pass'
  else:
    dq_res['schema'] = 'Fail'
    # raise Exception(f'schema validation Failed ! {file_name}')

  # Null constraint check
  i = 0
  for column in file_metadata['null_con_columns']:
    null_count = df.filter(col(column).isNull()).count()
    if null_count > 0:
      dq_res['null_constraint'] = 'Fail'
    #   raise Exception(f'null constraint Failed !{column}, {file_name}')
      break
    i+=1
  if i == len(file_metadata['null_con_columns']):
    dq_res['null_constraint'] = 'Pass'
  logging.info(f"processed {file_name}: result: {dq_res}")

  return dq_res

try:
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    date_str = args['date_str']
    time_str = args['time_str']
    file_list = {
        'agency': {
            'pk': ['agency_id'],          # pk unique validation & null check
            'required_columns': ['agency_name', 'agency_url', 'agency_timezone', 'agency_lang', 'agency_phone', 'agency_id'],         # schema validation
            'null_con_columns': ['agency_id', 'agency_name'],         # null check
            'values_validation': {}
            },
        'calendar': {
            'pk': ['service_id'],
            'required_columns': ['service_id', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', 'start_date', 'end_date'],
            'null_con_columns': ['service_id', 'start_date', 'end_date'],
            'values_validation': {
                'limit': {},
                'constant_value': {}
            }
            },
        'route': {
            'pk': ['route_id'],
            'required_columns': ['route_id', 'route_number', 'route_name', 'route_type', 'route_color', 'route_color_code', 'route_sort_order', 'agency_id'],
            'null_con_columns': ['route_id', 'agency_id', 'route_type'],
            'values_validation': {}
            },
        'stop_time': {
            'pk': ['trip_id', 'stop_sequence'],
            'required_columns': ['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence', 'timepoint'],
            'null_con_columns': ['trip_id', 'stop_id', 'stop_sequence' ],
            'values_validation': {}
            },
        'trips': {
            'pk':['trip_id'],
            'required_columns': ['route_id', 'service_id', 'trip_id', 'direction_id', 'shape_id', 'trip_headssign', 'block_id', 'wheelchair_accessible', 'bikes_allowed'],
            'null_con_columns': ['route_id', 'service_id', 'trip_id', 'direction_id', 'block_id'],
            'values_validation': {}
            },
        'stop': {
            'pk':['stop_id'],
            'required_columns': ['stop_id', 'stop_name', 'stop_lat', 'stop_lon'],
            'null_con_columns': ['stop_id', 'stop_name'],
            'values_validation': {}
            },
        'calendar_dates': {
            'pk':['service_id', 'exception_date'],
            'required_columns': ['id', 'service_id', 'exception_date', 'exception_type'],
            'null_con_columns': ['id', 'service_id'],
            'values_validation': {}
            }
      }
    dq_result = {}
    for file_name in file_list:
      dq_result[file_name] = data_quality_check(spark, file_name,file_list[file_name] ,date_str, time_str)
    
    response = s3_client.put_object(
        Bucket = 'odssey-enriched',
        Key = f'data_quality/date={date_str}/time={time_str}/data_checks.json',
        Body = json.dumps(dq_result),
        ContentType = 'application/json'
        )
    logging.info(f'response: {response}')

except Exception as e:
    logging.error(f'Data checks job failed with an error: {e}')

finally:
    job.commit()