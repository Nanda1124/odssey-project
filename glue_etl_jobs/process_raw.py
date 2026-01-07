import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import *

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'date_str', 'time_str'])

def process_trip(spark, date_str, time_str):
  path = f's3://odssey-landing-data/landing_zone/schedule/date={date_str}/time={time_str}/trips.csv'
  schema = StructType(
    [
        StructField('id', IntegerType(), True),
        StructField('route_id', IntegerType(), False),
        StructField('service_id', IntegerType(), False),
        StructField('trip_id', IntegerType(), False),
        StructField('direction_id', IntegerType(), True),
        StructField('shape_id', IntegerType(), True)
    ]
  )
  trips_df = spark.read.csv(path, header=True, schema=schema)
  trips_df_t1 = trips_df.withColumnRenamed("_c0", "id")
  trips_df_t2 = trips_df.withColumns({'trip_headssign': lit(0), 'block_id': lit(0), 'wheelchair_accessible': lit(0), 'bikes_allowed':lit(0), 'loaded_date': current_timestamp()})
  trips_df_t3 = trips_df_t2.drop(col('id'))
  trips_df_t3.show()
  trips_df_t3.printSchema()
  trips_df_t3.write.mode('overwrite').parquet(f's3://odssey-enriched/schedule/date={date_str}/time={time_str}/trips')

def process_route(spark, date_str, time_str):
  path = f's3://odssey-landing-data/landing_zone/schedule/date={date_str}/time={time_str}/routes.csv'
  schema = StructType(
      [
          StructField('id', IntegerType(), True),
          StructField('route_id', IntegerType(), False),
          StructField('route_number', IntegerType(), True),
          StructField('route_name', StringType(), False),
          StructField('route_type', IntegerType(), True),
          StructField('route_color', StringType(), True),
          StructField('route_color_code', StringType(), True),
          StructField('route_sort_order', IntegerType(), True),
          StructField('agency_id', IntegerType(), False)
      ]
  )

  routes_df = spark.read.csv(path, header=True, schema= schema)
  routes_df_t1 = routes_df.drop(col('id'))
  routes_df_t2 = routes_df_t1.withColumn('loaded_date', current_timestamp())
  routes_df_t2.show()
  routes_df_t2.printSchema()
  routes_df_t2.write.mode('overwrite').parquet(f's3://odssey-enriched/schedule/date={date_str}/time={time_str}/routes')

def process_agency(spark, date_str, time_str):
  path = f's3://odssey-landing-data/landing_zone/schedule/date={date_str}/time={time_str}/agency.csv'
  schema = StructType(
      [
          StructField('id', IntegerType(), True),
          StructField('agency_name', StringType(), False),
          StructField('agency_url', StringType(), False),
          StructField('agency_timezone', StringType(), True),
          StructField('agency_lang', StringType(), True),
          StructField('agency_phone', StringType(), True),
          StructField('agency_id', IntegerType(), False)
      ]
  )
  agency_df = spark.read.csv(path, header= True, schema= schema)
  agency_df_t1 = agency_df.drop(col('id'))
  agency_df_t2 = agency_df_t1.withColumn('loaded_date', current_timestamp())
  agency_df_t2.show()
  agency_df_t2.printSchema()
  agency_df_t2.write.mode('overwrite').parquet(f's3://odssey-enriched/schedule/date={date_str}/time={time_str}/agency')

def process_calendar(spark, date_str, time_str):
  path = f's3://odssey-landing-data/landing_zone/schedule/date={date_str}/time={time_str}/calendar.csv'
  schema = StructType(
      [
          StructField('id', IntegerType(), True),
          StructField('service_id', IntegerType(), False),
          StructField('monday', IntegerType(), False),
          StructField('tuesday', IntegerType(), False),
          StructField('wednesday', IntegerType(), False),
          StructField('thursday', IntegerType(), False),
          StructField('friday', IntegerType(), False),
          StructField('saturday', IntegerType(), False),
          StructField('sunday', IntegerType(), False),
          StructField('start_date', StringType(), False),
          StructField('end_date', StringType(), False),

      ]
  )
  calendar_df = spark.read.csv(path, header= True, schema= schema)
  calendar_df.show()
  calendar_df_t1 = calendar_df.withColumn('start_date', to_date(col('start_date'), 'yyyyMMdd'))
  calendar_df_t2 = calendar_df_t1.withColumn('end_date', to_date(col('end_date'), 'yyyyMMdd'))
  calendar_df_t3 = calendar_df_t2.drop(col('id'))
  calendar_df_t4 = calendar_df_t3.withColumn('loaded_date', current_timestamp())
  calendar_df_t4.show()
  calendar_df_t4.printSchema()
  calendar_df_t4.write.mode('overwrite').parquet(f's3://odssey-enriched/schedule/date={date_str}/time={time_str}/calendar')

def process_stop_time(spark, date_str, time_str):
  path = f's3://odssey-landing-data/landing_zone/schedule/date={date_str}/time={time_str}/stop_times.csv'
  schema = StructType(
      [
          StructField('id', IntegerType(), True),
          StructField('trip_id', IntegerType(), True),
          StructField('arrival_time', StringType(), True),
          StructField('departure_time', StringType(), True),
          StructField('stop_id', IntegerType(), True),
          StructField('stop_sequence', IntegerType(), True),
          StructField('time_point', IntegerType(), True),
      ]
  )
  stop_time_df = spark.read.csv(path, header=True, schema= schema)
  stop_time_df_t1 = stop_time_df.drop(col('id'))
  stop_time_df_t2 = stop_time_df_t1.withColumn('loaded_date', current_timestamp())
  stop_time_df_t2.show()
  stop_time_df_t2.printSchema()
  stop_time_df_t2.write.mode('overwrite').parquet(f's3://odssey-enriched/schedule/date={date_str}/time={time_str}/stop_times')

def process_stop(spark, date_str, time_str):
  path = f's3://odssey-landing-data/landing_zone/schedule/date={date_str}/time={time_str}/stops.csv'
  schema = StructType(
      [
          StructField('id', StringType(), True),
          StructField('stop_id', IntegerType(), True),
          StructField('stop_name', StringType(), True),
          StructField('stop_lat', StringType(), True),
          StructField('stop_lon', StringType(), True)
      ]
  )

  stop_df = spark.read.csv(path, header=True, schema=schema)
  stop_df_t1 = stop_df.drop(col('id'))
  stop_df_t2 = stop_df_t1.withColumn('loaded_date', current_timestamp())
  stop_df_t2.show()
  stop_df_t2.write.mode('overwrite').parquet(f's3://odssey-enriched/schedule/date={date_str}/time={time_str}/stops')

def process_calendar_dates(spark, date_str, time_str):
  path = f's3://odssey-landing-data/landing_zone/schedule/date={date_str}/time={time_str}/calendar_dates.csv'
  schema = StructType(
      [
          StructField('id', IntegerType(), True),
          StructField('service_id', IntegerType(), False),
          StructField('exception_date', StringType(), True),
          StructField('exception_type', IntegerType(), True)
      ]
  )
  calendar_dates_df = spark.read.csv(path, header=True, schema=schema)
  calendar_dates_df_t1 = calendar_dates_df.withColumn('exception_date', to_date(col('exception_date'), 'yyyyMMdd'))
  calendar_dates_df_t2 = calendar_dates_df_t1.withColumn('loaded_date', current_timestamp())
  calendar_dates_df_t2.show()
  calendar_dates_df_t2.printSchema()
  calendar_dates_df_t2.write.mode('overwrite').parquet(f's3://odssey-enriched/schedule/date={date_str}/time={time_str}/calendar_dates')

def process_weather(spark, date_str, time_str):
    path = f's3://odssey-landing-data/landing_zone/weather/date={date_str}/time={time_str}/weather.csv'
    weather_df = spark.read.csv(path, header = True, inferSchema= True)
    weather_df.show()
    weather_df.printSchema()
    weather_df.write.mode('overwrite').parquet(f"s3://odssey-enriched/weather/date={date_str}/time={time_str}")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
date_str = args['date_str']
time_str = args['time_str']
try:
    process_trip(spark, date_str, time_str)
    process_route(spark, date_str, time_str)
    process_agency(spark, date_str, time_str)
    process_calendar(spark, date_str, time_str)
    process_stop_time(spark, date_str, time_str)
    process_stop(spark, date_str, time_str)
    process_calendar_dates(spark, date_str, time_str)
    process_weather(spark, date_str, time_str)
except Exception as e:
    print(f'something went wrong: {e}')
job.commit()

