import requests
import logging
import json
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

def set_logger():
  root = logging.getLogger()
  for handlers in root.handlers[:]:
    root.removeHandler(handlers)
  logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
  return logging

def process_response(response_json):
  logging.info(response_json)
  # columns: dt, date, time, country, city, temp, feels_like, pressure, humidity, sea_level, grnd_level, wind_speed, wind_deg, wind_gust
  # 2 tables: cities table, weather_info
  # 3 cities table columns: id, name, country, latitude, longitude, sunrise, sunset
  weather_data = {}
  datetime_list = str(datetime.now()).split(' ')
  weather_data['date'] = datetime_list[0]
  weather_data['time_ust'] = datetime_list[1].split('.')[0]
  weather_data['city_id'] = response_json['id']
  weather_data['temperature'] = response_json['main']['temp']
  weather_data['temp_feel_like'] = response_json['main']['feels_like']
  weather_data['pressure'] = response_json['main']['pressure']
  weather_data['humidity'] = response_json['main']['humidity']
  weather_data['sea_level'] = response_json['main']['sea_level']
  weather_data['grnd_level'] = response_json['main']['grnd_level']
  weather_data['wind_speed'] = response_json['wind']['speed']
  weather_data['wind_deg'] = response_json['wind']['deg']
  weather_data['weather'] = response_json['weather'][0]['description']
  weather_data_list = list()
  weather_data_list.append(weather_data)
  df_one = pd.DataFrame(data=weather_data_list)
  logging.info(df_one.head())
  df_one.to_csv('weather_data.csv')
  return


def get_weather_data():
  logging = set_logger()
  city = 'Zagreb'
  api_token = "853e0eec7c1f10d8e9c7d5bcc3abb7c8"
  open_weather_url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&units=imperial&appid={api_token}"
  response = requests.get(open_weather_url)
  if response.status_code == 200:
    logging.info("Weather data request successful")
    logging.info(f"Status Code: {response.status_code}")
    process_response(response.json())
  else:
    logging.error("Not able to fetch weather data")
    logging.error(response.content)

# logging = set_logger()
# get_weather_data('Zagreb',logging)

default_args = {
    'owner': 'Nanda',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'get_weather_data',
    default_args=default_args,
    description='fetching the latest weather data'
)

get_weather = PythonOperator(
    task_id='get_latest_weather_data',
    python_callable= get_weather_data,
    dag=dag,
)
get_weather