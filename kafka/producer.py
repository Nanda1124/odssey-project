from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests, logging
import pandas as pd
from confluent_kafka import Producer
import json
import time
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict

default_args = {
    "owner": "Nanda",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def get_bus_position():
  conf = {'bootstrap.servers': 'localhost:9092'}
  producer = Producer(conf)
  topic = 'realtimeBusUpdates'
  response = requests.get('https://www.zet.hr/gtfs-rt-protobuf')
  if response.status_code != 200:
    logging.error(msg = f'not able to fetch the data: {response.conetent}')
  else:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    feed_dict = MessageToDict(feed)
    data = {}
    combined_bus_data = {}
    for entity in feed_dict['entity']:
      trip_instance_id = entity['id'].split('_')[0]

      # Initialize an entry for this trip_instance_id if it doesn't exist
      if trip_instance_id not in combined_bus_data:
          combined_bus_data[trip_instance_id] = {
              'bus_id': trip_instance_id, # This is the trip instance ID
              'trip_id': None,
              'route_id': None,
              'startDate': None,
              'latitude': None,
              'longitude': None,
          }

      if 'tripUpdate' in entity:
        # Update trip-related information
        combined_bus_data[trip_instance_id]['trip_id'] = entity['tripUpdate']['trip']['tripId']
        combined_bus_data[trip_instance_id]['route_id'] = entity['tripUpdate']['trip']['routeId']
        combined_bus_data[trip_instance_id]['startDate'] = entity['tripUpdate']['trip']['startDate']
        # bus_id is already set as trip_instance_id

      if 'vehicle' in entity:
        # Update vehicle position and actual vehicle ID information
        combined_bus_data[trip_instance_id]['latitude'] = entity['vehicle']['position']['latitude']
        combined_bus_data[trip_instance_id]['longitude'] = entity['vehicle']['position']['longitude']
        # bus_id is already set as trip_instance_id

    print(f'finished processing data for {len(combined_bus_data)} unique trip instances.')

    # Filter out entries that don't have latitude and longitude, as they represent buses without real-time position
    buses_with_positions = {
        k: v for k, v in combined_bus_data.items()
        if v['latitude'] is not None and v['longitude'] is not None
    }

    if not buses_with_positions:
        logging.warning("No buses with real-time position data found to send.")
    else:
        producer.produce(topic, json.dumps(buses_with_positions).encode('utf-8'))
        print(f'Sent data for {len(buses_with_positions)} buses with positions.')

        # Print info about the last processed bus_id (for debugging/confirmation)
        if buses_with_positions:
            last_bus_id_sent = list(buses_with_positions.keys())[-1]
            print(f'Last bus_id processed and sent: {last_bus_id_sent}')
        else:
            print(f'No bus data with positions to send.')

        producer.flush()
    print(f'Process done, total of {len(buses_with_positions)} bus positions sent to Kafka.')

with DAG(
    dag_id="get_realtime",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="Ingest weather data",
    schedule= "0 * * * * *"
) as dag:

    get_bus_live_position = PythonOperator(
        task_id="get_live_bus_position",
        python_callable=get_bus_position,
    )

get_bus_live_position