from confluent_kafka import Consumer, KafkaError
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import json

conn = snowflake.connector.connect(
    user='NANDADE',
    password='Nanda.2002@12315',
    account='OJGSMPD-VNC58167',
    warehouse='ODSSEY_WAREHOUSE',
    database='real_time',
    schema='position_updates'
)

# Consumer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest'  # Start reading from the beginning of the topic if no offset is committed
}

consumer = Consumer(conf)

# Subscribe to a topic (or a list of topics)
topic = 'realtimeBusUpdates'
consumer.subscribe([topic])

try:
    while True:
        # Poll for messages with a timeout of 1.0 second
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event - not an error, just informational
                print(f'Reached end of topic {msg.topic()} partition {msg.partition()} at offset {msg.offset()}')
            elif msg.error():
                # Other error
                print(msg.error())
        else:
            print(f"Received message from topic {msg.topic()}: {msg.value().decode('utf-8')}")
            msg_dict = json.loads(msg.value().decode('utf-8'))
            data_list = []
            for bus_id in msg_dict:
              data_list.append(msg_dict[bus_id])
            data = pd.DataFrame(data_list).dropna()
            success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=data,
            table_name='bus_updates',
            auto_create_table=True,
            quote_identifiers=False
            )

            print(f"Success: {success}, Rows Loaded: {nrows}")


except KeyboardInterrupt:
    pass

finally:
    # Close down the consumer to commit final offsets.
    consumer.close()
