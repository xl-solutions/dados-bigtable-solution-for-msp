import time
import random
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.cloud import bigtable
from google.cloud.bigtable import column_family

import os
from dotenv import load_dotenv

load_dotenv()

# Load variables from .env
PROJECT_ID = os.getenv('GCP_PROJECT_ID')

PUBSUB_TOPIC_ID = os.getenv('PUBSUB_TOPIC_ID')
PUBSUB_SUBSCRIPTION_ID = os.getenv('PUBSUB_SUBSCRIPTION_ID')

CBT_INSTANCE_ID = os.getenv('CBT_INSTANCE_ID')
CBT_CLUSTER_ID = os.getenv('CBT_CLUSTER_ID')
CBT_TABLE_ID = os.getenv('CBT_TABLE_ID')
CBT_LOCATION_ID = os.getenv('CBT_LOCATION_ID')

def create_pubsub_subscription():
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = subscriber.topic_path(PROJECT_ID, PUBSUB_TOPIC_ID)
    subscription_path = subscriber.subscription_path(PROJECT_ID, PUBSUB_SUBSCRIPTION_ID)
    try:
        subscriber.create_subscription(
            request={"name": subscription_path, "topic": topic_path}
        )
        print(f"Created Pub/Sub subscription: {PUBSUB_SUBSCRIPTION_ID}")
    except Exception as e:
        print(f"Subscription '{PUBSUB_SUBSCRIPTION_ID}' already exists or couldn't be created: {e}")

def create_bigtable_table():
    client = bigtable.Client(project=PROJECT_ID, admin=True)
    instance = client.instance(CBT_INSTANCE_ID)
    table = instance.table(CBT_TABLE_ID)

    # Create if not exist
    table = instance.table(CBT_TABLE_ID)
    if not table.exists():
        column_family_id = "sensor_data"
        column_families = {column_family_id: column_family.MaxVersionsGCRule(1)}
        table.create(column_families=column_families)
        print(f"Created BigTable table: {CBT_TABLE_ID}")
    else:
        print(f"BigTable table '{CBT_TABLE_ID}' already exists.")

#### qty msgs
NUM_MESSAGES = 15
TIMEOUT = 3.0

def generate_and_publish_messages():
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC_ID)

    for _ in range(NUM_MESSAGES):
        device_id = random.randint(1, 100)
        temperature = round(random.uniform(20.0, 30.0), 2)
        humidity = round(random.uniform(30.0, 60.0), 2)
        message_data = f"{{'device_id': {device_id}, 'temperature': {temperature}, 'humidity': {humidity}}}"
        future = publisher.publish(topic_path, message_data.encode("utf-8"))
        print(f"Published message to {PUBSUB_TOPIC_ID}: {message_data}")
        
        time.sleep(0.1)

def callback(message):
    print(f"Received message: {message.data}")
    
    message_data = eval(message.data.decode("utf-8"))
    device_id = str(message_data['device_id'])
    temperature = str(message_data['temperature'])
    humidity = str(message_data['humidity'])

    client = bigtable.Client(project=PROJECT_ID, admin=True)
    instance = client.instance(CBT_INSTANCE_ID)
    table = instance.table(CBT_TABLE_ID)
    
    # write bigtable
    row_key = f"device#{device_id}#{int(time.time())}"
    row = table.direct_row(row_key)
    row.set_cell("sensor_data", "temperature", temperature)
    row.set_cell("sensor_data", "humidity", humidity)
    row.commit()
    print(f"Data written to BigTable: {row_key} - Temperature: {temperature}, Humidity: {humidity}")

    # check msg it not redelivered
    message.ack()

def consume_messages_and_write_to_bigtable():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, PUBSUB_SUBSCRIPTION_ID)

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...\n")

    with subscriber:
        try:
            streaming_pull_future.result(timeout=TIMEOUT)
        except TimeoutError:
            streaming_pull_future.cancel()

if __name__ == "__main__":
    # Create Pub/Sub subs and BigTable table
    create_bigtable_table()
    create_pubsub_subscription()

    # Generate data and publish to pubsub
    generate_and_publish_messages()

    # Consume messages and load into BigTable
    consume_messages_and_write_to_bigtable()
