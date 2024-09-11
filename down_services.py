from google.cloud import pubsub_v1
from google.cloud import bigtable
from google.cloud.bigtable import enums
from google.cloud.bigquery import Client as BigQueryClient
from google.api_core.exceptions import NotFound

import os
from dotenv import load_dotenv

load_dotenv()

#Load variables from .env
PROJECT_ID = os.getenv('GCP_PROJECT_ID')

PUBSUB_TOPIC_ID = os.getenv('PUBSUB_TOPIC_ID')
PUBSUB_SUBSCRIPTION_ID = os.getenv('PUBSUB_SUBSCRIPTION_ID')

CBT_INSTANCE_ID = os.getenv('CBT_INSTANCE_ID')
CBT_CLUSTER_ID = os.getenv('CBT_CLUSTER_ID')
CBT_TABLE_ID = os.getenv('CBT_TABLE_ID')
CBT_LOCATION_ID = os.getenv('CBT_LOCATION_ID')

BIGQUERY_DATASET_ID = os.getenv('BIGQUERY_DATASET_ID')
BIGQUERY_TABLE_ID = os.getenv('BIGQUERY_TABLE_ID')

def delete_pubsub_topic():
    """Deletes a Pub/Sub topic."""
    # Initialize Pub/Sub client
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC_ID)
    
    # Delete the topic if it exists
    try:
        publisher.delete_topic(request={"topic": topic_path})
        print(f"Deleted Pub/Sub topic: {PUBSUB_TOPIC_ID}")
    except Exception as e:
        print(f"Failed to delete Pub/Sub topic '{PUBSUB_TOPIC_ID}': {e}")

def delete_pubsub_subscription():
    """Deletes a Pub/Sub subscription."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, PUBSUB_SUBSCRIPTION_ID)

    try:
        subscriber.delete_subscription(request={"subscription": subscription_path})
        print(f"Deleted Pub/Sub subscription: {PUBSUB_SUBSCRIPTION_ID}")
    except NotFound:
        print(f"Pub/Sub subscription '{PUBSUB_SUBSCRIPTION_ID}' not found.")

def delete_bigtable_table():
    """Deletes a BigTable table."""
    # Initialize BigTable client
    client = bigtable.Client(project=PROJECT_ID, admin=True)
    instance = client.instance(CBT_INSTANCE_ID)
    table = instance.table(CBT_TABLE_ID)
    
    # Delete the table if it exists
    if table.exists():
        table.delete()
        print(f"Deleted BigTable table: {CBT_TABLE_ID}")
    else:
        print(f"BigTable table '{CBT_TABLE_ID}' does not exist.")

def delete_bigtable_cluster():
    """Deletes a BigTable cluster."""
    # Initialize BigTable client
    client = bigtable.Client(project=PROJECT_ID, admin=True)
    instance = client.instance(CBT_INSTANCE_ID)
    cluster = instance.cluster(CBT_CLUSTER_ID)
    
    # Delete the cluster if it exists
    if cluster.exists():
        cluster.delete()
        print(f"Deleted BigTable cluster: {CBT_CLUSTER_ID}")
    else:
        print(f"BigTable cluster '{CBT_CLUSTER_ID}' does not exist.")

def delete_bigtable_instance():
    """Deletes a BigTable instance."""
    # Initialize BigTable client
    client = bigtable.Client(project=PROJECT_ID, admin=True)
    instance = client.instance(CBT_INSTANCE_ID)
    
    # Delete the instance if it exists
    if instance.exists():
        instance.delete()
        print(f"Deleted BigTable instance: {CBT_INSTANCE_ID}")
    else:
        print(f"BigTable instance '{CBT_INSTANCE_ID}' does not exist.")

def delete_bigquery_table():
    """Deletes a BigQuery table."""
    client = BigQueryClient(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}"

    try:
        client.delete_table(table_id, not_found_ok=True)
        print(f"Deleted BigQuery table: {BIGQUERY_TABLE_ID}")
    except Exception as e:
        print(f"Error deleting BigQuery table '{BIGQUERY_TABLE_ID}': {e}")

def delete_bigquery_dataset():
    """Deletes a BigQuery dataset, including all its contents."""
    client = BigQueryClient(project=PROJECT_ID)
    dataset_id = f"{PROJECT_ID}.{BIGQUERY_DATASET_ID}"

    try:
        client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
        print(f"Deleted BigQuery dataset: {BIGQUERY_DATASET_ID}")
    except Exception as e:
        print(f"Error deleting BigQuery dataset '{BIGQUERY_DATASET_ID}': {e}")

if __name__ == "__main__":
    delete_pubsub_subscription()
    delete_pubsub_topic()

    delete_bigtable_table()
    #delete_bigtable_cluster()
    delete_bigtable_instance()

    delete_bigquery_table()
    delete_bigquery_dataset()
