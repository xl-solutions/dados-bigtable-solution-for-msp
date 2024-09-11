from google.cloud import pubsub_v1
from google.cloud import bigtable
from google.cloud.bigtable import enums
from google.cloud.bigtable import column_family

import os
from dotenv import load_dotenv

load_dotenv()

#Load variables from .env
PROJECT_ID = os.getenv('GCP_PROJECT_ID')

PUBSUB_TOPIC_ID = os.getenv('PUBSUB_TOPIC_ID')

CBT_INSTANCE_ID = os.getenv('CBT_INSTANCE_ID')
CBT_CLUSTER_ID = os.getenv('CBT_CLUSTER_ID')
CBT_TABLE_ID = os.getenv('CBT_TABLE_ID')
CBT_LOCATION_ID = os.getenv('CBT_LOCATION_ID')

def create_pubsub_topic():
    """Creates a Pub/Sub topic if it doesn't already exist."""
    # Initialize Pub/Sub client
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC_ID)
    
    # Check if the topic already exists
    try:
        topic = publisher.get_topic(request={"topic": topic_path})
        print(f"Topic '{PUBSUB_TOPIC_ID}' already exists: {topic.name}")
    except Exception as e:
        # Topic does not exist; create it
        topic = publisher.create_topic(request={"name": topic_path})
        print(f"Created Pub/Sub topic: {topic.name}")

def create_bigtable_instance_and_table():
    """Creates a BigTable instance and table if they don't exist."""
    # Initialize BigTable client
    client = bigtable.Client(project=PROJECT_ID, admin=True)

    # Set up the BigTable instance
    instance = client.instance(
        CBT_INSTANCE_ID,
        instance_type=enums.Instance.Type.PRODUCTION,
        labels={"env": "test"}
    )

    cluster = instance.cluster(
        cluster_id=CBT_CLUSTER_ID,
        location_id=CBT_LOCATION_ID,
        serve_nodes=1,
        default_storage_type=enums.StorageType.SSD,
    )
    
    if not instance.exists():
        print("\nCreating an instance")
        # Create instance with given options
        operation = instance.create(clusters=[cluster])
        # Ensure the operation completes.
        operation.result(timeout=480)
        print("\nCreated instance: {}".format(instance))
    else:
        print(f"BigTable instance '{CBT_INSTANCE_ID}' already exists.")
    
    # Create a table within the instance if it doesn't exist
    #table = instance.table(CBT_TABLE_ID)
    #if not table.exists():
    #    column_family_id = "sensor_data"
    #    column_families = {column_family_id: column_family.MaxVersionsGCRule(1)}
    #    table.create(column_families=column_families)
    #    print(f"Created BigTable table: {CBT_TABLE_ID}")
    #else:
    #    print(f"BigTable table '{CBT_TABLE_ID}' already exists.")
    
if __name__ == "__main__":
    create_pubsub_topic()
    create_bigtable_instance_and_table()

