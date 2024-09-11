from google.cloud import bigtable
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
import pandas as pd

import os
from dotenv import load_dotenv

load_dotenv()

#Load variables from .env
PROJECT_ID = os.getenv('GCP_PROJECT_ID')

CBT_INSTANCE_ID = os.getenv('CBT_INSTANCE_ID')
CBT_TABLE_ID = os.getenv('CBT_TABLE_ID')
COLUMN_FAMILY_ID = "sensor_data"

BIGQUERY_DATASET_ID = os.getenv('BIGQUERY_DATASET_ID')
BIGQUERY_TABLE_ID = os.getenv('BIGQUERY_TABLE_ID')

def extract_data_from_bigtable():
    client = bigtable.Client(project=PROJECT_ID, admin=True)
    instance = client.instance(CBT_INSTANCE_ID)
    table = instance.table(CBT_TABLE_ID)

    rows = table.read_rows()
    rows.consume_all()

    data = []
    for row_key, row_data in rows.rows.items():
        row_key_decoded = row_key.decode("utf-8")
        print(f"Processing row: {row_key_decoded}")

        for family, columns in row_data.cells.items():
            #print(f"  Column Family: {family}")
            for column, cells in columns.items():
                if "temperature" in str(column):
                    temperature = cells[0].value.decode('utf-8')
                elif "humidity" in str(column):
                    humidity = cells[0].value.decode('utf-8')

                #print(f"    Column: {column}, Value: {cells[0].value.decode('utf-8')}")

        try:
            device_id, timestamp = row_key_decoded.split("#")[1:]
        except ValueError:
            print(f"Invalid row key format: {row_key_decoded}")
            continue

        data.append({
            "device_id": int(device_id),
            "timestamp": int(timestamp),
            "temperature": temperature,
            "humidity": humidity
        })

    df = pd.DataFrame(data)
    print("Extracted data from Bigtable:")
    print(df.head())
    return df

def create_bigquery_dataset():
    client = bigquery.Client(project=PROJECT_ID)
    dataset_id = f"{PROJECT_ID}.{BIGQUERY_DATASET_ID}"
    
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"

    try:
        client.create_dataset(dataset, timeout=30)
        print(f"Created BigQuery dataset: {BIGQUERY_DATASET_ID}")
    except Exception as e:
        print(f"Dataset '{BIGQUERY_DATASET_ID}' already exists or couldn't be created: {e}")

def create_bigquery_table():
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}"

    schema = [
        SchemaField("device_id", "INTEGER", mode="REQUIRED"),
        SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
        SchemaField("temperature", "STRING", mode="NULLABLE"),
        SchemaField("humidity", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id, schema=schema)

    try:
        client.create_table(table)
        print(f"Created BigQuery table: {BIGQUERY_TABLE_ID}")
    except Exception as e:
        print(f"Table '{BIGQUERY_TABLE_ID}' already exists or couldn't be created: {e}")
        truncate_bigquery_table()

def truncate_bigquery_table():
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}"

    query = f"TRUNCATE TABLE `{table_id}`"
    query_job = client.query(query)
    query_job.result()  # Wait for the job to complete
    print(f"Truncated BigQuery table: {BIGQUERY_TABLE_ID}")

def load_data_to_bigquery(df):
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}"

    job = client.load_table_from_dataframe(df, table_id)
    job.result()

    print(f"Loaded {job.output_rows} rows into {BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}.")

if __name__ == "__main__":
    # Extract data from CBT
    df = extract_data_from_bigtable()

    # Create BQ dataset and table
    create_bigquery_dataset()
    create_bigquery_table()

    # Load data to BQ
    load_data_to_bigquery(df)
