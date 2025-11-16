import pandas as pd
import json
import psycopg2
from io import StringIO, BytesIO
import requests
import gzip
import os
from dotenv import load_dotenv
from google.cloud import storage

# Load environment variables
print("Loading environment variables...")
load_dotenv()

try:
    # Retrieve MongoDB CSV data from FastAPI endpoint
    print(f"Fetching CSV data from FastAPI at {os.getenv('MONGODB_FASTAPI_GET_CSV_URL')} ...")
    response = requests.get(os.getenv("MONGODB_FASTAPI_GET_CSV_URL"))
    gz_buffer = BytesIO(response.content)

    print("Reading CSV data into DataFrame...")
    with gzip.open(gz_buffer, 'rt') as f:
        df = pd.read_csv(f, low_memory=False)
    print(f"DataFrame loaded with {len(df)} rows and {len(df.columns)} columns.")
except Exception as e:
    print(e)
    df = pd.read_csv("/Workflow_Mongodb_Postgrsql/")

# PostgreSQL configuration
table_name = os.getenv("TABLE_NAME")
postgre_db_config = {
    "dbname": os.getenv("POSTGRES_DB_NAME"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT")
}
print(f"PostgreSQL config loaded. Target table: '{table_name}'")

# Google Cloud Storage configuration
BUCKET_NAME = os.getenv("BUCKET_NAME")
SOURCE_BLOB_NAME = os.getenv("SOURCE_BLOB_NAME")
print(f"Fetching column mapping JSON from GCS bucket '{BUCKET_NAME}', blob '{SOURCE_BLOB_NAME}' ...")

# Load JSON column mapping from GCS
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)
blob = bucket.blob(SOURCE_BLOB_NAME)

json_data = blob.download_as_bytes()
column_mapping = json.load(BytesIO(json_data))
print(f"Column mapping loaded with {len(column_mapping)} entries.")

# Rename DataFrame columns according to the mapping
print("Renaming DataFrame columns...")
df.rename(columns=column_mapping, inplace=True)
print("Columns renamed successfully.")

# Insert DataFrame into PostgreSQL using COPY (high-performance)
print("Connecting to PostgreSQL...")
conn = psycopg2.connect(**postgre_db_config)
cur = conn.cursor()
print("Connection established.")

print("Preparing data for COPY...")
buffer = StringIO()
df.to_csv(buffer, index=False, header=False)  # no header for COPY
buffer.seek(0)

columns = ', '.join(df.columns)
sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV"

print(f"Inserting data into PostgreSQL table '{table_name}' ...")
cur.copy_expert(sql=sql, file=buffer)

conn.commit()
cur.close()
conn.close()
print(f"Insertion completed successfully into table '{table_name}'!")
