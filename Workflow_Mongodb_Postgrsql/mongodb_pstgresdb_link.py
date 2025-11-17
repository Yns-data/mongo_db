import pandas as pd
import json
import psycopg2
from io import StringIO, BytesIO
import requests
import gzip
import os
from dotenv import load_dotenv
from google.cloud import storage
from sql_requests import SQL_QUERIES

# Load environment variables
print("Loading environment variables...")
load_dotenv()

def get_dataframe_from_mongodb(date:str):
    try:
        # Retrieve MongoDB CSV data from FastAPI endpoint
        print(f"Fetching CSV data from FastAPI at {os.getenv('MONGODB_FASTAPI_GET_CSV_URL')} ...")
        response = requests.get(os.getenv("MONGODB_FASTAPI_GET_CSV_URL")+date)
        gz_buffer = BytesIO(response.content)

        print("Reading CSV data into DataFrame...")
        with gzip.open(gz_buffer, 'rt') as f:
            df = pd.read_csv(f, low_memory=False)
        print(f"DataFrame loaded with {len(df)} rows and {len(df.columns)} columns.")
        return df
    except Exception as e:
        raise Exception("Error occurred while loading data from mongodb")
        


# PostgreSQL configuration
def load_postgres_config():
    table_name = os.getenv("TABLE_NAME")
    postgre_db_config = {
        "dbname": os.getenv("POSTGRES_DB_NAME"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST"),
        "port": int(os.getenv("POSTGRES_PORT"))
    }
    print(f"PostgreSQL config loaded. Target table: '{table_name}'")
    return table_name, postgre_db_config
# PostgreSQL insertion
def copy_dataframe_to_postgres(df:pd.DataFrame, table_name:str, postgre_db_config:dict):
    """
    Insert a pandas DataFrame into a PostgreSQL table using COPY and context managers.
    """
    print("Preparing data for COPY...")
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    columns = ', '.join(df.columns)
    sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV"

    print("Connecting to PostgreSQL...")
    with psycopg2.connect(**postgre_db_config) as conn:
        with conn.cursor() as cur:
            print("Connection established.")
            print(f"Inserting data into PostgreSQL table '{table_name}' ...")

            cur.copy_expert(sql=sql, file=buffer)

    print("Data inserted successfully.")

def run_sql_from_string(sql_string:str, postgre_db_config:dict):
    """
    Execute multiple SQL statements from a string using psycopg2
    with automatic connection and cursor management.
    """

    try:
        with psycopg2.connect(
            **postgre_db_config
        ) as conn:

            with conn.cursor() as cur:

                print("[INFO] Running SQL string...")
                
                statements = sql_string.split(";")

                for stmt in statements:
                    stmt = stmt.strip()
                    if stmt:
                        try:
                            print(f"[INFO] Executing: {stmt[:80]}...")
                            cur.execute(stmt)
                        except Exception as e:
                            print(f"[ERROR] SQL execution failed: {e}")

    finally:
        print("[INFO] PostgreSQL operations finished.")


if __name__=="__main__":
    # loading inputs 
    
    table_name, postgre_db_config = load_postgres_config()
    
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    SOURCE_BLOB_NAME = os.getenv("MAPPING_FILE_BLOB_NAME")
    print(f"Fetching column mapping JSON from GCS bucket '{BUCKET_NAME}', blob '{SOURCE_BLOB_NAME}' ...")
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(SOURCE_BLOB_NAME)
    

    json_data = blob.download_as_bytes()
    column_mapping = json.load(BytesIO(json_data))
    print(f"Column mapping loaded with {len(column_mapping)} entries.")

    df = get_dataframe_from_mongodb(date="20251114-15-30-45")
    # df = pd.read_csv("Workflow_Mongodb_Postgrsql/afklm_removed_sch_flight_from_mongo_filtered_20251117-12-15-18.csv.gz")

    # Rename DataFrame columns according to the mapping
    print("Renaming DataFrame columns...")
    df.rename(columns=column_mapping, inplace=True)
    print("Columns renamed successfully.")
    # insertion of data
    try:
        copy_dataframe_to_postgres(df, table_name, postgre_db_config)
    except Exception as e:
        raise Exception("An Error has occured while copying mongodb data into postgre Sql database")
    
    # executing transformation requests
    run_sql_from_string(SQL_QUERIES, postgre_db_config)


