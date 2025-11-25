from workflow_mongodb_postgresql_functions.utilities import get_dataframe_from_mongodb, load_postgres_config, copy_dataframe_to_postgres, run_sql_from_string
from workflow_mongodb_postgresql_functions.sql_requests import SQL_QUERIES
from workflow_mongodb_postgresql_functions.gcp_logger import logger
import json
from io import BytesIO
from google.cloud import storage
import os
from dotenv import load_dotenv
load_dotenv()



if __name__=="__main__":
    # loading inputs 
    
    table_name, postgre_db_config = load_postgres_config()
    
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    SOURCE_BLOB_NAME = os.getenv("MAPPING_FILE_BLOB_NAME")
    logger.info(f"Fetching column mapping JSON from GCS bucket '{BUCKET_NAME}', blob '{SOURCE_BLOB_NAME}' ...")
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(SOURCE_BLOB_NAME)
    

    json_data = blob.download_as_bytes()
    column_mapping = json.load(BytesIO(json_data))
    logger.info(f"Column mapping loaded with {len(column_mapping)} entries.")

    # df = get_dataframe_from_mongodb()
    import pandas as pd 
    df = pd.read_csv("Workflow_Mongodb_Postgrsql/afklm_removed_sch_flight_from_mongo_filtered_20251117-12-15-18.csv.gz")

    # Rename DataFrame columns according to the mapping
    logger.info("Renaming DataFrame columns...")
    df.rename(columns=column_mapping, inplace=True)
    logger.info("Columns renamed successfully.")
    # insertion of data
    try:
        copy_dataframe_to_postgres(df, table_name, postgre_db_config)
    except Exception as e:
        raise Exception(f"An Error has occured while copying mongodb data into postgre Sql database: {e}")
    
    # executing transformation requests
    try:
        run_sql_from_string(SQL_QUERIES, postgre_db_config)
    except Exception as e:
        raise Exception(f"An Error has occured while executing SQL queries: {e}")


