from .gcp_logger import logger
import pandas as pd
import psycopg2
from io import StringIO, BytesIO
import requests
import gzip
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def get_dataframe_from_mongodb(date: str = None):
    try:
        # Retrieve MongoDB CSV data from FastAPI endpoint
        logger.info(f"Fetching CSV data from FastAPI at {os.getenv('MONGODB_FASTAPI_GET_CSV_URL')} ...")
        response = requests.get(os.getenv("MONGODB_FASTAPI_GET_CSV_URL")+date)
        gz_buffer = BytesIO(response.content)
        logger.info("Reading CSV data into DataFrame...")
        with gzip.open(gz_buffer, 'rt') as f:
            df = pd.read_csv(f, low_memory=False)
        return df
    except Exception as e:
        logger.error(f"An Error has occured while fetching data from MongoDB API: {e}")
        raise e


        


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
    logger.info(f"PostgreSQL config loaded. Target table: '{table_name}'")
    return table_name, postgre_db_config
# PostgreSQL insertion
def copy_dataframe_to_postgres(df:pd.DataFrame, table_name:str, postgre_db_config:dict):
    """
    Insert a pandas DataFrame into a PostgreSQL table using COPY and context managers.
    """
    logger.info("Preparing data for COPY...")
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    columns = ', '.join(df.columns)
    sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV"

    logger.info("Connecting to PostgreSQL...")
    with psycopg2.connect(**postgre_db_config) as conn:
        with conn.cursor() as cur:
            logger.info("Connection established.")
            logger.info(f"Inserting data into PostgreSQL table '{table_name}' ...")

            cur.copy_expert(sql=sql, file=buffer)

    logger.info("Data inserted successfully.")

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

                logger.info("Running SQL string...")
                
                statements = sql_string.split(";")

                for stmt in statements:
                    stmt = stmt.strip()
                    if stmt:
                        try:
                            logger.info(f"Executing: {stmt[:80]}...")
                            cur.execute(stmt)
                        except Exception as e:
                            logger.error(f"SQL execution failed: {e}")

    finally:
        logger.info("SQL execution finished.")