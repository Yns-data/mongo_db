from dst_de_airlines_api.CONNECTION.db_context import mongo_db_connect
from dst_de_airlines_api.CONNECTION.check_database_connection import check_db_connection

def get_all_collection_name():
    check_db_connection()
    return mongo_db_connect.list_collection_names()