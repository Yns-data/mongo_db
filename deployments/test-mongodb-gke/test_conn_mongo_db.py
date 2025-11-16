from pymongo import MongoClient, errors

MONGO_HOST = "34.154.65.11"
MONGO_PORT = 27017
MONGO_USER = "airlines"
MONGO_PASS = "airlines"
MONGO_DB   = "airlines_test"

try:
    client = MongoClient(
        host=MONGO_HOST,
        port=MONGO_PORT,
        username=MONGO_USER,
        password=MONGO_PASS,
        serverSelectionTimeoutMS=5000  # Timeout de 5 secondes
    )
    
    # Tester la connexion
    client.server_info()
    db = client[MONGO_DB]
    print("Connexion réussie ! Collections disponibles :", db.list_collection_names())

except errors.ServerSelectionTimeoutError:
    print(f"Impossible de se connecter à MongoDB sur {MONGO_HOST}:{MONGO_PORT}")
except errors.OperationFailure as e:
    print(f"Erreur d'authentification : {e}")
