from pymongo import MongoClient

# Remplace par ton username, password et EXTERNAL-IP
MONGO_USER = "admin"
MONGO_PASS = "airlines"
MONGO_HOST = "34.154.65.11"  # EXTERNAL-IP de ton LoadBalancer
MONGO_PORT = 27017

# Connexion à MongoDB
uri = "mongodb://airlines:airlines@34.154.65.11:27017/admin"
client = MongoClient(uri)

# Choisir la base et la collection
db = client["airlines_test"]
collection = db.test_collection

# Insérer un document
# doc = {"msg": "test persistence"}
# insert_result = collection.insert_one(doc)
# print(f"Document inséré avec _id: {insert_result.inserted_id}")

# Lire les documents
for d in collection.find():
    print(d)
