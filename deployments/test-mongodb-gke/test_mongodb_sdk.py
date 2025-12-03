from pymongo import MongoClient
from datetime import datetime

def init_mongodb():
    # Connexion à MongoDB (modifier si besoin)
    uri = "mongodb://airlines:airlines@34.32.126.15:27017/admin"
    client = MongoClient(uri)

    # Nom de la base
    db = client["test_persistence_db"]

    # Collections
    users = db["users"]
    products = db["products"]
    orders = db["orders"]

    # Nettoyer avant insertion (utile pour les tests)
    # users.drop()
    # products.drop()
    # orders.drop()

    # Données de test
    users_data = [
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
    ]

    products_data = [
        {"name": "Laptop", "price": 1200, "stock": 10},
        {"name": "Smartphone", "price": 800, "stock": 25},
    ]

    orders_data = [
        {"user": "Alice", "product": "Laptop", "date": datetime.now()},
        {"user": "Bob", "product": "Smartphone", "date": datetime.now()},
    ]

    # Inserer dans MongoDB
    users.insert_many(users_data)
    products.insert_many(products_data)
    orders.insert_many(orders_data)

    print("Base de données et collections créées avec succès !\n")

    print("Users:")
    for u in users.find():
        print(u)

    print("\nProducts:")
    for p in products.find():
        print(p)

    print("\nOrders:")
    for o in orders.find():
        print(o)

if __name__ == "__main__":
    init_mongodb()
