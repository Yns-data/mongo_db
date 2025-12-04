import psycopg2
from psycopg2 import sql
from faker import Faker

# Initialisation du générateur de données factices
fake = Faker()

# Configuration de la connexion PostgreSQL
DB_CONFIG = {
    "dbname": "airlines_db",
    "user": "younes",
    "password": "younes",
    "host": "34.155.121.43",      # ou l’IP/nom DNS de ton serveur
    "port": 5432
}

# Fonction d'insertion
def insert_fake_users(n=10):
    try:
        # Connexion à PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Insertion multiple de faux utilisateurs
        for _ in range(n):
            username = fake.user_name()
            email = fake.email()

            query = sql.SQL("""
SELECT * FROM "test-schema" 
            """)
            cur.execute(query, (username, email))

        # Validation
        conn.commit()
        print(f"{n} utilisateurs insérés avec succès dans test-schema.users ✅")

    except Exception as e:
        print("Erreur :", e)

    finally:
        if conn:
            cur.close()
            conn.close()

# Exécution principale
if __name__ == "__main__":
    insert_fake_users(20)  # Par exemple, 20 utilisateurs
