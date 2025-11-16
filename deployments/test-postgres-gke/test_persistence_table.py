import psycopg2
from datetime import datetime

# Config PostgreSQL
HOST = "34.155.129.75"          # Si port-forward en local
PORT = 5432
DB = "airlines_db"
USER = "younes"
PASSWORD = "younes"

TABLE_NAME = "test_persistence"

def main():
    try:
        # Connexion à la base
        conn = psycopg2.connect(
            host=HOST,
            port=PORT,
            database=DB,
            user=USER,
            password=PASSWORD
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Création de la table si elle n'existe pas
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                name TEXT,
                created_at TIMESTAMP
            );
        """)
        print(f"Table '{TABLE_NAME}' ready.")

        # Insertion d'une ligne avec timestamp
        now = datetime.now()
        cursor.execute(f"""
            INSERT INTO {TABLE_NAME} (name, created_at)
            VALUES (%s, %s)
            RETURNING id;
        """, (f"test-{now.strftime('%Y%m%d%H%M%S')}", now))
        inserted_id = cursor.fetchone()[0]
        print(f"Inserted row with id={inserted_id} at {now}")

        # Lecture de toutes les données
        cursor.execute(f"SELECT * FROM {TABLE_NAME};")
        rows = cursor.fetchall()
        print("Current rows in table:")
        for row in rows:
            print(row)

        cursor.close()
        conn.close()

    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    main()
