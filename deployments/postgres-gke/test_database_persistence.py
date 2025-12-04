import psycopg2

# Paramètres de connexion
host = "34.155.129.75"  # si tu utilises port-forward ; sinon EXTERNAL-IP
port = 5432
user = "younes"     # remplace par ton utilisateur
password = "younes" # remplace par le mot de passe
dbname = "postgres"            # base par défaut pour se connecter

try:
    # Connexion à PostgreSQL
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname
    )

    # Création d'un curseur
    cur = conn.cursor()

    # Liste des bases de données
    cur.execute("SELECT datname FROM pg_database;")
    databases = cur.fetchall()

    print("Bases de données PostgreSQL :")
    for db in databases:
        print(f" - {db[0]}")

    # Fermeture du curseur et connexion
    cur.close()
    conn.close()

except Exception as e:
    print("Erreur :", e)
