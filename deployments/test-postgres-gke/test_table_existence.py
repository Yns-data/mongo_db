import psycopg2

# Paramètres de connexion
host = "34.155.129.75"  # si tu utilises port-forward ; sinon EXTERNAL-IP
port = 5432
user = "younes"     # remplace par ton utilisateur
password = "younes" # remplace par le mot de passe
dbname = "airlines_db"            # base par défaut pour se connecter


try:
    # Connexion à PostgreSQL
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname
    )

    cur = conn.cursor()

    # Requête pour lister les tables dans le schéma public
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public';
    """)
    tables = cur.fetchall()

    print(f"Tables dans la base '{dbname}' :")
    for table in tables:
        print(f" - {table[0]}")

    cur.close()
    conn.close()

except Exception as e:
    print("Erreur :", e)
