import psycopg2
from psycopg2 import sql
import getpass

# === CONFIG ===
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "zHub",
    "user": "postgres",
    "password": None  # will be prompted
}

def get_connection():
    if DB_CONFIG["password"] is None:
        DB_CONFIG["password"] = getpass.getpass("Enter PostgreSQL password: ")
    
    return psycopg2.connect(**DB_CONFIG)

def initialize_database():
    conn = get_connection()
    cur = conn.cursor()
    
    # Create the datasets table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS govhack2025.datasets (
        id SERIAL PRIMARY KEY,
        source_name TEXT NOT NULL,
        title TEXT,
        description TEXT,
        url TEXT,
        format_ TEXT
    );
    """
    cur.execute(create_table_query)
    conn.commit()
    cur.close()
    conn.close()
    print("Database initialized and table ready.")
