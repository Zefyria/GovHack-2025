import psycopg2
from psycopg2 import sql
from getpass import getpass

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "govhack2025",
    "user": "postgres",
    "password": None  # will prompt
}

def get_connection():
    """Return a psycopg2 connection using DB_CONFIG."""
    if not DB_CONFIG["password"]:
        DB_CONFIG["password"] = getpass("Enter PostgreSQL password: ")
    return psycopg2.connect(**DB_CONFIG)

def init_db():
    """Create schema and datasets table if not exists."""
    conn = get_connection()
    cur = conn.cursor()

    # Create schema if not exists
    cur.execute("CREATE SCHEMA IF NOT EXISTS govhack2025;")

    # Create datasets table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS govhack2025.datasets (
            id SERIAL PRIMARY KEY,
            source_name TEXT NOT NULL,
            name TEXT,
            description TEXT,
            available BOOLEAN,
            access_type TEXT,
            download_url TEXT,
            last_updated DATE,
            UNIQUE(source_name, name)
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Database initialized and table ready.")

def refresh_source_in_db(source_name):
    """Delete all datasets for a given source before refreshing."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM govhack2025.datasets WHERE source_name = %s",
        (source_name,)
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"Previous datasets for '{source_name}' deleted.")

def insert_dataset(dataset, source_name):
    """
    Insert a dataset into the database.
    dataset: dict with keys name, description, available, access_type, download_url, last_updated
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO govhack2025.datasets 
            (source_name, name, description, available, access_type, download_url, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_name, name) DO NOTHING;
        """, (
            source_name,
            dataset.get("name"),
            dataset.get("description"),
            dataset.get("available"),
            dataset.get("access_type"),
            dataset.get("download_url"),
            dataset.get("last_updated")
        ))
        conn.commit()
    finally:
        cur.close()
        conn.close()
