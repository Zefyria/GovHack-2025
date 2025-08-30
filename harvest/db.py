import psycopg2
from psycopg2 import sql
from .logging_config import logger

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "zHub",
    "user": "postgres",
    "password": None  # will prompt
}

def init_db():
    if not DB_CONFIG["password"]:
        DB_CONFIG["password"] = input("Enter PostgreSQL password: ")

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    # Create schema if not exists
    cur.execute("CREATE SCHEMA IF NOT EXISTS govhack2025;")
    # Create table if not exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS govhack2025.datasets (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE,
            description TEXT,
            available BOOLEAN,
            download TEXT
        );
    """)

    logger.info("Database initialized and table ready.")
    return conn

def refresh_dataset(conn, dataset):
    """
    Delete dataset if it already exists, then insert fresh.
    """
    cur = conn.cursor()
    cur.execute("SELECT id FROM govhack2025.datasets WHERE name = %s", (dataset["name"],))
    if cur.fetchone():
        logger.info(f"Deleting old dataset: {dataset['name']}")
        cur.execute("DELETE FROM govhack2025.datasets WHERE name = %s", (dataset["name"],))
    # Insert fresh
    cur.execute("""
        INSERT INTO govhack2025.datasets (name, description, available, download)
        VALUES (%s, %s, %s, %s)
    """, (dataset["name"], dataset["description"], dataset["available"], dataset["download"]))
    conn.commit()
