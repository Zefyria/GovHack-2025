import psycopg2
from psycopg2 import sql
from getpass import getpass
from .logging_config import logger

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "govhack2025",
    "user": "postgres",
    "password": None  # Will prompt
}

def get_connection():
    if DB_CONFIG["password"] is None:
        DB_CONFIG["password"] = getpass("Enter PostgreSQL password: ")
    conn = psycopg2.connect(**DB_CONFIG)
    return conn

def init_db():
    logger.info("Initializing database...")
    conn = get_connection()
    cur = conn.cursor()
    # Create schema if not exists
    cur.execute("CREATE SCHEMA IF NOT EXISTS govhack2025;")
    # Create datasets table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS govhack2025.datasets (
            source_name TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            url TEXT,
            format TEXT,
            last_updated TIMESTAMP,
            PRIMARY KEY (source_name, title)
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Database initialized and table ready.")

def refresh_source_in_db(source_name):
    """Delete old datasets for a source before inserting new."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM govhack2025.datasets WHERE source_name = %s;",
        (source_name,)
    )
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Cleared previous datasets for {source_name}")
