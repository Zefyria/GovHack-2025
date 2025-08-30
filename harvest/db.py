import psycopg2
from psycopg2 import sql
from getpass import getpass
from .logging_config import logger

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "govhack2025",
    "user": "postgres",
    "password": None  # will prompt
}

def get_connection():
    if not DB_CONFIG["password"]:
        DB_CONFIG["password"] = getpass("Enter PostgreSQL password: ")
    conn = psycopg2.connect(**DB_CONFIG)
    return conn

def init_db():
    conn = get_connection()
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
    conn.commit()
    cur.close()
    conn.close()

def refresh_dataset(dataset):
    """Delete any existing dataset with same name and insert the new one"""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM govhack2025.datasets WHERE name = %s;", (dataset["name"],))
        cur.execute("""
            INSERT INTO govhack2025.datasets (name, description, available, download)
            VALUES (%s, %s, %s, %s);
        """, (dataset["name"], dataset["description"], dataset["available"], dataset["download"]))
        conn.commit()
        logger.info(f"Inserted dataset: {dataset['name']}")
    except Exception as e:
        logger.error(f"Failed to insert dataset {dataset['name']}: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()
