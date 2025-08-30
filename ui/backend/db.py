import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "zHub",
    "user": "postgres",
    "password": os.getenv("DB_PASSWORD")  # read from environment variable
}

def get_connection():
    if not DB_CONFIG["password"]:
        raise ValueError("Database password not set. Please set DB_PASSWORD in your environment.")
    return psycopg2.connect(**DB_CONFIG)

def initialize_database():
    conn = get_connection()
    cur = conn.cursor()
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
