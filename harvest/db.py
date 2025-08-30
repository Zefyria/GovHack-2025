import psycopg2
from psycopg2 import sql
import getpass
from .logging_config import logger

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "zHub",
    "user": "postgres"
}

SCHEMA_NAME = "govhack2025"
TABLE_NAME = "datasets"

TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
    id TEXT PRIMARY KEY,
    source TEXT,
    title TEXT,
    description TEXT,
    category TEXT,
    subcategory TEXT,
    keywords TEXT,
    license TEXT,
    access_level TEXT,
    has_api BOOLEAN,
    api_url TEXT,
    has_download BOOLEAN,
    download_url TEXT,
    file_types TEXT,
    geographic_coverage TEXT,
    time_coverage TEXT,
    last_updated TEXT,
    harvested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

def get_connection():
    password = getpass.getpass("Enter PostgreSQL password: ")
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            dbname=DB_CONFIG["dbname"],
            user=DB_CONFIG["user"],
            password=password
        )
        logger.info("Database connection established.")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def init_db():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA_NAME)))
            logger.info(f"Schema '{SCHEMA_NAME}' ready.")
            cur.execute(TABLE_SQL)
            logger.info(f"Table '{SCHEMA_NAME}.{TABLE_NAME}' ready.")
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to initialize DB schema/table: {e}")
        raise
    return conn

def upsert_dataset(conn, record):
    columns = record.keys()
    values = [record[col] for col in columns]

    query = sql.SQL("""
        INSERT INTO {}.{} ({})
        VALUES ({})
        ON CONFLICT (id) DO UPDATE SET
        {}
    """).format(
        sql.Identifier(SCHEMA_NAME),
        sql.Identifier(TABLE_NAME),
        sql.SQL(', ').join(map(sql.Identifier, columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(values)),
        sql.SQL(', ').join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
            for c in columns if c != "id"
        )
    )
    try:
        with conn.cursor() as cur:
            cur.execute(query, values)
            conn.commit()
            logger.info(f"Upserted dataset: {record.get('title')}")
    except Exception as e:
        logger.error(f"Failed to upsert dataset {record.get('id')}: {e}")
