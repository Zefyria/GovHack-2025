import psycopg2
from psycopg2 import sql
from getpass import getpass
from .logging_config import logger

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "govhack2025",
    "user": "postgres",
    "password": None  # will be prompted
}

SCHEMA = "govhack2025"
TABLE = "datasets"


def get_connection():
    if not DB_CONFIG["password"]:
        DB_CONFIG["password"] = getpass("Enter PostgreSQL password: ")
    conn = psycopg2.connect(**DB_CONFIG)
    return conn


def init_db():
    """Initialize schema and table with required columns."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        # Create schema if not exists
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA)))

        # Create table if not exists
        cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.{} (
                id SERIAL PRIMARY KEY
            )
        """).format(sql.Identifier(SCHEMA), sql.Identifier(TABLE)))

        # Ensure all columns exist
        required_columns = {
            "source_name": "VARCHAR(255)",
            "title": "TEXT",
            "description": "TEXT",
            "url": "TEXT",
            "format_": "VARCHAR(50)"
        }

        for col, col_type in required_columns.items():
            cur.execute(sql.SQL("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_schema = %s
                          AND table_name = %s
                          AND column_name = %s
                    ) THEN
                        ALTER TABLE {}.{} ADD COLUMN {} {};
                    END IF;
                END
                $$;
            """).format(sql.Identifier(SCHEMA), sql.Identifier(TABLE),
                        sql.Identifier(col), sql.SQL(col_type)),
                        (SCHEMA, TABLE, col))
        conn.commit()
        logger.info("Database initialized and table ready.")
    except Exception as e:
        logger.exception(f"Error initializing DB: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def refresh_source_in_db(source_name):
    """Delete previous datasets for a given source."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql.SQL("DELETE FROM {}.{} WHERE source_name = %s").format(
            sql.Identifier(SCHEMA), sql.Identifier(TABLE)), (source_name,))
        conn.commit()
        logger.info(f"Cleared previous datasets for {source_name}")
    except Exception as e:
        logger.exception(f"Failed to clear datasets for {source_name}: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def insert_dataset(source_name, title, description, url, format_):
    """Insert a single dataset record."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql.SQL("""
            INSERT INTO {}.{} (source_name, title, description, url, format)
            VALUES (%s, %s, %s, %s, %s)
        """).format(sql.Identifier(SCHEMA), sql.Identifier(TABLE)),
                    (source_name, title, description, url, format_))
        conn.commit()
        logger.info(f"Inserted dataset: {title}")
    except Exception as e:
        logger.exception(f"Failed to insert dataset '{title}': {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()
