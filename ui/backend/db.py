import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()  # loads .env file

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "zHub",
    "user": "postgres",
    "password": os.getenv("DB_PASSWORD"),
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)
