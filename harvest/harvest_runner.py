from .db import init_db
from .connectors import harvest_all
from .logging_config import logger

def main():
    logger.info("Initializing database...")
    init_db()
    logger.info("Database initialized and table ready.")

    logger.info("Starting harvest...")
    harvest_all("urls.json")
    logger.info("Harvest completed.")

if __name__ == "__main__":
    main()
