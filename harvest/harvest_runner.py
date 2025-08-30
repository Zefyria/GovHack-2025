from .db import init_db
from .connectors import harvest_all
from .logging_config import logger

def main():
    init_db()
    harvest_all()
    logger.info("Harvest completed.")

if __name__ == "__main__":
    main()
