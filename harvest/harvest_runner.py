from .db import initialize_database
from .connectors import fetch_all_datasets
from .logging_config import logger

def main():
    # Initialize DB and table
    initialize_database()
    
    # Run the harvest for all sources
    fetch_all_datasets()
    
    logger.info("Harvest completed.")

if __name__ == "__main__":
    main()
