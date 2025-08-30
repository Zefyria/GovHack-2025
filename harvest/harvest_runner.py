import json
from .db import init_db, refresh_dataset
from .logging_config import logger
from .connectors import harvest_all

def run_harvest():
    # Initialize DB
    conn = init_db()
    logger.info("Database initialized and table ready.")

    # Load URLs from JSON
    with open("harvest/urls.json", "r") as f:
        urls = json.load(f)

    # Harvest datasets dynamically
    all_datasets = harvest_all(urls)

    # Insert into DB (refresh old entries)
    for source_name, datasets in all_datasets.items():
        for ds in datasets:
            refresh_dataset(conn, ds)
            logger.info(f"Inserted dataset: {ds['name']}")

    logger.info("Harvest completed.")

if __name__ == "__main__":
    run_harvest()
