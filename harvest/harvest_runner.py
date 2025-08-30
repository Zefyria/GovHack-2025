import json
import logging
from harvest.connectors import fetch_api_data
from harvest.db import init_db, refresh_source_in_db

# Set up logging to console and file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("harvest.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def harvest_all(urls_file):
    # Load sources from JSON
    with open(urls_file, "r") as f:
        sources = json.load(f)

    for source in sources:
        logger.info(f"Processing {source['name']} ({source['type']})")
        try:
            # Remove previous entries for this source to avoid duplicates
            refresh_source_in_db(source["name"])
            
            datasets = fetch_api_data(source)
            if not datasets:
                logger.info(f"No datasets returned for {source['name']}")
            else:
                logger.info(f"{len(datasets)} datasets returned for {source['name']}")
                # Insert datasets into database
                from harvest.db import insert_dataset
                for ds in datasets:
                    insert_dataset(ds, source_name=source["name"])
                    logger.info(f"Inserted dataset: {ds['name']}")
        except Exception as e:
            logger.error(f"Failed to harvest {source['name']}: {e}")

    logger.info("Harvest completed.")

def main():
    logger.info("Initializing database...")
    init_db()
    urls_path = "harvest/urls.json"
    logger.info("Starting harvest...")
    harvest_all(urls_path)

if __name__ == "__main__":
    main()
