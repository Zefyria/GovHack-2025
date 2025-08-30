# harvest_runner.py
import json
import importlib
from .db import init_db, upsert_dataset
from .logging_config import logger

def run_harvest():
    conn = init_db()
    # Load URLs from JSON
    with open("harvest/urls.json", "r") as f:
        urls = json.load(f)

    for entry in urls:
        connector_module_name = f"harvest.connectors.{entry['type']}_connector"
        try:
            module = importlib.import_module(connector_module_name)
            if hasattr(module, "harvest"):
                datasets = module.harvest(entry["url"])
                logger.info(f"{entry['name']} returned {len(datasets)} datasets")
                for ds in datasets:
                    upsert_dataset(conn, ds)
                    logger.info(f"Upserted dataset: {ds['name']}")
        except Exception as e:
            logger.error(f"Failed to process {entry['name']}: {e}")

if __name__ == "__main__":
    run_harvest()
