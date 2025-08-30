import json
import importlib
from .logging_config import logger
from .db import upsert_dataset

def load_sources(json_file="harvest/urls.json"):
    with open(json_file) as f:
        return json.load(f)

def run_harvest(conn):
    sources = load_sources()

    for s in sources:
        source_type = s.get("type")
        try:
            module_name = f"harvest.connectors.{source_type}_connector"
            connector_module = importlib.import_module(module_name)

            for record in connector_module.harvest(s["url"]):
                try:
                    upsert_dataset(conn, record)
                except Exception as e:
                    logger.error(f"Failed to insert dataset {record.get('id')}: {e}")

        except ModuleNotFoundError:
            logger.warning(f"No connector found for type '{source_type}', skipping {s['name']}")
        except Exception as e:
            logger.error(f"Error harvesting {s['name']} ({source_type}): {e}")
