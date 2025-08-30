import json
import os
import requests
from .db import refresh_source_in_db, get_connection
from .logging_config import logger
from datetime import datetime

MAX_ROWS = 200  # Limit for testing

# get the folder where this script is located
BASE_DIR = os.path.dirname(__file__)
urls_file = os.path.join(BASE_DIR, "urls.json")

def fetch_api_data(source):
    url = source.get("url")
    if not url:
        logger.info(f"No URL provided for {source['name']}")
        return []

    logger.info(f"Fetching API data from {source['name']}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        results = []

        if "result" in data:  # Data.gov.au CKAN
            packages = data["result"].get("results", [])[:MAX_ROWS]
            for pkg in packages:
                results.append({
                    "title": pkg.get("title"),
                    "description": pkg.get("notes"),
                    "url": pkg.get("url") or pkg.get("resources", [{}])[0].get("url"),
                    "format": pkg.get("resources", [{}])[0].get("format") if pkg.get("resources") else None,
                    "last_updated": pkg.get("metadata_modified")
                })
        else:  # Generic API (e.g. ABS)
            results.append({
                "title": data.get("title") or "ABS dataset",
                "description": data.get("description") or "",
                "url": url,
                "format": "json",
                "last_updated": datetime.now()
            })

        return results[:MAX_ROWS]

    except Exception as e:
        logger.error(f"Failed to fetch API data: {e}")
        return []

def insert_datasets(source_name, datasets):
    if not datasets:
        logger.info(f"No datasets returned for {source_name}")
        return

    conn = get_connection()
    cur = conn.cursor()
    for d in datasets:
        try:
            cur.execute("""
                INSERT INTO govhack2025.datasets
                (source_name, title, description, url, format, last_updated)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (source_name, title) DO UPDATE
                SET description=EXCLUDED.description,
                    url=EXCLUDED.url,
                    format=EXCLUDED.format,
                    last_updated=EXCLUDED.last_updated;
            """, (
                source_name,
                d.get("title"),
                d.get("description"),
                d.get("url"),
                d.get("format"),
                d.get("last_updated")
            ))
            logger.info(f"Inserted dataset: {d.get('title')}")
        except Exception as e:
            logger.error(f"Failed to insert dataset: {e}")
    conn.commit()
    cur.close()
    conn.close()

def harvest_all(urls_file="urls.json"):
    logger.info("Starting harvest...")
    with open(urls_file, "r") as f:
        sources = json.load(f)

    for source in sources:
        source_name = source.get("name")
        source_type = source.get("type")
        refresh_source_in_db(source_name)

        if source_type == "api":
            datasets = fetch_api_data(source)
            insert_datasets(source_name, datasets)
        else:
            logger.warning(f"Unknown source type '{source_type}' for {source_name}")
