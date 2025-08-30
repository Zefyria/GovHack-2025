import json
import os
import requests
from .db import insert_dataset, refresh_source_in_db
import logging

logger = logging.getLogger(__name__)

# Determine absolute path to urls.json
BASE_DIR = os.path.dirname(__file__)
URLS_FILE = os.path.join(BASE_DIR, "urls.json")

def fetch_api_data(source, max_rows=200):
    """Fetch datasets from an API-type source (Data.gov.au, ABS, etc.) with pagination."""
    logger.info(f"Fetching API data from {source['name']}")
    datasets = []
    rows_per_page = 100
    start = 0
    total_fetched = 0
    base_url = source.get("url")
    if not base_url:
        logger.warning(f"No URL provided for {source['name']}")
        return datasets

    while total_fetched < max_rows:
        params = {"q": "", "rows": min(rows_per_page, max_rows - total_fetched), "start": start}
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logger.error(f"Failed to fetch API data: {e}")
            break

        # Data.gov.au structure
        results = data.get("result", {}).get("results", []) or data.get("data", [])
        if not results:
            break

        for d in results:
            title = d.get("title") or d.get("name")
            description = d.get("notes") or d.get("description") or ""
            url = d.get("url")
            fmt = None
            # Some datasets have 'resources' array
            if "resources" in d and d["resources"]:
                url = d["resources"][0].get("url") or url
                fmt = d["resources"][0].get("format_")
            datasets.append({
                "source_name": source["name"],
                "title": title,
                "description": description,
                "url": url,
                "format_": fmt
            })

        fetched_now = len(results)
        total_fetched += fetched_now
        start += fetched_now
        logger.info(f"Fetched {total_fetched} datasets from {source['name']}")

    return datasets


def fetch_html_data(source, max_rows=200):
    """Generic HTML scraper for datasets (e.g., AIHW)"""
    logger.info(f"Fetching HTML data from {source['name']}")
    datasets = []
    url = source.get("url")
    if not url:
        logger.warning(f"No URL provided for {source['name']}")
        return datasets

    try:
        response = requests.get(url)
        response.raise_for_status()
        html = response.text
        # For now, placeholder: you would parse HTML for dataset links/titles
        # For testing, return empty list or mock
    except Exception as e:
        logger.error(f"Failed to fetch HTML data: {e}")
        return datasets

    return datasets[:max_rows]


def harvest_all(max_rows=200):
    """Main harvest function"""
    logger.info("Starting harvest...")
    # Load sources
    try:
        with open(URLS_FILE, "r", encoding="utf-8") as f:
            sources = json.load(f)
    except Exception as e:
        logger.error(f"Failed to load urls.json: {e}")
        return

    for source in sources:
        logger.info(f"Processing {source['name']} ({source['type']})")
        try:
            # Clear previous datasets
            refresh_source_in_db(source["name"])
            logger.info(f"Cleared previous datasets for {source['name']}")

            # Fetch datasets
            if source["type"] == "api":
                datasets = fetch_api_data(source, max_rows=max_rows)
            elif source["type"] == "html":
                datasets = fetch_html_data(source, max_rows=max_rows)
            else:
                logger.warning(f"Unknown source type {source['type']} for {source['name']}")
                datasets = []

            if not datasets:
                logger.info(f"No datasets returned for {source['name']}")
                continue

            # Insert datasets
            for d in datasets:
                try:
                    insert_dataset(**d)
                    logger.info(f"Inserted dataset: {d['title']}")
                except Exception as e:
                    logger.error(f"Failed to insert dataset: {e}")

            logger.info(f"{source['name']} returned {len(datasets)} datasets")

        except Exception as e:
            logger.error(f"Failed to process {source['name']}: {e}")

    logger.info("Harvest completed.")
