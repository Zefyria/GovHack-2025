import json
import requests
from bs4 import BeautifulSoup
from .logging_config import logger
from .db import refresh_dataset

def fetch_api_data(source):
    url = source["url"]
    headers = source.get("headers", {"User-Agent": "PAKFA-GovHack-2025/1.0"})
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        datasets = []

        # generic handling: extract dataset info if it's a list of objects
        if isinstance(data, list):
            for d in data:
                datasets.append({
                    "name": d.get("title") or d.get("name") or "Unnamed Dataset",
                    "description": d.get("description") or "",
                    "available": True,
                    "download": d.get("format") or d.get("download_url") or ""
                })
        elif isinstance(data, dict) and "data" in data:
            for d in data["data"]:
                datasets.append({
                    "name": d.get("title") or d.get("name") or "Unnamed Dataset",
                    "description": d.get("description") or "",
                    "available": True,
                    "download": d.get("format") or d.get("download_url") or ""
                })
        else:
            logger.warning(f"{source['name']} API returned unexpected format")
        return datasets
    except Exception as e:
        logger.error(f"Failed to harvest {source['name']}: {e}")
        return []

def scrape_html(source):
    url = source["url"]
    headers = {"User-Agent": "PAKFA-GovHack-2025/1.0"}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")
        datasets = []

        # generic scraping: find <a> tags with downloadable links
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if any(ext in href.lower() for ext in [".csv", ".xls", ".xlsx", ".json"]):
                datasets.append({
                    "name": a.text.strip() or "Unnamed Dataset",
                    "description": "",
                    "available": True,
                    "download": href
                })
        return datasets
    except Exception as e:
        logger.error(f"Failed to harvest {source['name']}: {e}")
        return []

def harvest_all(urls_file="urls.json"):
    with open(urls_file, "r") as f:
        sources = json.load(f)

    for source in sources:
        logger.info(f"Processing {source['name']} ({source['type']})")
        if source["type"].lower() == "api":
            datasets = fetch_api_data(source)
        elif source["type"].lower() == "html":
            datasets = scrape_html(source)
        else:
            logger.warning(f"Unknown source type for {source['name']}")
            datasets = []

        if datasets:
            for ds in datasets:
                refresh_dataset(ds)
            logger.info(f"{source['name']} returned {len(datasets)} datasets")
        else:
            logger.info(f"{source['name']} returned 0 datasets")
