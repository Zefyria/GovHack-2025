import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def fetch_api_data(source):
    datasets = []
    url = source.get("url")
    headers = {"User-Agent": "Mozilla/5.0"}

    if source["type"] == "api":
        params = {"q": "", "rows": source.get("max_rows", 10)}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            for result in data.get("result", {}).get("results", []):
                datasets.append({
                    "name": result.get("title"),
                    "description": result.get("notes", ""),
                    "available": True,
                    "download": result.get("resources", [{}])[0].get("url")
                })
        else:
            logger.error(f"Failed to fetch API data: {response.status_code}")
    
    elif source["type"] == "html_aihw":
        page = 1
        fetched_rows = 0
        max_rows = source.get("max_rows", 10)
        while fetched_rows < max_rows:
            page_url = f"{url}?page={page}"
            resp = requests.get(page_url, headers=headers)
            if resp.status_code != 200:
                break
            soup = BeautifulSoup(resp.text, "html.parser")
            links = soup.select("a[href$='.xlsx'], a[href$='.pdf']")
            for link in links:
                if fetched_rows >= max_rows:
                    break
                datasets.append({
                    "name": link.text.strip(),
                    "description": "AIHW report",
                    "available": True,
                    "download": link['href']
                })
                fetched_rows += 1
            page += 1

    return datasets

def fetch_all_data(sources):
    all_datasets = []
    for source in sources:
        datasets = fetch_api_data(source)
        all_datasets.extend(datasets)
    return all_datasets
