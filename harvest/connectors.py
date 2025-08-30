import requests
import os
import io
import json
from .db import get_connection

# Get the directory of this file (connectors.py)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
URLS_PATH = os.path.join(BASE_DIR, "urls.json")


def clear_previous_datasets(source_name):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM govhack2025.datasets WHERE source_name = %s",
        (source_name,)
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"Cleared previous datasets for {source_name}")


def insert_dataset(source_name, title, description, url, format_):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO govhack2025.datasets (source_name, title, description, url, format_)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (source_name, title, description, url, format_)
    )

    conn.commit()
    cur.close()
    conn.close()


def fetch_api_source(source_name, api_url):
    clear_previous_datasets(source_name)

    rows_per_fetch = 100
    start = 0
    total = 0

    print(f"Fetching API data from {source_name}")

    while True:
        response = requests.get(api_url, params={"start": start, "rows": rows_per_fetch})
        if response.status_code != 200:
            print(f"Failed to fetch API data: {response.status_code} {response.reason} for url: {response.url}")
            break

        data = response.json()
        results = data.get("result", {}).get("results", [])

        if not results:
            break

        for dataset in results:
            title = dataset.get("title")
            description = dataset.get("notes")
            dataset_url = dataset.get("url")
            format_ = None
            if dataset.get("resources"):
                format_ = ", ".join(res.get("format", "") for res in dataset["resources"] if res.get("format"))

            insert_dataset(source_name, title, description, dataset_url, format_)

        start += rows_per_fetch
        total += len(results)

        # Print progress every 100 datasets
        if total % 100 == 0:
            print(f"Fetched {total} datasets from {source_name}")

    print(f"{source_name} returned {total} datasets")


def fetch_all_datasets():
    """Harvest all sources from urls.json"""
    try:
        with io.open(URLS_PATH, encoding="utf-8") as f:
            sources = json.load(f)
    except FileNotFoundError:
        print(f"urls.json file not found at {URLS_PATH}")
        return

    for source in sources:
        name = source.get("name")
        url = source.get("url")
        source_type = source.get("type")

        print(f"Processing {name} ({source_type})")

        if source_type == "api" and url:
            fetch_api_source(name, url)
        else:
            print(f"No URL provided for {name}")
