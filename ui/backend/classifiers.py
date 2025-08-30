import os
import json
import requests
from bs4 import BeautifulSoup
from db import get_connection

BASE_DIR = os.path.dirname(__file__)
URLS_PATH = os.path.join(BASE_DIR, "urls.json")

TOPICS = {
    "finance": ["tax", "finance", "economic", "budget"],
    "health": ["health", "hospital", "disease", "alcohol", "tobacco"],
    "education": ["education", "school", "students", "teachers"],
    "environment": ["environment", "emissions", "climate", "energy"],
}

def classify_topic(title, description=""):
    text = f"{title} {description}".lower()
    for topic, keywords in TOPICS.items():
        if any(k in text for k in keywords):
            return topic
    return "other"

def insert_dataset(db_conn, source_name, title, description, url, format_="", topic="other"):
    cur = db_conn.cursor()
    cur.execute(
        """
        INSERT INTO govhack2025.datasets (source_name, title, description, url, format_)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (source_name, title, description, url, format_)
    )
    db_conn.commit()
    cur.close()

def fetch_abs(conn, url, max_results=50):
    print("Processing ABS API datasets")
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch ABS: {e}")
        return

    from xml.etree import ElementTree as ET
    root = ET.fromstring(resp.content)
    ns = {'struc': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
          'com': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common'}

    dataflows = root.findall('.//struc:Dataflow', ns)
    count = 0
    for df in dataflows:
        if count >= max_results:
            break
        name_elem = df.find('com:Name', ns)
        desc_elem = df.find('com:Description', ns)
        title = name_elem.text if name_elem is not None else df.get('id')
        description = desc_elem.text if desc_elem is not None else ""
        dataset_url = f"https://data.api.abs.gov.au/rest/data/{df.get('id')}/all"
        topic = classify_topic(title, description)
        insert_dataset(conn, "ABS", title, description, dataset_url, "xml", topic)
        count += 1
    print(f"ABS returned {count} datasets")

def fetch_data_gov_au(conn, url, max_results=50):
    print("Processing Data.gov.au datasets")
    start = 0
    rows_per_request = 50
    total_fetched = 0
    while total_fetched < max_results:
        try:
            response = requests.get(f"{url}&start={start}&rows={rows_per_request}", timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Failed Data.gov.au: {e}")
            break
        data = response.json()
        results = data.get('result', {}).get('results', [])
        if not results:
            break
        for item in results:
            if total_fetched >= max_results:
                break
            title = item.get('title', '')
            description = item.get('notes', '')
            dataset_url = item.get('url', '')
            format_ = item.get('format', '')
            topic = classify_topic(title, description)
            insert_dataset(conn, "Data.gov.au", title, description, dataset_url, format_)
            total_fetched += 1
        start += rows_per_request
    print(f"Data.gov.au returned {total_fetched} datasets")

def fetch_ato(conn, url, max_results=50):
    print("Processing ATO datasets")
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed ATO page: {e}")
        return
    soup = BeautifulSoup(resp.text, "html.parser")
    link = soup.select_one("a[href$='.xlsx']")
    if link:
        href = link['href']
        if href.startswith("/"):
            href = f"https://data.gov.au{href}"
        title_tag = soup.select_one("title")
        title = title_tag.text.strip() if title_tag else "Download"
        topic = classify_topic(title)
        insert_dataset(conn, "ATO", title, None, href, "xlsx", topic)
        print(f"ATO returned 1 dataset")
    else:
        print("ATO returned 0 datasets")
