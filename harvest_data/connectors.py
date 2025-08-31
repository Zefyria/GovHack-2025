import os
import json
import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from .db import get_connection

BASE_DIR = os.path.dirname(__file__)
URLS_PATH = os.path.join(BASE_DIR, "urls.json")


def insert_dataset(db_conn, source_name, title, description, url, format_=""):
    """Insert a single dataset into the database."""
    cur = db_conn.cursor()
    cur.execute(
        """
        INSERT INTO govhack2025.datasets (source_name, title, description, url, format_)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (source_name, title, description, url, format_)
    )
    db_conn.commit()
    cur.close()


# ---------------- ABS API FETCH ----------------
def fetch_abs(conn):
    print("Processing Australian Bureau of Statistics (api)")
    url = "https://data.api.abs.gov.au/rest/dataflow/ABS"

    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch API data: {e} for url: {url}")
        return

    # Parse XML
    root = ET.fromstring(response.content)
    ns = {
        'struc': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
        'com': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
        'msg': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message'
    }

    dataflows = root.findall('.//struc:Dataflow', ns)
    count = 0
    for df in dataflows:
        name_elem = df.find('com:Name', ns)
        desc_elem = df.find('com:Description', ns)
        title = name_elem.text if name_elem is not None else df.get('id')
        description = desc_elem.text if desc_elem is not None else ""
        dataset_url = f"https://data.api.abs.gov.au/rest/data/{df.get('id')}/all"

        insert_dataset(conn, "Australian Bureau of Statistics", title, description, dataset_url, "xml")
        count += 1
        if count % 100 == 0:
            print(f"Inserted {count} ABS datasets so far...")

    print(f"Australian Bureau of Statistics returned {len(dataflows)} datasets")


# ---------------- ATO FETCH ----------------
def fetch_ato(conn, urls):
    """
    Fetch ATO XLSX datasets from data.gov.au dataset pages.

    - urls: list of dataset page URLs
    - Inserts only the first XLSX per page, with title from <title> tag
    """
    print("Processing ATO XLSX datasets")
    count = 0
    headers = {"User-Agent": "Mozilla/5.0"}

    for page_url in urls:
        try:
            resp = requests.get(page_url, headers=headers, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"Failed to fetch ATO page {page_url}: {e}")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        # Use <title> tag as dataset title
        title_tag = soup.find("title")
        title = title_tag.get_text(strip=True) if title_tag else page_url

        # Find first XLSX link
        xlsx_link = soup.find("a", href=lambda h: h and h.lower().endswith(".xlsx"))
        if not xlsx_link:
            print(f"No XLSX found on page {page_url}")
            continue

        href = xlsx_link['href']
        if href.startswith("/"):
            href = f"https://www.data.gov.au{href}"

        insert_dataset(conn, "ATO", title, None, href, "xlsx")
        count += 1

        if count % 10 == 0:
            print(f"Inserted {count} ATO datasets so far...")

    print(f"ATO returned {count} datasets")


# ---------------- Data.gov.au API FETCH ----------------
def fetch_data_gov_au(conn, base_url):
    """Fetch ALL datasets from Data.gov.au using pagination (no limit)."""
    print("Processing Data.gov.au (api)")
    start = 0
    rows_per_request = 100  # fetch 100 per request for efficiency
    total_count = 0

    while True:
        try:
            url = f"{base_url}?start={start}&rows={rows_per_request}"
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"Failed to fetch API data: {e} for url: {url}")
            break

        data = resp.json()
        results = data.get("result", {}).get("results", [])
        if not results:
            break

        for item in results:
            insert_dataset(
                conn,
                "Data.gov.au",
                item.get("title"),
                item.get("notes"),
                item.get("url"),
                item.get("format", "")
            )
            total_count += 1

        start += rows_per_request
        print(f"Fetched {total_count} datasets from Data.gov.au so far...")

    print(f"Data.gov.au fetch complete. Total: {total_count}")


# ---------------- MAIN FETCH ALL ----------------
def fetch_all_datasets():
    """Fetch datasets from all sources listed in urls.json."""
    conn = get_connection()

    # Load urls.json
    with open(URLS_PATH, "r", encoding="utf-8") as f:
        urls_data = json.load(f)

    for source in urls_data:
        name = source["name"]
        url = source["url"]
        type_ = source["type"]

        print(f"Starting source: {name}")
        if "ato" in type_:
            fetch_ato(conn, url)
        elif type_ == "data.gov.au api":
            fetch_data_gov_au(conn, url)
        elif type_ == "abs api":
            fetch_abs(conn)
        else:
            print(f"Unknown source type: {type_} for source: {name}")

    conn.close()
    print("Harvest completed.")
