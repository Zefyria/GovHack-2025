import os
import json
import requests
import xml.etree.ElementTree as ET
from .db import get_connection

# ================= URLS =================
BASE_DIR = os.path.dirname(__file__)
URLS_PATH = os.path.join(BASE_DIR, "urls.json")

with open(URLS_PATH, "r", encoding="utf-8") as f:
    urls = json.load(f)

# ================= DB INSERT HELPER =================
def insert_dataset(db_conn, source_name, title, description, url, format_=""):
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

# ================= DATA.GOV.AU FETCH =================
def fetch_data_gov_au(url, db_conn, max_fetch=50):
    start = 0
    rows = 50  # only fetch 50 per request
    total_fetched = 0

    fetch_url = f"{url}&start={start}"  # no loop, just one fetch
    try:
        resp = requests.get(fetch_url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"Failed to fetch API data: {e} for url: {fetch_url}")
        return

    datasets = data.get("result", {}).get("results", [])
    for ds in datasets[:max_fetch]:
        insert_dataset(
            db_conn,
            "Data.gov.au",
            ds.get("title", ""),
            ds.get("notes", ""),
            ds.get("url", ""),
            format_="json"
        )

    print(f"Fetched {len(datasets[:max_fetch])} datasets from Data.gov.au")


# ================= ABS FETCH =================
def fetch_abs(url, db_conn, max_fetch=50):
    try:
        resp = requests.get(url, timeout=120)  # longer timeout
        resp.raise_for_status()
    except Exception as e:
        print(f"Failed to fetch API data: {e} for url: {url}")
        return

    from xml.etree import ElementTree as ET
    root = ET.fromstring(resp.content)
    datasets = root.findall(".//structure:Dataflow", namespaces={
        "structure": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
        "common": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common"
    })

    for df in datasets[:max_fetch]:
        title_el = df.find(".//common:Name", namespaces={"common": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common"})
        desc_el = df.find(".//common:Description", namespaces={"common": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common"})
        title = title_el.text if title_el is not None else ""
        description = desc_el.text if desc_el is not None else ""
        insert_dataset(db_conn, "ABS", title, description, url, format_="xml")

    print(f"Australian Bureau of Statistics returned {min(len(datasets), max_fetch)} datasets")

# ================= RUN HARVEST =================
def fetch_all_datasets():
    conn = get_connection()

    for source in urls:
        name = source.get("name")
        url = source.get("url")
        print(f"Processing {name} ({source.get('type')})")

        # Clear previous datasets for this source
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM govhack2025.datasets WHERE source_name=%s", (name,)
        )
        conn.commit()
        cur.close()
        print(f"Cleared previous datasets for {name}")

        # Fetch data
        if name == "Data.gov.au":
            fetch_data_gov_au(url, conn)
        elif name == "Australian Bureau of Statistics":
            fetch_abs(url, conn)

    conn.close()
    print("Harvest completed.")
