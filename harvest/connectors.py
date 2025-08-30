from .db import get_connection
import requests

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
    print(f"Inserted dataset: {title}")

# Example function to fetch from data.gov.au
def fetch_data_gov_au():
    source_name = "Data.gov.au"
    clear_previous_datasets(source_name)
    
    url = "https://data.gov.au/api/3/action/package_search"
    rows_per_fetch = 100
    start = 0
    total = 0
    
    while True:
        response = requests.get(url, params={"start": start, "rows": rows_per_fetch})
        data = response.json()
        results = data.get("result", {}).get("results", [])
        
        if not results:
            break
        
        for dataset in results:
            title = dataset.get("title")
            description = dataset.get("notes")
            dataset_url = dataset.get("url")
            format_ = ", ".join(dataset.get("resources")[0].get("format", "").split()) if dataset.get("resources") else None
            
            insert_dataset(source_name, title, description, dataset_url, format_)
        
        start += rows_per_fetch
        total += len(results)
        print(f"Fetched {total} datasets from {source_name}")

def fetch_all_datasets():
    """Harvest all sources."""
    fetch_data_gov_au()
    fetch_abs_data()  # if you have an ABS fetch function
