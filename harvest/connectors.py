import requests
from bs4 import BeautifulSoup
from .logging_config import logger

def harvest_url(entry):
    datasets = []
    url = entry["url"]
    type_ = entry["type"].lower()

    try:
        if type_ == "api":
            # Attempt CKAN-style or ABS-style API discovery
            r = requests.get(f"{url}/api/3/action/package_list")  # CKAN default
            if r.status_code == 200:
                package_names = r.json().get("result", [])
                for name in package_names[:50]:
                    info_r = requests.get(f"{url}/api/3/action/package_show?id={name}")
                    info_r.raise_for_status()
                    pkg = info_r.json()["result"]
                    datasets.append({
                        "name": pkg.get("title"),
                        "description": pkg.get("notes"),
                        "available": True,
                        "download": ", ".join([f['format'] for f in pkg.get("resources", [])])
                    })
            else:
                # fallback for other API style (e.g., ABS JSON)
                r = requests.get(f"{url}catalogue/1.0.0/data")
                r.raise_for_status()
                data = r.json()
                for item in data.get('dataSets', []):
                    datasets.append({
                        "name": item.get("title"),
                        "description": item.get("summary"),
                        "available": True,
                        "download": "csv"
                    })

        elif type_ == "html":
            r = requests.get(url)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, 'lxml')
            for link in soup.find_all('a'):
                href = link.get('href')
                if href and any(ext in href for ext in ['.csv', '.xls', '.xlsx', '.json', '.pdf']):
                    datasets.append({
                        "name": link.text.strip() or href.split('/')[-1],
                        "description": "Extracted from page",
                        "available": True,
                        "download": href.split('.')[-1]
                    })
        else:
            logger.warning(f"Unknown type '{type_}' for {entry['name']}")

    except Exception as e:
        logger.error(f"Failed to harvest {entry['name']}: {e}")

    return datasets

def harvest_all(urls):
    results = {}
    for entry in urls:
        logger.info(f"Processing {entry['name']} ({entry['type']})")
        datasets = harvest_url(entry)
        logger.info(f"{entry['name']} returned {len(datasets)} datasets")
        results[entry['name']] = datasets
    return results
