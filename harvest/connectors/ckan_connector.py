from ..logging_config import logger
from datetime import datetime
import requests

def harvest(base_url):
    logger.info(f"Harvesting CKAN portal {base_url}")
    
    # Example: fetch dataset list from CKAN API
    # Placeholder for real CKAN API
    datasets = [
        {"id": "ckan:edu001", "title": "Education Statistics", "metadata_modified": "2025-08-15T00:00:00"}
    ]
    
    for ds in datasets:
        try:
            last_updated = ds.get("metadata_modified")
            if not last_updated:
                last_updated = datetime.today().strftime("%Y-%m-%d")
            else:
                last_updated = last_updated.split("T")[0]  # ISO date format
            
            yield {
                "id": ds["id"],
                "source": "Data.gov.au",
                "title": ds["title"],
                "description": "Student enrolments by state",
                "category": "Education",
                "subcategory": "Schools",
                "keywords": "students, enrolment, education",
                "license": "Open",
                "access_level": "Open",
                "has_api": True,
                "api_url": f"{base_url}/api/3/action/package_show?id={ds['id']}",
                "has_download": True,
                "download_url": f"{base_url}/datasets/{ds['id']}.csv",
                "file_types": "csv",
                "geographic_coverage": "Australia",
                "time_coverage": "2015-2025",
                "last_updated": last_updated
            }
        except Exception as e:
            logger.error(f"Failed to process CKAN dataset {ds['id']}: {e}")
