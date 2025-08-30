from logging_config import logger
from datetime import datetime

def harvest(base_url):
    logger.info(f"Scraping HTML site {base_url}")
    
    datasets = [
        {"id": "html:aihw001", "title": "Health Reports"}
    ]
    
    for ds in datasets:
        try:
            last_updated = datetime.today().strftime("%Y-%m-%d")
            
            yield {
                "id": ds["id"],
                "source": "AIHW",
                "title": ds["title"],
                "description": "AIHW annual health reports",
                "category": "Health",
                "subcategory": "Reports",
                "keywords": "health, reports",
                "license": "Open",
                "access_level": "Open",
                "has_api": False,
                "api_url": "",
                "has_download": True,
                "download_url": f"{base_url}/reports/data.zip",
                "file_types": "zip",
                "geographic_coverage": "Australia",
                "time_coverage": "2010-2025",
                "last_updated": last_updated
            }
        except Exception as e:
            logger.error(f"Failed to process HTML dataset {ds['id']}: {e}")
