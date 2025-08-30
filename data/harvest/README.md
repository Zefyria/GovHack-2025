pakfa/
├─ harvest/
│  ├─ run_harvest.py         # MAIN SCRIPT
│  ├─ connectors.py          # decides which connector to use per URL
│  ├─ db.py                  # DB connection + schema creation
│  ├─ normalise_utils.py     # normalize fields (title, desc, API link, etc.)
│  ├─ robots_helper.py       # check robots.txt
│  ├─ logging_config.py      # logging setup
│  ├─ urls.json              # list of URLs to harvest
│  └─ connectors/            
│      ├─ abs_connector.py
│      ├─ ckan_connector.py
│      └─ html_scraper.py
