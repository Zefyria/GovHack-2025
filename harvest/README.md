```
pakfa/
├─ harvest/
│  ├─ harvest_runner.py      # MAIN SCRIPT (runs full harvest)
│  ├─ connectors.py          # dynamic connector; loops through urls.json
│  ├─ db.py                  # DB connection + schema creation + refresh_dataset
│  ├─ normalise_utils.py     # normalize fields (title, description, API link, etc.)
│  ├─ robots_helper.py       # check robots.txt for scraping rules
│  ├─ logging_config.py      # logging setup (console + file)
│  ├─ urls.json              # list of sources to harvest (name, url, type, notes)
│  └─ __init__.py            # package marker
```