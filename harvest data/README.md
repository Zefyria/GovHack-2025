```
pakfa/
├─ harvest/
│  ├─ __init__.py            # package marker
│  ├─ connectors.py          # dynamic connector; loops through urls.json
│  ├─ db.py                  # DB connection + schema creation + refresh_dataset
│  ├─ harvest_runner.py      # MAIN SCRIPT (runs full harvest)
│  ├─ logging_config.py      # logging setup (console + file)
│  ├─ urls.json              # list of sources to harvest (name, url, type, notes)
│  └─ harvest.log            # log file generated at runtime
```