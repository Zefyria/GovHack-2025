# connectors/abs_connector.py
def harvest(url):
    # Placeholder: replace with real ABS API fetch
    datasets = [
        {
            "name": "Labour Force Survey",
            "description": "ABS Labour Force Survey dataset",
            "available": True,
            "download": "csv"
        },
        {
            "name": "Census 2021",
            "description": "ABS Census data 2021",
            "available": True,
            "download": "xlsx"
        }
    ]
    return datasets
