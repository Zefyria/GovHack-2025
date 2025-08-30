from db import init_db
from connectors import run_harvest

if __name__ == "__main__":
    conn = init_db()
    run_harvest(conn)
    print("Harvest complete. Check harvest.log for details.")
