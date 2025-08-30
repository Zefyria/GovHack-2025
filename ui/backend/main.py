from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from db import get_connection

app = FastAPI()

# Allow frontend to call backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Replace with frontend URL in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/datasets")
def get_datasets():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT source_name, title, description, url, format_ FROM govhack2025.datasets ORDER BY id DESC"
    )
    rows = cur.fetchall()
    datasets = [
        {
            "source_name": r[0],
            "title": r[1],
            "description": r[2],
            "url": r[3],
            "format_": r[4],
        }
        for r in rows
    ]
    cur.close()
    conn.close()
    return datasets
