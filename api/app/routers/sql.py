
import os, requests
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

TRINO_URL = os.getenv("TRINO_URL", "http://trino:8080")

router = APIRouter()

class SQLRequest(BaseModel):
    sql: str

@router.post("/sql")
def run_sql(req: SQLRequest):
    # Minimal passthrough using Trino's REST (for demo; harden later or use a client lib)
    # Trino REST: POST /v1/statement with SQL in body
    resp = requests.post(f"{TRINO_URL}/v1/statement", data=req.sql, headers={"X-Trino-User": "api"})
    if resp.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Trino error: {resp.text}")
    first = resp.json()
    # Follow nextUri if present to collect all data pages
    data = []
    next_uri = first.get("nextUri")
    if "data" in first:
        data.extend(first["data"])
    while next_uri:
        r = requests.get(next_uri)
        j = r.json()
        if "data" in j:
            data.extend(j["data"])
        next_uri = j.get("nextUri")
    return {"columns": first.get("columns", []), "data": data}
