
from fastapi import FastAPI, Depends, Header, HTTPException
from app.routers import health, datasets, sql

app = FastAPI(title="Core Engine API", version="0.1.0")

# Routers
app.include_router(health.router, prefix="")
app.include_router(datasets.router, prefix="/v1")
app.include_router(sql.router, prefix="/v1")
