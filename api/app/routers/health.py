
from fastapi import APIRouter

router = APIRouter()

@router.get("/health")
def health():
    return {"status": "ok"}

@router.get("/v1/version")
def version():
    return {"version": "0.1.0"}
