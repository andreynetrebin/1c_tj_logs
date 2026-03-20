# app/api/routes.py
from fastapi import APIRouter
from app.api.endpoints import parsing, analysis, events, sessions, admin

api_router = APIRouter()

# Parsing endpoints
api_router.include_router(parsing.router, prefix="/parsing", tags=["parsing"])

# Analysis endpoints  
api_router.include_router(analysis.router, prefix="/analysis", tags=["analysis"])

# Events endpoints
api_router.include_router(events.router, prefix="/events", tags=["events"])

api_router.include_router(sessions.router, prefix="/sessions", tags=["sessions"])
api_router.include_router(admin.router, prefix="/admin", tags=["admin"])  # ← Добавить