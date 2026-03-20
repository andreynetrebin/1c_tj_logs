# app/api/endpoints/sessions.py
"""
API эндпоинты для управления сессиями парсинга
"""

import logging
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from datetime import datetime
from typing import Optional, List, Dict, Any

from app.database import get_db, ParsingSession
from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", response_model=List[Dict[str, Any]])
async def get_sessions(
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        status: Optional[str] = Query(None),
        db: Session = Depends(get_db)
):
    """Получить список сессий парсинга"""

    query = db.query(ParsingSession)
    if status:
        query = query.filter(ParsingSession.status == status)

    sessions = query.order_by(
        ParsingSession.created_at.desc()
    ).offset(offset).limit(limit).all()

    result = []
    for s in sessions:
        elapsed = None
        if s.started_at:
            end = s.completed_at or datetime.utcnow()
            elapsed = (end - s.started_at).total_seconds()

        result.append({
            "session_id": s.session_id,
            "status": s.status,
            "start_date": s.start_date.isoformat() if s.start_date else None,
            "end_date": s.end_date.isoformat() if s.end_date else None,
            "directory_name": s.directory_name,
            "comment": s.comment or "",
            "progress_percent": s.progress_percent or 0.0,
            "total_events": s.total_events or 0,
            "total_files": s.total_files or 0,
            "processed_files": s.processed_files or 0,
            "created_at": s.created_at.isoformat() if s.created_at else None,
            "completed_at": s.completed_at.isoformat() if s.completed_at else None,
            "elapsed_seconds": elapsed,
            "error_message": s.error_message
        })

    return result


@router.put("/{session_id}/comment")
async def update_session_comment(
        session_id: str,
        comment: str,
        db: Session = Depends(get_db)
):
    """Обновить комментарий к сессии"""

    session = db.query(ParsingSession).filter(
        ParsingSession.session_id == session_id
    ).first()

    if not session:
        raise HTTPException(status_code=404, detail="Сессия не найдена")

    session.comment = comment
    session.updated_at = datetime.utcnow()
    db.commit()

    return {"session_id": session_id, "comment": session.comment}


@router.delete("/{session_id}")
async def delete_session(session_id: str, db: Session = Depends(get_db)):
    """Удалить сессию"""

    session = db.query(ParsingSession).filter(
        ParsingSession.session_id == session_id
    ).first()

    if not session:
        raise HTTPException(status_code=404, detail="Сессия не найдена")

    # Опционально: удалить таблицу в ClickHouse
    try:
        from app.services.parsing_service import get_clickhouse_client
        ch = get_clickhouse_client()
        table = f"tj_events_{session_id.replace('-', '_')}"
        db_name = settings.database.clickhouse.database
        ch.command(f"DROP TABLE IF EXISTS {db_name}.`{table}`")
    except Exception as e:
        logger.warning(f"⚠️ Не удалось удалить таблицу ClickHouse: {e}")

    db.delete(session)
    db.commit()

    return {"message": f"Сессия {session_id} удалена"}