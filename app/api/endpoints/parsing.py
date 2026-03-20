# app/api/endpoints/parsing.py
import logging
from fastapi import APIRouter, Depends, BackgroundTasks, HTTPException, Query
from sqlalchemy.orm import Session
from datetime import datetime
from typing import Optional, List, Dict, Any


from app.database import get_db
from app.models import ParsingSession, ParsingFile  # ← Импорт моделей
from app.schemas import (
    ParsingRequest, 
    ParsingResponse, 
    ParsingProgress, 
    ParsingSessionInfo
)
from app.services.parsing_service import ParsingService


logger = logging.getLogger(__name__)  # ← Добавить эту строку после импортов!

router = APIRouter()
parsing_service = ParsingService()


# app/api/endpoints/parsing.py

@router.post("/start", response_model=ParsingResponse)
async def start_parsing(
        request: ParsingRequest,
        background_tasks: BackgroundTasks,
        db: Session = Depends(get_db)
):
    """Запустить парсинг логов с проверкой перекрытия интервалов"""
    try:
        logger.info(f"Starting parsing: {request.start_date} to {request.end_date}")

        # 🔍 ПРОВЕРКА 1: Валидация дат (только текущий день, в пределах часа)
        today = datetime.now().date()
        if request.start_date.date() != today or request.end_date.date() != today:
            raise HTTPException(
                status_code=400,
                detail=f"Парсинг разрешён только для текущей даты ({today})"
            )

        if request.start_date.hour != request.end_date.hour:
            raise HTTPException(
                status_code=400,
                detail="Интервал должен укладываться в пределах одного часа"
            )

        # 🔍 ПРОВЕРКА 2: Перекрытие с существующими running-сессиями
        overlapping_session = db.query(ParsingSession).filter(
            ParsingSession.status == "running",
            ParsingSession.start_date <= request.end_date,
            ParsingSession.end_date >= request.start_date
        ).first()

        if overlapping_session:
            logger.warning(
                f"⚠️  Запрос перекрывается с сессией {overlapping_session.session_id}: "
                f"{overlapping_session.start_date} - {overlapping_session.end_date}"
            )
            # 🔧 Возвращаем редирект на существующую сессию
            return {
                "session_id": overlapping_session.session_id,
                "status": "already_running",
                "message": f"Парсинг уже выполняется для пересекающегося интервала. "
                           f"Перейдите к просмотру прогресса сессии {overlapping_session.session_id[:8]}...",
                "redirect_url": f"/parsing/progress/{overlapping_session.session_id}"
            }

        # 🔍 ПРОВЕРКА 3: Перекрытие с recently completed сессиями (опционально)
        # recent_completed = db.query(ParsingSession).filter(
        #     ParsingSession.status == "completed",
        #     ParsingSession.completed_at >= datetime.utcnow() - timedelta(hours=1),
        #     ParsingSession.start_date <= request.end_date,
        #     ParsingSession.end_date >= request.start_date
        # ).first()
        # if recent_completed:
        #     return {
        #         "session_id": recent_completed.session_id,
        #         "status": "already_completed",
        #         "message": "Данные для этого интервала уже обработаны",
        #         "redirect_url": f"/analysis/{recent_completed.session_id}"
        #     }

        # ✅ Создаём новую сессию
        session_id = await parsing_service.start_parsing(
            db=db,
            start_date=request.start_date,
            end_date=request.end_date
        )

        # Запускаем фоновую задачу
        background_tasks.add_task(
            parsing_service.run_parsing_task,
            db=db,
            session_id=session_id,
            start_date=request.start_date,
            end_date=request.end_date
        )

        logger.info(f"Parsing started with session_id: {session_id}")

        return ParsingResponse(
            session_id=session_id,
            status="started",
            message="Парсинг запущен"
        )

    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Unexpected error: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка: {str(e)}")


# app/api/endpoints/parsing.py — добавьте этот endpoint

# app/api/endpoints/parsing.py — в функции get_parsing_sessions

@router.get("/sessions", response_model=List[Dict[str, Any]])
async def get_parsing_sessions(
        limit: int = Query(5, ge=1, le=100),  # 🔧 По умолчанию 5 для главной страницы
        offset: int = Query(0, ge=0),
        db: Session = Depends(get_db)
):
    """Получить список сессий парсинга"""

    # 🔧 Сортируем по start_date (дата запуска), а не created_at
    sessions = db.query(ParsingSession).order_by(
        ParsingSession.start_date.desc()  # ← Сортировка по дате запуска!
    ).offset(offset).limit(limit).all()

    result = []
    for s in sessions:
        elapsed = None
        if s.started_at:
            end_time = s.completed_at or datetime.utcnow()
            elapsed = (end_time - s.started_at).total_seconds()

        result.append({
            "session_id": s.session_id,
            "status": s.status,
            "start_date": s.start_date.isoformat() if s.start_date else None,  # ← Для JS
            "end_date": s.end_date.isoformat() if s.end_date else None,
            "directory_name": s.directory_name,
            "progress_percent": s.progress_percent or 0.0,
            "total_events": s.total_events or 0,
            "total_files": s.total_files or 0,
            "processed_files": s.processed_files or 0,
            "started_at": s.started_at.isoformat() if s.started_at else None,
            "completed_at": s.completed_at.isoformat() if s.completed_at else None,
            "elapsed_seconds": elapsed,
            "error_message": s.error_message
        })

    return result


@router.get("/sessions/{session_id}/progress", response_model=ParsingProgress)
async def get_progress(session_id: str, db: Session = Depends(get_db)):
    """Получить прогресс парсинга"""
    from app.database import ParsingSession
    session = db.query(ParsingSession).filter(ParsingSession.session_id == session_id).first()
    if not session:
        raise HTTPException(status_code=404, detail="Сессия не найдена")
    
    return ParsingProgress(
        session_id=session.session_id,
        status=session.status,
        progress_percent=session.progress_percent,
        total_files=session.total_files,
        processed_files=session.processed_files,
        total_events=session.total_events
    )

@router.post("/sessions/{session_id}/cancel")
async def cancel_parsing(session_id: str, db: Session = Depends(get_db)):
    """Отменить парсинг"""
    await parsing_service.cancel_parsing(session_id)
    return {"status": "cancelled", "session_id": session_id}


# app/api/endpoints/parsing.py

# app/api/endpoints/parsing.py

@router.get("/progress/{session_id}", response_model=ParsingProgress)
async def get_parsing_progress(session_id: str, db: Session = Depends(get_db)):
    """Получить детальный прогресс парсинга с информацией о текущем файле"""

    session = db.query(ParsingSession).filter(
        ParsingSession.session_id == session_id
    ).first()

    if not session:
        raise HTTPException(status_code=404, detail="Сессия не найдена")

    # 🔧 Формируем информацию о текущем файле
    current_file = None
    if session.status == "running" and session.current_file_name:
        current_file = {
            "name": session.current_file_name,
            "path": session.current_file_path,
            "size_bytes": session.current_file_size,
            "size_mb": round(session.current_file_size / 1024 / 1024, 2) if session.current_file_size else None,
            "events_count": session.current_file_events or 0,
            "started_at": session.updated_at.isoformat() if hasattr(session,
                                                                    'updated_at') and session.updated_at else None
        }

    # Расчёт времени
    elapsed_seconds = None
    eta_seconds = None
    if session.started_at:
        end_time = session.completed_at or datetime.utcnow()
        elapsed_seconds = (end_time - session.started_at).total_seconds()

        if session.progress_percent > 0 and session.status == "running":
            eta_seconds = (
                                      elapsed_seconds / session.progress_percent * 100) - elapsed_seconds if session.progress_percent > 0 else None

    return ParsingProgress(
        session_id=session.session_id,
        status=session.status,
        progress_percent=session.progress_percent or 0.0,
        total_files=session.total_files or 0,
        processed_files=session.processed_files or 0,
        total_events=session.total_events or 0,
        error_message=session.error_message,
        started_at=session.started_at,
        completed_at=session.completed_at,
        elapsed_seconds=elapsed_seconds,
        eta_seconds=eta_seconds,
        current_file=current_file  # 🔧 Добавляем информацию о текущем файле
    )