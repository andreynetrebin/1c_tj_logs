# app/api/endpoints/admin.py
"""
API эндпоинты для админ-панели
⚠️  Внимание: Эти эндпоинты позволяют удалять данные!
"""

import logging
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional, List, Dict, Any

from app.database import get_db, ParsingSession, ParsingFile
from app.services.parsing_service import get_clickhouse_client
from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter()


def _get_session_table_name(session_id: str) -> str:
    """Получить имя таблицы ClickHouse для сессии"""
    safe_id = session_id.replace('-', '_')
    return f"tj_events_{safe_id}"


@router.get("/sessions", response_model=List[Dict[str, Any]])
async def admin_get_sessions(
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        status: Optional[str] = Query(None),
        db: Session = Depends(get_db)
):
    """
    [ADMIN] Получить список всех сессий для управления

    🔐 Требует авторизации (реализовать на уровне reverse proxy или middleware)
    """
    query = db.query(ParsingSession)

    if status:
        query = query.filter(ParsingSession.status == status)

    sessions = query.order_by(
        ParsingSession.created_at.desc()
    ).offset(offset).limit(limit).all()

    result = []
    for s in sessions:
        # Получить размер таблицы в ClickHouse (опционально)
        table_size_mb = None
        try:
            ch_client = get_clickhouse_client()
            table_name = _get_session_table_name(s.session_id)
            database = settings.database.clickhouse.database

            size_query = f"""
            SELECT round(sum(data_compressed_bytes) / 1024 / 1024, 2) as size_mb
            FROM system.parts
            WHERE database = '{database}' AND table = '{table_name}' AND active = 1
            """
            size_result = ch_client.query(size_query).first_row
            table_size_mb = size_result[0] if size_result and size_result[0] else None
            ch_client.close()
        except Exception as e:
            logger.debug(f"Не удалось получить размер таблицы {s.session_id}: {e}")

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
            "error_message": s.error_message,
            "clickhouse_table_size_mb": table_size_mb
        })

    return result


@router.get("/sessions/{session_id}", response_model=Dict[str, Any])
async def admin_get_session_detail(session_id: str, db: Session = Depends(get_db)):
    """[ADMIN] Получить детальную информацию о сессии"""

    session = db.query(ParsingSession).filter(
        ParsingSession.session_id == session_id
    ).first()

    if not session:
        raise HTTPException(status_code=404, detail="Сессия не найдена")

    # Получить информацию о таблице в ClickHouse
    ch_info = {}
    try:
        ch_client = get_clickhouse_client()
        table_name = _get_session_table_name(session_id)
        database = settings.database.clickhouse.database

        # Проверить существование таблицы
        check_query = f"""
        SELECT count() FROM system.tables 
        WHERE database = '{database}' AND name = '{table_name}'
        """
        exists = ch_client.query(check_query).first_row[0] > 0

        if exists:
            # Получить статистику
            stats_query = f"""
            SELECT 
                count() as total_rows,
                round(sum(data_compressed_bytes) / 1024 / 1024, 2) as size_mb,
                min(timestamp) as min_ts,
                max(timestamp) as max_ts
            FROM {database}.`{table_name}`
            """
            stats = ch_client.query(stats_query).first_row
            ch_info = {
                "exists": True,
                "table_name": table_name,
                "total_rows": stats[0] if stats[0] else 0,
                "size_mb": stats[1] if stats[1] else 0,
                "time_range": {
                    "start": stats[2].isoformat() if stats[2] else None,
                    "end": stats[3].isoformat() if stats[3] else None
                }
            }
        else:
            ch_info = {"exists": False, "table_name": table_name}

        ch_client.close()
    except Exception as e:
        logger.error(f"❌ Ошибка получения информации о таблице: {e}")
        ch_info = {"exists": False, "error": str(e)}

    return {
        "session_id": session.session_id,
        "status": session.status,
        "start_date": session.start_date.isoformat() if session.start_date else None,
        "end_date": session.end_date.isoformat() if session.end_date else None,
        "directory_name": session.directory_name,
        "comment": session.comment or "",
        "progress_percent": session.progress_percent or 0.0,
        "total_events": session.total_events or 0,
        "total_files": session.total_files or 0,
        "processed_files": session.processed_files or 0,
        "created_at": session.created_at.isoformat() if session.created_at else None,
        "completed_at": session.completed_at.isoformat() if session.completed_at else None,
        "error_message": session.error_message,
        "clickhouse": ch_info
    }


@router.delete("/sessions/{session_id}", response_model=Dict[str, Any])
async def admin_delete_session(
        session_id: str,
        confirm: bool = Query(True, description="Подтверждение удаления"),
        db: Session = Depends(get_db)
):
    """
    [ADMIN] Удалить сессию и все связанные данные

    ⚠️  Это необратимая операция!
    - Удаляет таблицу в ClickHouse
    - Удаляет запись из SQLite (с каскадным удалением файлов)

    🔐 Требует параметра ?confirm=true для предотвращения случайного удаления
    """

    if not confirm:
        raise HTTPException(
            status_code=400,
            detail="Требуется подтверждение: добавьте ?confirm=true к запросу"
        )

    # 1. Найти сессию в SQLite
    session = db.query(ParsingSession).filter(
        ParsingSession.session_id == session_id
    ).first()

    if not session:
        raise HTTPException(status_code=404, detail="Сессия не найдена")

    logger.warning(f"🗑️ ADMIN: Начало удаления сессии {session_id}")

    # 2. Удалить таблицу в ClickHouse
    ch_deleted = False
    try:
        ch_client = get_clickhouse_client()
        table_name = _get_session_table_name(session_id)
        database = settings.database.clickhouse.database

        # Проверить существование таблицы
        check_query = f"""
        SELECT count() FROM system.tables 
        WHERE database = '{database}' AND name = '{table_name}'
        """
        exists = ch_client.query(check_query).first_row[0] > 0

        if exists:
            # Получить количество строк для лога
            count_query = f"SELECT count() FROM {database}.`{table_name}`"
            row_count = ch_client.query(count_query).first_row[0]

            # Удалить таблицу
            drop_query = f"DROP TABLE IF EXISTS {database}.`{table_name}`"
            ch_client.command(drop_query)

            logger.info(f"🗑️ ClickHouse: удалена таблица {table_name} ({row_count:,} строк)")
            ch_deleted = True
        else:
            logger.info(f"⏭️ ClickHouse: таблица {table_name} не найдена")

        ch_client.close()

    except Exception as e:
        logger.error(f"❌ Ошибка удаления таблицы ClickHouse: {e}")
        # Не прерываем удаление — продолжаем с SQLite

    # 3. Удалить сессию из SQLite (каскадно удалит ParsingFile)
    try:
        # Сначала удалить связанные файлы (явно, для надёжности)
        db.query(ParsingFile).filter(
            ParsingFile.session_id == session.id
        ).delete(synchronize_session=False)

        # Затем удалить саму сессию
        db.delete(session)
        db.commit()

        logger.info(f"🗑️ SQLite: удалена сессия {session_id} и {session.total_files or 0} файлов")

    except Exception as e:
        db.rollback()
        logger.error(f"❌ Ошибка удаления из SQLite: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка удаления из базы: {str(e)}")

    return {
        "message": f"Сессия {session_id} успешно удалена",
        "deleted": {
            "session_id": session_id,
            "clickhouse_table_deleted": ch_deleted,
            "sqlite_records_deleted": True,
            "events_count": session.total_events or 0,
            "files_count": session.total_files or 0
        }
    }


@router.post("/sessions/{session_id}/comment", response_model=Dict[str, Any])
async def admin_update_comment(
        session_id: str,
        comment: str,
        db: Session = Depends(get_db)
):
    """[ADMIN] Обновить комментарий к сессии"""

    session = db.query(ParsingSession).filter(
        ParsingSession.session_id == session_id
    ).first()

    if not session:
        raise HTTPException(status_code=404, detail="Сессия не найдена")

    session.comment = comment
    db.commit()

    logger.info(f"✏️ ADMIN: Обновлён комментарий для сессии {session_id}")

    return {
        "session_id": session_id,
        "comment": session.comment,
        "updated_at": session.updated_at.isoformat() if session.updated_at else None
    }