# app/api/endpoints/analysis.py
"""
API эндпоинты для анализа результатов парсинга
"""

import logging
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from datetime import datetime
from typing import Optional, List, Dict, Any

from app.database import get_db, ParsingSession
from app.services.parsing_service import get_clickhouse_client
from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter()


def _get_session_table_name(session_id: str) -> str:
    """Получить имя таблицы ClickHouse для сессии"""
    safe_id = session_id.replace('-', '_')
    return f"tj_events_{safe_id}"


def _build_where_clause(
    event_name: Optional[str] = None,
    severity: Optional[str] = None,
    directory_name: Optional[str] = None,
    min_duration_ms: Optional[float] = None,
    max_duration_ms: Optional[float] = None,
    search: Optional[str] = None
) -> str:
    """
    Построить WHERE clause для ClickHouse запроса
    🔧 Исправлено: правильная обработка числовых параметров
    """
    conditions = []

    if event_name:
        conditions.append(f"event_name = '{event_name}'")

    if severity:
        conditions.append(f"severity = '{severity}'")

    if directory_name:
        conditions.append(f"directory_name = '{directory_name}'")

    # 🔧 ИСПРАВЛЕНО: правильная обработка float параметров
    if min_duration_ms is not None:
        try:
            min_val = float(min_duration_ms)
            conditions.append(f"duration_ms >= {min_val}")
        except (ValueError, TypeError):
            logger.warning(f"⚠️ Неверное значение min_duration_ms: {min_duration_ms}")

    if max_duration_ms is not None:
        try:
            max_val = float(max_duration_ms)
            conditions.append(f"duration_ms <= {max_val}")
        except (ValueError, TypeError):
            logger.warning(f"⚠️ Неверное значение max_duration_ms: {max_duration_ms}")

    # Поиск по тексту
    if search:
        search_escaped = search.replace("'", "''")
        conditions.append(
            f"(context LIKE '%{search_escaped}%' OR sql LIKE '%{search_escaped}%' OR exception LIKE '%{search_escaped}%')"
        )

    return " AND ".join(conditions) if conditions else "1=1"


@router.get("/{session_id}/event-types", response_model=List[str])
async def get_event_types(session_id: str, db: Session = Depends(get_db)):
    """Получить уникальные типы событий для сессии (для dropdown)"""
    try:
        ch_client = get_clickhouse_client()
        table_name = f"tj_events_{session_id.replace('-', '_')}"
        database = settings.database.clickhouse.database

        query = f"""
        SELECT DISTINCT event_name 
        FROM {database}.`{table_name}`
        WHERE event_name != '' AND event_name IS NOT NULL
        ORDER BY event_name ASC
        """
        result = ch_client.query(query)
        return sorted([row[0] for row in result.result_rows if row[0]])
    except Exception as e:
        logger.error(f"❌ Ошибка получения типов событий: {e}")
        return []


@router.get("/{session_id}")
async def get_analysis(
    session_id: str,
    event_name: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    directory_name: Optional[str] = Query(None),
    min_duration_ms: Optional[float] = Query(None),
    max_duration_ms: Optional[float] = Query(None),  # 🔧 НОВОЕ
    db: Session = Depends(get_db)
):
    """Получить аналитику по сессии парсинга"""

    # 1. Проверить что сессия существует
    session = db.query(ParsingSession).filter(
        ParsingSession.session_id == session_id
    ).first()

    if not session:
        raise HTTPException(status_code=404, detail="Сессия не найдена")

    # 2. Подключиться к ClickHouse
    try:
        ch_client = get_clickhouse_client()
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к ClickHouse: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка подключения к ClickHouse: {str(e)}")

    table_name = _get_session_table_name(session_id)
    database = settings.database.clickhouse.database

    # 3. Построить WHERE clause
    where_clause = _build_where_clause(
        event_name=event_name,
        severity=severity,
        directory_name=directory_name,
        min_duration_ms=min_duration_ms,
        max_duration_ms=max_duration_ms
    )

    logger.info(f"📊 Анализ сессии {session_id}, фильтр: {where_clause}")

    # 4. Общая статистика
    try:
        summary_query = f"""
        SELECT 
            count() as total,
            min(timestamp) as min_ts,
            max(timestamp) as max_ts,
            uniq(directory_name) as dir_count
        FROM {database}.`{table_name}`
        WHERE {where_clause}
        """
        summary_result = ch_client.query(summary_query).first_row

        total_events = summary_result[0] if summary_result[0] else 0
        min_ts = summary_result[1]
        max_ts = summary_result[2]
        dir_count = summary_result[3]

        logger.info(f"📊 Найдено событий: {total_events}")

    except Exception as e:
        logger.error(f"❌ Ошибка запроса статистики: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка запроса: {str(e)}")

    # 5. Распределение по типам событий
    try:
        event_types_query = f"""
        SELECT event_name, count() as cnt 
        FROM {database}.`{table_name}`
        WHERE {where_clause}
        GROUP BY event_name 
        ORDER BY cnt DESC 
        LIMIT 20
        """
        event_types_result = ch_client.query(event_types_query).result_rows
        event_types = {row[0]: row[1] for row in event_types_result}
    except Exception as e:
        logger.error(f"❌ Ошибка запроса типов событий: {e}")
        event_types = {}

    # 6. Распределение по серьёзности
    try:
        severity_query = f"""
        SELECT severity, count() as cnt 
        FROM {database}.`{table_name}`
        WHERE {where_clause}
        GROUP BY severity 
        ORDER BY cnt DESC
        """
        severity_result = ch_client.query(severity_query).result_rows
        severity_dist = {row[0]: row[1] for row in severity_result}
    except Exception as e:
        logger.error(f"❌ Ошибка запроса серьёзности: {e}")
        severity_dist = {}

    # 7. Список инфобаз
    try:
        directories_query = f"""
        SELECT DISTINCT directory_name 
        FROM {database}.`{table_name}`
        WHERE {where_clause}
        ORDER BY directory_name
        """
        directories_result = ch_client.query(directories_query).result_rows
        directories = [row[0] for row in directories_result]
    except Exception as e:
        logger.error(f"❌ Ошибка запроса инфобаз: {e}")
        directories = []

    # 🔧 8. Ошибки MS SQL (вместо "Топ ошибок")
    try:
        mssql_errors_query = f"""
        SELECT timestamp, event_name, duration_ms, mssql_error_code, directory_name, source_file,
               usr, context, sql, exception, severity
        FROM {database}.`{table_name}`
        WHERE {where_clause} 
          AND mssql_error_code IS NOT NULL 
          AND mssql_error_code > 0
        ORDER BY timestamp DESC
        LIMIT 50
        """
        mssql_result = ch_client.query(mssql_errors_query).result_rows
        mssql_errors = []
        for row in mssql_result:
            mssql_errors.append({
                "timestamp": row[0].isoformat() if row[0] else "",
                "event_name": row[1],
                "duration_ms": float(row[2]) if row[2] else 0,
                "mssql_error_code": row[3],
                "directory_name": row[4],
                "source_file": row[5],
                "usr": row[6],
                "context": row[7][:500] if row[7] else None,
                "sql": row[8][:500] if row[8] else None,
                "exception": row[9][:500] if row[9] else None,
                "severity": row[10]
            })
    except Exception as e:
        logger.error(f"❌ Ошибка запроса ошибок MS SQL: {e}")
        mssql_errors = []

    # 9. Топ медленных
    try:
        slow_query = f"""
        SELECT timestamp, event_name, duration_ms, directory_name, source_file,
               usr, context, sql, exception, severity
        FROM {database}.`{table_name}`
        WHERE {where_clause} AND duration_ms > 1000
        ORDER BY duration_ms DESC
        LIMIT 50
        """
        slow_result = ch_client.query(slow_query).result_rows
        top_slow = []
        for row in slow_result:
            top_slow.append({
                "timestamp": row[0].isoformat() if row[0] else "",
                "event_name": row[1],
                "duration_ms": float(row[2]) if row[2] else 0,
                "directory_name": row[3],
                "source_file": row[4],
                "usr": row[5],
                "context": row[6][:500] if row[6] else None,
                "sql": row[7][:500] if row[7] else None,
                "exception": row[8][:500] if row[8] else None,
                "severity": row[9]
            })
    except Exception as e:
        logger.error(f"❌ Ошибка запроса топ медленных: {e}")
        top_slow = []

    # 🔧 10. Временная шкала ПО СЕКУНДАМ (не по минутам)
    try:
        timeline_query = f"""
        SELECT 
            toStartOfSecond(timestamp) as second,
            count() as cnt,
            avg(duration_ms) as avg_dur
        FROM {database}.`{table_name}`
        WHERE {where_clause}
        GROUP BY second
        ORDER BY second ASC
        LIMIT 300
        """
        timeline_result = ch_client.query(timeline_query).result_rows
        timeline = []
        for row in timeline_result:
            timeline.append({
                "timestamp": row[0].isoformat() if row[0] else "",
                "count": row[1],
                "avg_duration_ms": round(float(row[2]), 2) if row[2] else 0
            })
    except Exception as e:
        logger.error(f"❌ Ошибка запроса временной шкалы: {e}")
        timeline = []

    # 11. Рассчитать процент фильтрации
    filter_percent = 0
    if session.total_events > 0:
        filtered = session.total_events - total_events
        filter_percent = round(100 * filtered / session.total_events, 1)

    # 12. Сформировать ответ
    return {
        "summary": {
            "session_id": session_id,
            "total_events": total_events,
            "total_filtered": session.total_events - total_events if session.total_events > total_events else 0,
            "filter_percent": filter_percent,
            "time_range": {
                "start": min_ts.isoformat() if min_ts else session.start_date.isoformat(),
                "end": max_ts.isoformat() if max_ts else session.end_date.isoformat()
            },
            "directories": directories,
            "event_types": event_types,
            "severity_distribution": severity_dist,
            "mssql_errors": mssql_errors[:20],  # 🔧 НОВОЕ: вместо top_errors
            "top_slow": top_slow[:20],
            "timeline": timeline
        },
        "filters_applied": {
            "event_name": event_name,
            "severity": severity,
            "directory_name": directory_name,
            "min_duration_ms": min_duration_ms,
            "max_duration_ms": max_duration_ms
        }
    }


@router.get("/{session_id}/events", response_model=Dict[str, Any])
async def get_analysis_events(
    session_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=1000),
    event_name: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    directory_name: Optional[str] = Query(None),
    min_duration_ms: Optional[float] = Query(None),
    max_duration_ms: Optional[float] = Query(None),  # 🔧 НОВОЕ
    search: Optional[str] = Query(None, description="Поиск по тексту"),
    db: Session = Depends(get_db)
):
    """Получить список событий с пагинацией и фильтрами"""

    # Проверка сессии
    session = db.query(ParsingSession).filter(
        ParsingSession.session_id == session_id
    ).first()

    if not session:
        raise HTTPException(status_code=404, detail="Сессия не найдена")

    # Подключение к ClickHouse
    try:
        ch_client = get_clickhouse_client()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка подключения: {str(e)}")

    table_name = _get_session_table_name(session_id)
    database = settings.database.clickhouse.database

    # Построить WHERE clause
    where_clause = _build_where_clause(
        event_name=event_name,
        severity=severity,
        directory_name=directory_name,
        min_duration_ms=min_duration_ms,
        max_duration_ms=max_duration_ms,
        search=search
    )

    logger.info(f"📋 Запрос событий: session={session_id}, where={where_clause}")

    # Пагинация
    offset = (page - 1) * page_size

    # Запрос событий
    try:
        events_query = f"""
        SELECT timestamp, event_name, level, duration_ms, directory_name, source_file,
               usr, context, sql, exception, severity, mssql_error_code
        FROM {database}.`{table_name}`
        WHERE {where_clause}
        ORDER BY timestamp DESC
        LIMIT {page_size} OFFSET {offset}
        """

        logger.info(f"🔍 Выполняю запрос: {events_query[:500]}...")
        events_result = ch_client.query(events_query).result_rows

        events = []
        for row in events_result:
            events.append({
                "timestamp": row[0].isoformat() if row[0] else "",
                "event_name": row[1],
                "level": row[2],
                "duration_ms": float(row[3]) if row[3] else 0,
                "directory_name": row[4],
                "source_file": row[5],
                "usr": row[6],
                "context": row[7][:1000] if row[7] else None,
                "sql": row[8][:1000] if row[8] else None,
                "exception": row[9][:1000] if row[9] else None,
                "severity": row[10],
                "mssql_error_code": row[11] if len(row) > 10 else None
            })

        logger.info(f"✅ Найдено {len(events)} событий")

    except Exception as e:
        logger.error(f"❌ Ошибка запроса событий: {e}")
        events = []

    # Общее количество для пагинации
    try:
        count_query = f"""
        SELECT count() FROM {database}.`{table_name}`
        WHERE {where_clause}
        """
        total = ch_client.query(count_query).first_row[0]
    except Exception as e:
        logger.error(f"❌ Ошибка подсчёта: {e}")
        total = 0

    return {
        "events": events,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if page_size > 0 else 0,
        "filters": {
            "event_name": event_name,
            "severity": severity,
            "directory_name": directory_name,
            "min_duration_ms": min_duration_ms,
            "max_duration_ms": max_duration_ms,
            "search": search
        }
    }