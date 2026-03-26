# app/api/endpoints/events.py
"""
API эндпоинты для работы с событиями из ClickHouse
"""

import logging
from fastapi import APIRouter, Depends, HTTPException, Query, Response  # 🔧 Добавили Response
from sqlalchemy.orm import Session
from typing import Optional, List, Dict, Any
from datetime import datetime

from app.database import get_db, ParsingSession
from app.services.parsing_service import get_clickhouse_client
from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter()


def _get_session_table_name(session_id: str) -> str:
    """Получить имя таблицы ClickHouse для сессии"""
    safe_id = session_id.replace('-', '_')
    return f"tj_events_{safe_id}"


@router.get("/event-types", response_model=List[str])
async def get_event_types(
        session_id: Optional[str] = Query(None, description="ID сессии для фильтрации")
):
    """
    Получить список всех уникальных типов событий (event_name) из ClickHouse
    🔧 Без проверки сессии в SQLite — работает напрямую с таблицей
    """
    try:
        ch_client = get_clickhouse_client()
        database = settings.database.clickhouse.database

        if not session_id:
            logger.info("📋 event-types запрошен без session_id")
            return []

        # Формируем имя таблицы
        table_name = f"tj_events_{session_id.replace('-', '_')}"

        # 🔧 Проверяем что таблица существует
        try:
            check_query = f"SELECT 1 FROM {database}.`{table_name}` LIMIT 1"
            ch_client.query(check_query)
        except Exception as table_err:
            logger.info(f"📋 Таблица {table_name} не найдена: {table_err}")
            return []

        # Запрос уникальных типов событий
        query = f"""
        SELECT DISTINCT event_name 
        FROM {database}.`{table_name}`
        WHERE event_name != '' AND event_name IS NOT NULL
        ORDER BY event_name ASC
        """

        result = ch_client.query(query)
        event_types = [row[0] for row in result.result_rows if row[0]]

        logger.info(f"📋 Найдено {len(event_types)} типов событий для сессии {session_id[:8]}...")
        return sorted(event_types)

    except Exception as e:
        logger.error(f"❌ Ошибка получения типов событий: {e}")
        return ['DBMSSQL', 'SDBL', 'EXCP', 'TLOCK', 'HTTP', 'V8DBConnection', 'Usr', 'Session']


@router.get("/users", response_model=List[str])
async def get_event_users(
        session_id: Optional[str] = Query(None, description="ID сессии для фильтрации")
):
    """
    Получить список всех уникальных пользователей (usr) из событий

    Если указан session_id — возвращает пользователей только из этой сессии.
    """
    try:
        ch_client = get_clickhouse_client()
        database = settings.database.clickhouse.database

        if not session_id:
            logger.info("👤 users запрошен без session_id")
            return []

        table_name = f"tj_events_{session_id.replace('-', '_')}"

        # Проверяем существование таблицы
        try:
            check_query = f"SELECT 1 FROM {database}.`{table_name}` LIMIT 1"
            ch_client.query(check_query)
        except Exception:
            logger.info(f"👤 Таблица {table_name} не найдена")
            return []

        # Запрос уникальных пользователей
        query = f"""
        SELECT DISTINCT usr 
        FROM {database}.`{table_name}`
        WHERE usr != '' AND usr IS NOT NULL
        ORDER BY usr ASC
        """

        result = ch_client.query(query)
        users = [row[0] for row in result.result_rows if row[0]]

        logger.info(f"👤 Найдено {len(users)} пользователей для сессии {session_id[:8]}...")
        return sorted(users)

    except Exception as e:
        logger.error(f"❌ Ошибка получения пользователей: {e}")
        return []


@router.get("/mssql-errors", response_model=List[Dict[str, Any]])
async def get_mssql_error_codes(
        session_id: Optional[str] = Query(None, description="ID сессии для фильтрации")
):
    """
    Получить список уникальных кодов ошибок MSSQL с описаниями
    """
    try:
        ch_client = get_clickhouse_client()
        database = settings.database.clickhouse.database

        if not session_id:
            return []

        table_name = f"tj_events_{session_id.replace('-', '_')}"

        # Проверяем существование таблицы
        try:
            check_query = f"SELECT 1 FROM {database}.`{table_name}` LIMIT 1"
            ch_client.query(check_query)
        except Exception:
            return []

        # Запрос уникальных кодов ошибок с количеством
        query = f"""
        SELECT mssql_error_code, count() as cnt
        FROM {database}.`{table_name}`
        WHERE mssql_error_code IS NOT NULL AND mssql_error_code > 0
        GROUP BY mssql_error_code
        ORDER BY cnt DESC
        LIMIT 50
        """

        result = ch_client.query(query)
        errors = []

        # Описания частых ошибок
        descriptions = {
            1205: 'Deadlock victim',
            2627: 'Unique constraint violation',
            547: 'Foreign key violation',
            2601: 'Duplicate key',
            1222: 'Lock timeout',
            18456: 'Login failed',
            4060: 'Cannot open database',
            208: 'Invalid object name',
            207: 'Invalid column name',
            102: 'Syntax error'
        }

        for row in result.result_rows:
            code = row[0]
            count = row[1]
            errors.append({
                'code': code,
                'count': count,
                'description': descriptions.get(code, 'Unknown error'),
                'label': f'{code} — {descriptions.get(code, "Unknown")} ({count})'
            })

        logger.info(f"⚠️ Найдено {len(errors)} кодов ошибок MSSQL")
        return errors

    except Exception as e:
        logger.error(f"❌ Ошибка получения кодов ошибок: {e}")
        return []


@router.get("/{session_id}", response_model=Dict[str, Any])
async def get_events(
        session_id: str,
        event_name: Optional[str] = Query(None, description="Фильтр по типу события"),
        severity: Optional[str] = Query(None, description="Фильтр по серьёзности"),
        category: Optional[str] = Query(None, description="Фильтр по категории"),
        min_duration_ms: Optional[float] = Query(None),
        max_duration_ms: Optional[float] = Query(None),
        directory_name: Optional[str] = Query(None, description="Фильтр по инфобазе"),
        duration_min: Optional[float] = Query(None, description="Мин. длительность (мс)"),
        duration_max: Optional[float] = Query(None, description="Макс. длительность (мс)"),
        search: Optional[str] = Query(None, description="Поиск по тексту (context, sql, exception)"),
        user: Optional[str] = Query(None, description="Фильтр по пользователю"),
        mssql_error: Optional[int] = Query(None, description="Фильтр по коду ошибки MSSQL (например, 1205)"),
        # 🔧 НОВОЕ: Фильтры по времени с миллисекундами
        time_from: Optional[str] = Query(None, description="Время от: YYYY-MM-DD HH:MM:SS.mmm"),
        time_to: Optional[str] = Query(None, description="Время до: YYYY-MM-DD HH:MM:SS.mmm"),
        page: int = Query(1, ge=1, description="Номер страницы"),
        page_size: int = Query(50, ge=1, le=1000, description="Размер страницы"),
        sort_by: Optional[str] = Query('timestamp', description="Поле для сортировки"),
        sort_order: Optional[str] = Query('desc', description="Направление: asc/desc"),
        db: Session = Depends(get_db)
):
    """Получить события из ClickHouse с фильтрацией и поиском"""

    # 1. Проверить что сессия существует в SQLite
    session = db.query(ParsingSession).filter(
        ParsingSession.session_id == session_id
    ).first()

    # 🔧 Валидация параметров сортировки (защита от SQL injection)
    allowed_sort_columns = {
        'timestamp': 'timestamp',
        'duration_ms': 'duration_ms',
        'event_name': 'event_name',
        'level': 'level'
    }

    sort_column = allowed_sort_columns.get(sort_by, 'timestamp')
    sort_direction = 'ASC' if sort_order.lower() == 'asc' else 'DESC'

    if not session:
        raise HTTPException(status_code=404, detail="Сессия не найдена")

    # 2. Получить имя таблицы для сессии
    table_name = _get_session_table_name(session_id)
    database = settings.database.clickhouse.database

    # 🔧 3. Установить значения по умолчанию для time_from/time_to из интервала сессии
    # Если параметры не переданы — используем границы сессии
    if time_from is None and session.start_date:
        # Форматируем в формат: YYYY-MM-DD HH:MM:SS.mmm
        time_from = session.start_date.strftime('%Y-%m-%d %H:%M:%S.') + f'{session.start_date.microsecond // 1000:03d}'

    if time_to is None and session.end_date:
        time_to = session.end_date.strftime('%Y-%m-%d %H:%M:%S.') + f'{session.end_date.microsecond // 1000:03d}'

    # 4. Построить WHERE clause
    where_conditions = []

    # 🔧 Вспомогательная функция для экранирования строк (защита от SQL injection)
    def escape_sql_string(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return value.replace("'", "''")

    # Строковые фильтры (с экранированием)
    if event_name:
        where_conditions.append(f"event_name = '{escape_sql_string(event_name)}'")
    if severity:
        where_conditions.append(f"severity = '{escape_sql_string(severity)}'")
    if category:
        where_conditions.append(f"category = '{escape_sql_string(category)}'")
    if directory_name:
        where_conditions.append(f"directory_name = '{escape_sql_string(directory_name)}'")
    if user:
        where_conditions.append(f"usr = '{escape_sql_string(user)}'")

    # Числовые фильтры (безопасны, т.к. типизированы)
    if duration_min is not None:
        where_conditions.append(f"duration_ms >= {float(duration_min)}")
    if duration_max is not None:
        where_conditions.append(f"duration_ms <= {float(duration_max)}")
    if min_duration_ms is not None:
        where_conditions.append(f"duration_ms >= {float(min_duration_ms)}")
    if max_duration_ms is not None:
        where_conditions.append(f"duration_ms <= {float(max_duration_ms)}")

    # 🔧 Фильтр по коду ошибки MSSQL
    if mssql_error is not None:
        where_conditions.append(f"mssql_error_code = {int(mssql_error)}")

    # 🔧 ФИЛЬТРЫ ПО ВРЕМЕНИ (с миллисекундами)
    # ClickHouse функция parseDateTimeBestEffort поддерживает формат с миллисекундами
    if time_from:
        # Экранирование не нужно, т.к. формат строго фиксирован и валидируется на фронтенде
        where_conditions.append(f"timestamp >= parseDateTimeBestEffort('{escape_sql_string(time_from)}')")

    if time_to:
        where_conditions.append(f"timestamp <= parseDateTimeBestEffort('{escape_sql_string(time_to)}')")

    # 🔍 ПОИСК ПО ТЕКСТУ
    if search:
        search_escaped = escape_sql_string(search)
        where_conditions.append(
            f"(context LIKE '%{search_escaped}%' OR sql LIKE '%{search_escaped}%' OR sdbl LIKE '%{search_escaped}%')"
        )

    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

    logger.info(
        f"📋 Запрос событий: session={session_id}, time_from={time_from}, time_to={time_to}, where={where_clause}")

    # 5. Подключиться к ClickHouse
    ch_client = get_clickhouse_client()
    offset = (page - 1) * page_size

    # 6. Запрос к ClickHouse с пагинацией
    query = f"""
    SELECT 
        timestamp, event_name, level, duration_ms,
        p_processName, usr, tableName, context, sql, exception,
        severity, category, directory_name, source_file, line_number,
        t_computerName, t_connectID, dbpid, osThread, sessionID,
        trans, func, locks, waitConnections, deadlockConnectionIntersections,
        lkaid, lka, lkp, lkpid, lksrc,
        rows, rowsAffected, description, data, planSQLText, sdbl,
        mssql_error_code
    FROM {database}.`{table_name}`
    WHERE {where_clause}
    ORDER BY {sort_column} {sort_direction}
    LIMIT {page_size} OFFSET {offset}
    """

    try:
        logger.info(f"🔍 Выполняю запрос: {query[:500]}...")
        result = ch_client.query(query)
    except Exception as e:
        logger.error(f"❌ Ошибка запроса к ClickHouse: {e}")
        if "Unknown table" in str(e) or "404" in str(e):
            return {
                'events': [],
                'total': 0,
                'page': page,
                'page_size': page_size,
                'total_pages': 0,
                'table': table_name
            }
        raise HTTPException(status_code=500, detail=f"Ошибка ClickHouse: {str(e)}")

    # 7. Преобразовать результаты
    events = []
    for row in result.result_rows:
        events.append({
            'timestamp': row[0].isoformat() if row[0] else None,
            'event_name': row[1],
            'level': row[2],
            'duration_ms': float(row[3]) if row[3] else 0,
            'process_name': row[4],
            'user_name': row[5],
            'table_name': row[6],
            'context': row[7][:1000] if row[7] else None,
            'sql': row[8][:1000] if row[8] else None,
            'exception': row[9][:1000] if row[9] else None,
            'severity': row[10],
            'category': row[11],
            'directory_name': row[12],
            'source_file': row[13],
            'line_number': row[14],
            'computerName': row[15],
            'connectID': row[16],
            'dbpid': row[17],
            'osThread': row[18],
            'sessionID': row[19],
            'trans': row[20],
            'func': row[21],
            'locks': row[22],
            'waitConnections': row[23],
            'deadlockConnectionIntersections': row[24],
            'lkaid': row[25],
            'lka': row[26],
            'lkp': row[27],
            'lkpid': row[28],
            'lksrc': row[29],
            'rows': row[30],
            'rowsAffected': row[31],
            'description': row[32],
            'data': row[33],
            'planSQLText': row[34][:1000] if row[34] else None,
            'sdbl': row[35][:1000] if row[35] else None,
            'mssql_error_code': row[36] if len(row) > 36 else None,
        })

    # 8. Получить общее количество (для пагинации)
    count_query = f"""
    SELECT count() FROM {database}.`{table_name}`
    WHERE {where_clause}
    """
    try:
        total = ch_client.query(count_query).first_row[0]
    except:
        total = 0

    # 🔧 3. Установить значения по умолчанию для time_from/time_to из интервала сессии
    if time_from is None and session.start_date:
        # Конвертируем datetime в строку с миллисекундами
        time_from = session.start_date.strftime('%Y-%m-%d %H:%M:%S.') + f'{session.start_date.microsecond // 1000:03d}'
        logger.info(f"🕐 time_from из сессии: {time_from}")

    if time_to is None and session.end_date:
        time_to = session.end_date.strftime('%Y-%m-%d %H:%M:%S.') + f'{session.end_date.microsecond // 1000:03d}'
        logger.info(f"🕐 time_to из сессии: {time_to}")

    logger.info(
        f"✅ Найдено {len(events)} событий (из {total} всего), страница {page}/{(total + page_size - 1) // page_size if page_size > 0 else 1}")

    return {
        'events': events,
        'total': total,
        'page': page,
        'page_size': page_size,
        'total_pages': (total + page_size - 1) // page_size if page_size > 0 else 0,
        'table': table_name,
        'filters': {
            'event_name': event_name,
            'severity': severity,
            'category': category,
            'directory_name': directory_name,
            'duration_min': duration_min,
            'duration_max': duration_max,
            'search': search,
            'mssql_error': mssql_error,
            # 🔧 Возвращаем временные фильтры (для сохранения состояния на фронтенде)
            'time_from': time_from,
            'time_to': time_to,
            # 🔧 Границы сессии (для инициализации полей на фронтенде при первом заходе)
            'session_start': session.start_date.strftime(
                '%Y-%m-%d %H:%M:%S.') + f'{session.start_date.microsecond // 1000:03d}' if session.start_date else None,
            'session_end': session.end_date.strftime(
                '%Y-%m-%d %H:%M:%S.') + f'{session.end_date.microsecond // 1000:03d}' if session.end_date else None,
        }
    }

@router.get("/{session_id}/detail/{event_index}", response_model=Dict[str, Any])
async def get_event_detail(
        session_id: str,
        event_index: int,
        db: Session = Depends(get_db)
):
    """
    Получить детальную информацию о событии по индексу
    🔧 ClickHouse не имеет автоинкрементного id, используем LIMIT/OFFSET
    """

    # Проверка сессии
    session = db.query(ParsingSession).filter(
        ParsingSession.session_id == session_id
    ).first()

    if not session:
        raise HTTPException(status_code=404, detail="Сессия не найдена")

    table_name = _get_session_table_name(session_id)
    database = settings.database.clickhouse.database
    ch_client = get_clickhouse_client()

    # 🔧 Запрос с ограничением по индексу
    query = f"""
    SELECT 
        timestamp, event_name, level, duration_ms,
        p_processName, t_computerName, t_connectID, usr, dbpid, osThread,
        sessionID, trans, func, tableName,
        context, sql, sdbl, planSQLText, exception,
        locks, waitConnections, deadlockConnectionIntersections,
        lkaid, lka, lkp, lkpid, lksrc,
        rows, rowsAffected, description, data,
        severity, category, directory_name, source_file, line_number
    FROM {database}.`{table_name}`
    ORDER BY timestamp ASC
    LIMIT 1 OFFSET {event_index}
    """

    logger.info(f"🔍 Детальный запрос события: index={event_index}, table={table_name}")

    try:
        result = ch_client.query(query)
    except Exception as e:
        logger.error(f"❌ Ошибка запроса детали события: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка ClickHouse: {str(e)}")

    if not result.result_rows:
        logger.warning(f"⚠️ Событие не найдено: index={event_index}")
        raise HTTPException(status_code=404, detail="Событие не найдено")

    row = result.result_rows[0]
    columns = result.column_names
    event = dict(zip(columns, row))

    # Преобразовать в удобный формат
    return {
        'index': event_index,
        'timestamp': event.get('timestamp').isoformat() if event.get('timestamp') else None,
        'event_name': event.get('event_name'),
        'level': event.get('level'),
        'duration_ms': float(event.get('duration_ms', 0)),
        'process_name': event.get('p_processName'),
        'computer_name': event.get('t_computerName'),
        'connect_id': event.get('t_connectID'),
        'user_name': event.get('usr'),
        'dbpid': event.get('dbpid'),
        'os_thread': event.get('osThread'),
        'session_id': event.get('sessionID'),
        'trans': event.get('trans'),
        'func': event.get('func'),
        'table_name': event.get('tableName'),
        'context': event.get('context'),
        'sql': event.get('sql'),
        'sdbl': event.get('sdbl'),
        'plan_sql_text': event.get('planSQLText'),
        'exception': event.get('exception'),
        'locks': event.get('locks'),
        'wait_connections': event.get('waitConnections'),
        'deadlock_intersections': event.get('deadlockConnectionIntersections'),
        'lkaid': event.get('lkaid'),
        'lka': event.get('lka'),
        'lkp': event.get('lkp'),
        'lkpid': event.get('lkpid'),
        'lksrc': event.get('lksrc'),
        'rows': event.get('rows'),
        'rows_affected': event.get('rowsAffected'),
        'description': event.get('description'),
        'data': event.get('data'),
        'severity': event.get('severity', 'info'),
        'category': event.get('category', 'other'),
        'directory_name': event.get('directory_name'),
        'source_file': event.get('source_file'),
        'line_number': event.get('line_number')
    }


# app/api/endpoints/events.py — добавьте в конец файла

@router.get("/{session_id}/report", response_model=Dict[str, Any])
async def get_session_report(
        session_id: str,
        db: Session = Depends(get_db)
):
    """
    [REPORT] Получить детальную статистику по сессии

    Возвращает агрегированные данные для отчёта:
    - Общая статистика
    - Распределение по типам/пользователям/инфобазам
    - Статистика длительности
    - Ошибки MS SQL
    """

    # Проверка сессии
    session = db.query(ParsingSession).filter(
        ParsingSession.session_id == session_id
    ).first()

    if not session:
        raise HTTPException(status_code=404, detail="Сессия не найдена")

    try:
        ch_client = get_clickhouse_client()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка подключения к ClickHouse: {str(e)}")

    table_name = _get_session_table_name(session_id)
    database = settings.database.clickhouse.database

    result = {}

    # 1. Общая статистика
    try:
        summary_query = f"""
        SELECT 
            count() as total,
            uniq(usr) as unique_users,
            uniq(directory_name) as unique_dirs,
            countIf(mssql_error_code IS NOT NULL AND mssql_error_code > 0) as mssql_errors,
            min(timestamp) as min_ts,
            max(timestamp) as max_ts
        FROM {database}.`{table_name}`
        """
        summary = ch_client.query(summary_query).first_row

        total_events = summary[0] or 0
        duration_seconds = None
        if summary[4] and summary[5]:  # min_ts, max_ts
            duration_seconds = (summary[5] - summary[4]).total_seconds()

        result.update({
            'total_events': total_events,
            'unique_users': summary[1] or 0,
            'unique_directories': summary[2] or 0,
            'mssql_errors_count': summary[3] or 0,
            'time_range': {
                'start': summary[4].isoformat() if summary[4] else None,
                'end': summary[5].isoformat() if summary[5] else None
            },
            'duration_seconds': duration_seconds,
            'events_per_second': round(total_events / duration_seconds,
                                       2) if duration_seconds and duration_seconds > 0 else None,
            'events_per_minute': round(total_events / (duration_seconds / 60),
                                       1) if duration_seconds and duration_seconds > 0 else None
        })
    except Exception as e:
        logger.error(f"❌ Ошибка общей статистики: {e}")
        result.update({
            'total_events': 0,
            'unique_users': 0,
            'unique_directories': 0,
            'mssql_errors_count': 0,
            'time_range': {'start': None, 'end': None},
            'duration_seconds': None
        })

    # 2. Распределение по типам событий
    try:
        types_query = f"""
        SELECT event_name, count() as cnt 
        FROM {database}.`{table_name}`
        WHERE event_name != ''
        GROUP BY event_name 
        ORDER BY cnt DESC 
        LIMIT 20
        """
        types_result = ch_client.query(types_query).result_rows
        result['event_types'] = {row[0]: row[1] for row in types_result}
    except:
        result['event_types'] = {}

    # 3. Статистика длительности — ИСПРАВЛЕНО
    try:
        duration_query = f"""
        SELECT 
            min(duration_ms) as min_dur,
            avg(duration_ms) as avg_dur,
            max(duration_ms) as max_dur,
            quantile(0.5)(duration_ms) as median_dur,
            quantile(0.95)(duration_ms) as p95_dur
        FROM {database}.`{table_name}`
        WHERE duration_ms IS NOT NULL 
          AND duration_ms > 0
          AND duration_ms < 10000000  -- 🔧 Исключаем аномально большие значения (>2.7 часа)
        """
        dur_result = ch_client.query(duration_query).first_row

        # 🔧 ИСПРАВЛЕНО: правильный порядок индексов + проверка на None вместо 0
        result['duration_stats'] = {
            'min': float(dur_result[0]) if dur_result[0] is not None else None,  # [0] = min ✓
            'avg': float(dur_result[1]) if dur_result[1] is not None else None,  # [1] = avg ✓
            'max': float(dur_result[2]) if dur_result[2] is not None else None,  # [2] = max ✓ (было ошибочно median)
            'median': float(dur_result[3]) if dur_result[3] is not None else None,  # [3] = median ✓ (было ошибочно max)
            'p95': float(dur_result[4]) if dur_result[4] is not None else None  # [4] = p95 ✓
        }

    except Exception as e:
        logger.error(f"❌ Ошибка статистики длительности: {e}")
        result['duration_stats'] = {
            'min': None, 'avg': None, 'max': None, 'median': None, 'p95': None
        }


    # 4. Ошибки MS SQL
    try:
        mssql_query = f"""
        SELECT mssql_error_code, count() as cnt 
        FROM {database}.`{table_name}`
        WHERE mssql_error_code IS NOT NULL AND mssql_error_code > 0
        GROUP BY mssql_error_code 
        ORDER BY cnt DESC
        LIMIT 20
        """
        mssql_result = ch_client.query(mssql_query).result_rows
        result['mssql_error_codes'] = [
            {'code': row[0], 'count': row[1]}
            for row in mssql_result
        ]
    except:
        result['mssql_error_codes'] = []

    # 5. Топ пользователей
    try:
        users_query = f"""
        SELECT usr, count() as cnt 
        FROM {database}.`{table_name}`
        WHERE usr != '' AND usr IS NOT NULL
        GROUP BY usr 
        ORDER BY cnt DESC 
        LIMIT 10
        """
        users_result = ch_client.query(users_query).result_rows
        result['top_users'] = [
            {'usr': row[0], 'count': row[1]}
            for row in users_result
        ]
    except:
        result['top_users'] = []

    ch_client.close()
    logger.info(f"📊 Отчёт сгенерирован для сессии {session_id[:8]}...")

    return result


@router.get("/{session_id}/export")
async def export_events(
        session_id: str,
        format: str = Query("csv", description="Формат: csv или xlsx"),
        event_name: Optional[str] = Query(None),
        severity: Optional[str] = Query(None),
        category: Optional[str] = Query(None),
        min_duration_ms: Optional[float] = Query(None),
        max_duration_ms: Optional[float] = Query(None),
        directory_name: Optional[str] = Query(None),
        search: Optional[str] = Query(None),
        user: Optional[str] = Query(None),
        mssql_error: Optional[int] = Query(None),
        time_from: Optional[str] = Query(None),
        time_to: Optional[str] = Query(None),
        db: Session = Depends(get_db)
):
    """Экспорт событий в CSV или XLSX"""

    # Проверка сессии
    session = db.query(ParsingSession).filter(
        ParsingSession.session_id == session_id
    ).first()

    if not session:
        raise HTTPException(status_code=404, detail="Сессия не найдена")

    # Получение имени таблицы
    table_name = _get_session_table_name(session_id)
    database = settings.database.clickhouse.database
    ch_client = get_clickhouse_client()

    # Построение WHERE clause (аналогично get_events)
    where_conditions = []

    def escape_sql_string(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return value.replace("'", "''")

    if event_name:
        where_conditions.append(f"event_name = '{escape_sql_string(event_name)}'")
    if severity:
        where_conditions.append(f"severity = '{escape_sql_string(severity)}'")
    if category:
        where_conditions.append(f"category = '{escape_sql_string(category)}'")
    if directory_name:
        where_conditions.append(f"directory_name = '{escape_sql_string(directory_name)}'")
    if user:
        where_conditions.append(f"usr = '{escape_sql_string(user)}'")
    if min_duration_ms is not None:
        where_conditions.append(f"duration_ms >= {float(min_duration_ms)}")
    if max_duration_ms is not None:
        where_conditions.append(f"duration_ms <= {float(max_duration_ms)}")
    if mssql_error is not None:
        where_conditions.append(f"mssql_error_code = {int(mssql_error)}")
    if time_from:
        where_conditions.append(f"timestamp >= parseDateTimeBestEffort('{escape_sql_string(time_from)}')")
    if time_to:
        where_conditions.append(f"timestamp <= parseDateTimeBestEffort('{escape_sql_string(time_to)}')")
    if search:
        search_escaped = escape_sql_string(search)
        where_conditions.append(
            f"(context LIKE '%{search_escaped}%' OR sql LIKE '%{search_escaped}%' OR sdbl LIKE '%{search_escaped}%')"
        )

    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

    # Запрос всех событий (без пагинации для экспорта)
    # 🔧 Ограничим 10000 записей для производительности
    query = f"""
    SELECT 
        timestamp, event_name, level, duration_ms,
        p_processName, usr, tableName, context, sql, exception,
        severity, category, directory_name, source_file, line_number,
        t_computerName, t_connectID, dbpid, osThread, sessionID,
        trans, func, locks, waitConnections, deadlockConnectionIntersections,
        lkaid, lka, lkp, lkpid, lksrc,
        rows, rowsAffected, description, data, planSQLText, sdbl,
        mssql_error_code
    FROM {database}.`{table_name}`
    WHERE {where_clause}
    ORDER BY timestamp DESC
    LIMIT 10000
    """

    logger.info(f"📤 Экспорт событий: format={format}, where={where_clause}")

    try:
        result = ch_client.query(query)
    except Exception as e:
        logger.error(f"❌ Ошибка экспорта: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка ClickHouse: {str(e)}")

    # Подготовка данных
    columns = [
        'Время', 'Тип события', 'Уровень', 'Длительность (мс)',
        'Процесс', 'Пользователь', 'Таблица', 'Контекст', 'SQL', 'Исключение',
        'Severity', 'Категория', 'Инфобаза', 'Файл', 'Строка',
        'Компьютер', 'Connect ID', 'DB PID', 'OS Thread', 'Session ID',
        'Trans', 'Func', 'Locks', 'Wait Connections', 'Deadlock Intersections',
        'lkaid', 'lka', 'lkp', 'lkpid', 'lksrc',
        'Rows', 'Rows Affected', 'Описание', 'Data', 'Plan SQL', 'SDBL',
        'Код ошибки MSSQL'
    ]

    rows = []
    for row in result.result_rows:
        rows.append([
            row[0].isoformat() if row[0] else '',  # timestamp
            row[1] or '',  # event_name
            row[2] or '',  # level
            float(row[3]) if row[3] else 0,  # duration_ms
            row[4] or '',  # p_processName
            row[5] or '',  # usr
            row[6] or '',  # tableName
            (row[7] or '')[:1000],  # context
            (row[8] or '')[:1000],  # sql
            (row[9] or '')[:1000],  # exception
            row[10] or '',  # severity
            row[11] or '',  # category
            row[12] or '',  # directory_name
            row[13] or '',  # source_file
            row[14] or '',  # line_number
            row[15] or '',  # t_computerName
            row[16] or '',  # t_connectID
            row[17] or '',  # dbpid
            row[18] or '',  # osThread
            row[19] or '',  # sessionID
            row[20] or '',  # trans
            row[21] or '',  # func
            row[22] or '',  # locks
            row[23] or '',  # waitConnections
            row[24] or '',  # deadlockConnectionIntersections
            row[25] or '',  # lkaid
            row[26] or '',  # lka
            row[27] or '',  # lkp
            row[28] or '',  # lkpid
            row[29] or '',  # lksrc
            row[30] or '',  # rows
            row[31] or '',  # rowsAffected
            row[32] or '',  # description
            row[33] or '',  # data
            (row[34] or '')[:1000],  # planSQLText
            (row[35] or '')[:1000],  # sdbl
            row[36] if len(row) > 36 else '',  # mssql_error_code
        ])

    # Генерация файла
    timestamp_now = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"events_{session_id[:8]}_{timestamp_now}"

    if format.lower() == 'xlsx':
        return generate_xlsx(columns, rows, filename)
    else:
        return generate_csv(columns, rows, filename)


def generate_csv(columns: List[str], rows: List[List], filename: str):
    """Генерация CSV файла"""
    import csv
    import io

    output = io.StringIO()
    writer = csv.writer(output, delimiter=';', quoting=csv.QUOTE_ALL)
    writer.writerow(columns)
    writer.writerows(rows)

    response = Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={filename}.csv",
            "Content-Type": "text/csv; charset=utf-8-sig"  # BOM для Excel
        }
    )
    logger.info(f"✅ CSV сгенерирован: {len(rows)} записей")
    return response


def generate_xlsx(columns: List[str], rows: List[List], filename: str):
    """Генерация XLSX файла"""
    from openpyxl import Workbook
    import io

    wb = Workbook()
    ws = wb.active
    ws.title = "События"

    # Заголовки
    ws.append(columns)

    # Данные
    for row in rows:
        ws.append(row)

    # Авто-ширина колонок
    for column in ws.columns:
        max_length = 0
        column_letter = column[0].column_letter
        for cell in column:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = min(max_length + 2, 50)
        ws.column_dimensions[column_letter].width = adjusted_width

    # Сохранение в буфер
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)

    response = Response(
        content=output.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename={filename}.xlsx"
        }
    )
    logger.info(f"✅ XLSX сгенерирован: {len(rows)} записей")
    return response