# app/services/parsing_service.py
"""
Сервис парсинга технологических журналов 1С
Версия: 12.0 (ProcessPoolExecutor + улучшенное логирование + раннее завершение + БЕЗ фильтрации)
"""

# ============================================================================
# 🔧 КОНСТАНТЫ (ДОЛЖНЫ БЫТЬ ДО ВСЕХ ИМПОРТОВ!)
# ============================================================================

UTF8_BOM = b'\xef\xbb\xbf'
POSSIBLE_ENCODINGS = ['utf-8-sig', 'utf-8', 'cp1251', 'cp866']

# 🔧 УБРАНА ФИЛЬТРАЦИЯ - сохраняем ВСЕ события в интервале!

# Количество процессов для парсинга
NUM_PROCESSES = 4

# Интервалы логирования прогресса
PROGRESS_LOG_LINES = 100000  # Каждые 100K строк
PROGRESS_LOG_SECONDS = 30  # Каждые 30 секунд

# ============================================================================
# ИМПОРТЫ
# ============================================================================

import asyncio
import logging
import uuid
import re
import time
import chardet
import clickhouse_connect
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from concurrent.futures import ProcessPoolExecutor
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Text, BigInteger, ForeignKey, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

from app.database import ParsingSession, ParsingFile
from app.config import settings

logger = logging.getLogger(__name__)

# ============================================================================
# 🔍 ПАТТЕРН ДЛЯ ПАРСИНГА ЗАГОЛОВКОВ
# ============================================================================

EVENT_HEADER_PATTERN = re.compile(
    rb'^(\d{2}):(\d{2})\.(\d+)-(\d+),'
    rb'\s*([^,]+),'
    rb'\s*(\d+),'
    rb'\s*(.*)$'
)


# ============================================================================
# WORKER ФУНКЦИЯ ДЛЯ MULTIPROCESSING (С УЛУЧШЕННЫМ ЛОГИРОВАНИЕМ)
# ============================================================================

def _parse_file_worker(args: Tuple) -> Dict[str, Any]:
    """
    Worker функция для выполнения в отдельном процессе
    🔧 УЛУЧШЕННОЕ ЛОГИРОВАНИЕ для отладки зависаний
    Args:
        args: (file_path_str, worker_id, start_date, end_date, ch_config,
               logs_dir, batch_size, session_id, state_db_path)
    Returns:
        dict: {'file': str, 'events': int, 'status': str, 'error': Optional[str]}
    """
    (file_path_str, worker_id, start_date, end_date, ch_config,
     logs_dir, batch_size, session_id, state_db_path) = args

    # 🔧 Локальные импорты для изоляции процесса
    import logging
    import re
    import time
    import chardet
    import clickhouse_connect
    from datetime import datetime
    from pathlib import Path
    from typing import Dict, Any, Optional, List
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    # Настройка логирования с меткой воркера
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    worker_logger = logging.getLogger(f'worker-{worker_id}')

    # Константы (локальные для процесса)
    UTF8_BOM = b'\xef\xbb\xbf'
    POSSIBLE_ENCODINGS = ['utf-8-sig', 'utf-8', 'cp1251', 'cp866']

    EVENT_HEADER_PATTERN = re.compile(
        rb'^(\d{2}):(\d{2})\.(\d+)-(\d+),'
        rb'\s*([^,]+),'
        rb'\s*(\d+),'
        rb'\s*(.*)$'
    )

    # 🔧 Локальные вспомогательные функции
    def _detect_encoding_local(fp: Path) -> str:
        try:
            with open(fp, 'rb') as f:
                raw = f.read(10000)
                if raw.startswith(UTF8_BOM):
                    raw = raw[3:]
                result = chardet.detect(raw)
                if result and result['confidence'] > 0.7:
                    enc = result['encoding']
                    if enc in POSSIBLE_ENCODINGS:
                        return enc
                for enc in POSSIBLE_ENCODINGS:
                    try:
                        raw.decode(enc)
                        return enc
                    except:
                        continue
                return 'utf-8-sig'
        except Exception as e:
            worker_logger.warning(f"[{fp.name}] Ошибка определения кодировки: {e}")
            return 'utf-8-sig'

    def _parse_date_local(fn: str) -> datetime:
        basename = os.path.basename(fn)
        match = re.match(r'(\d{2})(\d{2})(\d{2})(\d{2})\.log', basename)
        if match:
            yy, mm, dd, hh = match.groups()
            year = 2000 + int(yy)
            return datetime(year, int(mm), int(dd), int(hh))
        return datetime.now()

    def _parse_props_local(prop_str: str) -> Dict[str, str]:
        props = {}
        if not prop_str.strip():
            return props
        key = None
        val = []
        in_q = False
        qc = None
        i = 0
        while i < len(prop_str):
            c = prop_str[i]
            if c in '"\'':
                if not in_q:
                    in_q = True
                    qc = c
                elif c == qc:
                    if i + 1 < len(prop_str) and prop_str[i + 1] == c:
                        val.append(c)
                        i += 1
                    else:
                        in_q = False
                        qc = None
                        val.append(c)
            elif c == ',' and not in_q:
                if key:
                    v = ''.join(val).strip()
                    if len(v) >= 2 and v[0] == v[-1] and v[0] in '"\'':
                        v = v[1:-1]
                    props[key] = v
                    key = None
                    val = []
            elif c == '=' and key is None:
                key = ''.join(val).strip()
                val = []
            else:
                val.append(c)
            i += 1
        if key and val:
            v = ''.join(val).strip()
            if len(v) >= 2 and v[0] == v[-1] and v[0] in '"\'':
                v = v[1:-1]
            props[key] = v
        return props


    def _build_event_local(ts, en, lvl, dur, props_str, mn, fp, enc, dn) -> Optional[Dict[str, Any]]:
        try:
            props = _parse_props_local(props_str)

            # 🔧 Извлекаем код ошибки MSSQL из exception/context
            sdbl_text = props.get('Sdbl', '')
            context_text = props.get('Context', '')
            sql_text = props.get('Sql', props.get('SQL', ''))

            # Ищем код ошибки во всех текстовых полях
            mssql_error_code = None
            for text_field in [sdbl_text, context_text, sql_text]:
                if text_field:
                    mssql_error_code = _extract_mssql_error_code(text_field)
                    if mssql_error_code:
                        break

            return {
                'timestamp': ts, 'directory_name': dn, 'source_file': fp.name,
                'line_number': mn, 'loaded_at': datetime.now(), 'event_name': en,
                'level': lvl, 'duration_ms': dur / 1000,
                'p_processName': props.get('p:processName', ''),
                't_computerName': props.get('t:computerName', ''),
                't_connectID': props.get('t:connectID', ''),
                'usr': props.get('Usr', ''), 'dbpid': props.get('dbpid', ''),
                'osThread': props.get('OSThread', ''),
                'sessionID': props.get('SessionID', ''),
                'trans': props.get('Trans', ''), 'func': props.get('Func', ''),
                'tableName': props.get('tableName', ''),
                'context': props.get('Context', '')[:100000],
                'sql': props.get('Sql', props.get('SQL', ''))[:100000],
                'sdbl': props.get('Sdbl', '')[:100000],
                'planSQLText': props.get('planSQLText', '')[:100000],
                'exception': props.get('Exception', '')[:100000],
                'locks': props.get('Locks', ''),
                'waitConnections': props.get('WaitConnections', ''),
                'deadlockConnectionIntersections': props.get('DeadlockConnectionIntersections', ''),
                'lkaid': props.get('lkaid', ''), 'lka': props.get('lka', ''),
                'lkp': props.get('lkp', ''), 'lkpid': props.get('lkpid', ''),
                'lksrc': props.get('lksrc', ''),
                'rows': props.get('Rows', ''), 'rowsAffected': props.get('RowsAffected', ''),
                'description': props.get('Description', ''), 'data': props.get('Data', ''),
                'severity': 'info', 'category': 'other',
                'mssql_error_code': mssql_error_code  # 🔧 НОВОЕ ПОЛЕ
            }
        except Exception as e:
            worker_logger.debug(f"Ошибка построения события: {e}")
            return None

    # Упрощённый ClickHouseLoader для воркера
    class WorkerCHLoader:
        def __init__(self, cfg, sess_id):
            self.config = cfg
            self.session_id = sess_id
            self.client = None
            safe_id = sess_id.replace('-', '_') if sess_id else ''
            self.table_name = f"tj_events_{safe_id}" if sess_id else "tj_events"
            self._connect()

        def _connect(self):
            worker_logger.info(f"🔌 Подключение к ClickHouse: {self.config.get('host')}:{self.config.get('port')}")
            self.client = clickhouse_connect.get_client(
                host=self.config.get('host', 'localhost'),
                port=int(self.config.get('port', 8123)),
                username=self.config.get('username', 'default'),
                password=self.config.get('password', ''),
                database=self.config.get('database', 'NetrebinAA'),
                compress=True,
                send_receive_timeout=300
            )
            # Создать таблицу
            quoted = f"`{self.table_name}`"
            db = self.config.get('database', 'NetrebinAA')
            worker_logger.info(f"📋 Проверка таблицы: {db}.{quoted}")
            self.client.command(f"""
                CREATE TABLE IF NOT EXISTS {db}.{quoted}
                (timestamp DateTime64(6, 'Europe/Moscow'), directory_name LowCardinality(String),
                 source_file String, line_number UInt64, loaded_at DateTime64(6, 'Europe/Moscow'),
                 event_name LowCardinality(String), level UInt8, duration_ms Float32,
                 p_processName LowCardinality(String), t_computerName String, t_connectID String,
                 usr LowCardinality(String), dbpid String, osThread String, sessionID String,
                 trans String, func String, tableName LowCardinality(String),
                 context String CODEC(ZSTD(3)), sql String CODEC(ZSTD(3)),
                 sdbl String CODEC(ZSTD(3)), planSQLText String CODEC(ZSTD(3)),
                 exception String CODEC(ZSTD(3)), locks String, waitConnections String,
                 deadlockConnectionIntersections String, lkaid String, lka String,
                 lkp String, lkpid String, lksrc String, rows String, rowsAffected String,
                 description String, data String CODEC(ZSTD(3)),
                 severity LowCardinality(String), category LowCardinality(String),
                 mssql_error_code Nullable(UInt32),  -- 🔧 НОВОЕ ПОЛЕ для кода ошибки MSSQL
                 INDEX idx_ts timestamp TYPE minmax GRANULARITY 8192,
                 INDEX idx_en event_name TYPE bloom_filter GRANULARITY 8192)
                ENGINE = MergeTree()
                ORDER BY (timestamp, event_name, directory_name)
            """)
            worker_logger.info(f"✅ Таблица готова: {self.table_name}")


        def insert_batch(self, batch):
            if not batch:
                return True
            try:
                rows = []
                for e in batch:
                    rows.append([
                        e.get('timestamp'), e.get('directory_name', 'root'), e.get('source_file', ''),
                        int(e.get('line_number', 0)), datetime.now(), e.get('event_name', ''),
                        int(e.get('level', 0)), float(e.get('duration_ms', 0)),
                        e.get('p_processName', ''), e.get('t_computerName', ''), e.get('t_connectID', ''),
                        e.get('usr', ''), e.get('dbpid', ''), e.get('osThread', ''),
                        e.get('sessionID', ''), e.get('trans', ''), e.get('func', ''),
                        e.get('tableName', ''), e.get('context', '')[:100000] if e.get('context') else '',
                        e.get('sql', '')[:100000] if e.get('sql') else '',
                        e.get('sdbl', '')[:100000] if e.get('sdbl') else '',
                        e.get('planSQLText', '')[:100000] if e.get('planSQLText') else '',
                        e.get('exception', '')[:100000] if e.get('exception') else '',
                        e.get('locks', ''), e.get('waitConnections', ''),
                        e.get('deadlockConnectionIntersections', ''),
                        e.get('lkaid', ''), e.get('lka', ''), e.get('lkp', ''),
                        e.get('lkpid', ''), e.get('lksrc', ''),
                        e.get('rows', ''), e.get('rowsAffected', ''),
                        e.get('description', ''), e.get('data', ''),
                        e.get('severity', 'info'), e.get('category', 'other'),
                        e.get('mssql_error_code')  # 🔧 НОВОЕ ПОЛЕ (последнее в списке)
                    ])
                worker_logger.debug(f"📤 Вставка {len(rows)} событий в {self.table_name}")
                self.client.insert(
                    table=f"`{self.table_name}`",
                    data=rows,
                    column_names=[
                        'timestamp', 'directory_name', 'source_file', 'line_number', 'loaded_at',
                        'event_name', 'level', 'duration_ms', 'p_processName', 't_computerName',
                        't_connectID', 'usr', 'dbpid', 'osThread', 'sessionID', 'trans', 'func',
                        'tableName', 'context', 'sql', 'sdbl', 'planSQLText', 'exception',
                        'locks', 'waitConnections', 'deadlockConnectionIntersections',
                        'lkaid', 'lka', 'lkp', 'lkpid', 'lksrc', 'rows', 'rowsAffected',
                        'description', 'data', 'severity', 'category', 'mssql_error_code'  # 🔧 НОВОЕ
                    ]
                )
                worker_logger.debug(f"✅ Вставка завершена")
                return True
            except Exception as e:
                worker_logger.error(f"❌ Ошибка вставки в ClickHouse: {e}")
                return False

        def close(self):
            if self.client:
                self.client.close()
                worker_logger.info(f"🔌 ClickHouse отключён")

    # 🔧 Вспомогательные функции для обновления SQLite из воркера
    def _update_session_current_file(sess_id: str, file_name: str, file_path: str, file_size: int, db_path: str):
        """Обновить информацию о текущем файле в сессии"""
        try:
            engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
            SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            db = SessionLocal()

            session = db.query(ParsingSession).filter(ParsingSession.session_id == sess_id).first()
            if session:
                session.current_file_name = file_name
                session.current_file_path = file_path
                session.current_file_size = file_size
                session.current_file_events = 0
                db.commit()
            db.close()
        except Exception as e:
            worker_logger.debug(f"Не удалось обновить текущий файл: {e}")

    def _update_session_file_events(sess_id: str, events_count: int, db_path: str):
        """Обновить счётчик событий текущего файла"""
        try:
            engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
            SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            db = SessionLocal()

            session = db.query(ParsingSession).filter(ParsingSession.session_id == sess_id).first()
            if session:
                session.current_file_events = events_count
                db.commit()
            db.close()
        except Exception as e:
            worker_logger.debug(f"Не удалось обновить счётчик событий: {e}")

    def _clear_session_current_file(sess_id: str, db_path: str):
        """Очистить информацию о текущем файле после завершения"""
        try:
            engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
            SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            db = SessionLocal()

            session = db.query(ParsingSession).filter(ParsingSession.session_id == sess_id).first()
            if session:
                session.current_file_name = None
                session.current_file_path = None
                session.current_file_size = None
                session.current_file_events = 0
                db.commit()
            db.close()
        except Exception as e:
            worker_logger.debug(f"Не удалось очистить текущий файл: {e}")

    # Основная логика
    file_path = Path(file_path_str)
    log_dir = Path(logs_dir)

    # 🔧 Логирование начала обработки
    worker_logger.info(f"🔧 [Worker-{worker_id}] 🚀 НАЧАЛО: {file_path.name}")
    start_time = time.time()

    try:
        # 🔧 Проверка существования и размера файла
        if not file_path.exists():
            worker_logger.error(f"❌ [Worker-{worker_id}] Файл не найден: {file_path}")
            return {'file': str(file_path), 'events': 0, 'status': 'deleted', 'error': 'File not found'}

        file_size = file_path.stat().st_size
        worker_logger.info(f"📊 [Worker-{worker_id}] Размер файла: {file_size / 1024 / 1024:.2f} МБ")

        # 🔧 Проверка прав доступа
        if not os.access(file_path, os.R_OK):
            worker_logger.error(f"❌ [Worker-{worker_id}] Нет прав на чтение: {file_path}")
            return {'file': str(file_path), 'events': 0, 'status': 'permission_error', 'error': 'Permission denied'}

        # 🔧 Получение directory_name
        try:
            rel_path = file_path.relative_to(log_dir)
            file_display = str(rel_path)
            directory_name = rel_path.parts[0] if len(rel_path.parts) > 1 else 'root'
        except ValueError:
            file_display = str(file_path)
            directory_name = file_path.parent.name if file_path.parent != file_path.parent.parent else 'root'

        # 🔧 Обновление информации о текущем файле в сессии
        _update_session_current_file(session_id, file_path.name, file_display, file_size, state_db_path)

        # 🔧 Определение кодировки
        file_encoding = _detect_encoding_local(file_path)
        worker_logger.info(f"🔤 [Worker-{worker_id}] Кодировка: {file_encoding}")

        # 🔧 Базовая дата из имени файла
        base_date = _parse_date_local(str(file_path))
        worker_logger.info(f"📅 [Worker-{worker_id}] Базовая дата: {base_date}")

        # 🔧 Подключение к ClickHouse
        ch_loader = None
        try:
            ch_loader = WorkerCHLoader(ch_config, session_id)
            worker_logger.info(f"✅ [Worker-{worker_id}] ClickHouse подключён")
        except Exception as e:
            worker_logger.error(f"❌ [Worker-{worker_id}] Ошибка подключения к ClickHouse: {e}")
            return {'file': file_display, 'directory': directory_name, 'events': 0, 'status': 'clickhouse_error',
                    'error': str(e)}

        # 🔧 Статистика
        local_stats = {'events': 0, 'lines': 0, 'errors': 0}
        local_batch = []
        current_header = None
        current_lines = []
        match_num = 0
        reached_end = False

        # 🔧 Тайминги для логирования
        last_log_time = time.time()
        last_progress_lines = 0

        worker_logger.info(f"📖 [Worker-{worker_id}] Открываю файл для чтения...")

        # 🔧 Чтение файла с логированием прогресса
        with open(file_path, 'rb', buffering=65536) as f:
            for line_number, line_bytes in enumerate(f, 1):
                local_stats['lines'] += 1

                # 🔧 Прогресс-логирование каждые N строк или секунд
                current_time = time.time()
                if (line_number - last_progress_lines >= PROGRESS_LOG_LINES) or (
                        current_time - last_log_time > PROGRESS_LOG_SECONDS):
                    elapsed = current_time - start_time
                    lines_per_sec = line_number / elapsed if elapsed > 0 else 0
                    worker_logger.info(
                        f"📊 [Worker-{worker_id}] Прогресс: "
                        f"{line_number:,} строк, {local_stats['events']:,} событий, "
                        f"{elapsed:.1f} сек, {lines_per_sec:,.0f} строк/сек"
                    )
                    last_log_time = current_time
                    last_progress_lines = line_number

                header_match = EVENT_HEADER_PATTERN.match(line_bytes)

                if header_match:
                    if current_header and not reached_end:
                        match_num += 1
                        try:
                            minute = int(current_header.group(1))
                            second = int(current_header.group(2))
                            microseconds = current_header.group(3).decode(file_encoding, errors='replace')
                            duration = int(current_header.group(4).decode(file_encoding, errors='replace'))
                            event_name = current_header.group(5).decode(file_encoding, errors='replace')
                            level = int(current_header.group(6).decode(file_encoding, errors='replace'))
                            properties_start = current_header.group(7).decode(file_encoding, errors='replace')

                            microsec = microseconds.ljust(6, '0')[:6]
                            event_timestamp = base_date.replace(minute=minute, second=second, microsecond=int(microsec))

                            # 🔍 🔧 РАННЕЕ ЗАВЕРШЕНИЕ: если вышли за end_date — прекращаем файл
                            if event_timestamp > end_date:
                                worker_logger.info(
                                    f"⏹ [Worker-{worker_id}] Раннее завершение: {event_timestamp} > {end_date}")
                                reached_end = True
                                break  # ← 🔧 КРИТИЧНО: выход из цикла чтения файла

                            # 🔍 ФИЛЬТРАЦИЯ ПО ИНТЕРВАЛУ: пропускаем события до start_date
                            if event_timestamp < start_date:
                                current_header = header_match
                                current_lines = []
                                continue

                            # 🔧 СОХРАНЯЕМ ВСЕ СОБЫТИЯ В ИНТЕРВАЛЕ (без фильтрации по типу!)
                            full_props = properties_start
                            if current_lines:
                                content_decoded = '\n'.join(
                                    line.decode(file_encoding, errors='replace') for line in current_lines)
                                full_props = properties_start + '\n' + content_decoded

                            event = _build_event_local(event_timestamp, event_name, level, duration,
                                                       full_props, match_num, file_path, file_encoding, directory_name)
                            if event:
                                local_batch.append(event)
                                local_stats['events'] += 1

                                if len(local_batch) >= batch_size:
                                    insert_start = time.time()
                                    if ch_loader.insert_batch(local_batch):
                                        insert_time = time.time() - insert_start
                                        worker_logger.debug(
                                            f"✅ [Worker-{worker_id}] Батч {len(local_batch)} вставлен за {insert_time:.2f} сек")
                                    else:
                                        worker_logger.error(f"❌ [Worker-{worker_id}] Ошибка вставки батча")
                                    local_batch = []

                                # 🔧 Обновлять счётчик событий текущего файла каждые 1000 событий
                                if local_stats['events'] % 1000 == 0:
                                    _update_session_file_events(session_id, local_stats['events'], state_db_path)
                        except Exception as e:
                            worker_logger.error(f"❌ [Worker-{worker_id}] Ошибка парсинга события #{match_num}: {e}")
                            local_stats['errors'] += 1

                    if reached_end:
                        continue

                    current_header = header_match
                    current_lines = []
                elif current_header and not reached_end:
                    current_lines.append(line_bytes.rstrip(b'\n\r'))

        # 🔧 Обработка последнего события
        if current_header and not reached_end:
            match_num += 1
            try:
                minute = int(current_header.group(1))
                second = int(current_header.group(2))
                microseconds = current_header.group(3).decode(file_encoding, errors='replace')
                duration = int(current_header.group(4).decode(file_encoding, errors='replace'))
                event_name = current_header.group(5).decode(file_encoding, errors='replace')
                level = int(current_header.group(6).decode(file_encoding, errors='replace'))
                properties_start = current_header.group(7).decode(file_encoding, errors='replace')

                microsec = microseconds.ljust(6, '0')[:6]
                event_timestamp = base_date.replace(minute=minute, second=second, microsecond=int(microsec))

                # 🔍 Проверка интервала для последнего события
                if start_date <= event_timestamp <= end_date:
                    full_props = properties_start
                    if current_lines:
                        content_decoded = '\n'.join(
                            line.decode(file_encoding, errors='replace') for line in current_lines)
                        full_props = properties_start + '\n' + content_decoded

                    event = _build_event_local(event_timestamp, event_name, level, duration,
                                               full_props, match_num, file_path, file_encoding, directory_name)
                    if event:
                        local_batch.append(event)
                        local_stats['events'] += 1
            except Exception as e:
                worker_logger.error(f"❌ [Worker-{worker_id}] Ошибка последнего события: {e}")
                local_stats['errors'] += 1

        # 🔧 Вставка последнего батча
        if local_batch:
            worker_logger.info(f"📦 [Worker-{worker_id}] Вставка финального батча: {len(local_batch)} событий")
            insert_start = time.time()
            if ch_loader.insert_batch(local_batch):
                insert_time = time.time() - insert_start
                worker_logger.info(f"✅ [Worker-{worker_id}] Финальный батч вставлен за {insert_time:.2f} сек")
            else:
                worker_logger.error(f"❌ [Worker-{worker_id}] Ошибка вставки финального батча")

        # 🔧 Закрытие соединения
        if ch_loader:
            ch_loader.close()

        # 🔧 Очистка информации о текущем файле
        _clear_session_current_file(session_id, state_db_path)

        # 🔧 Итоговое логирование
        elapsed = time.time() - start_time
        rate = local_stats['events'] / elapsed if elapsed > 0 else 0
        status_msg = "раннее завершение" if reached_end else "завершён"

        worker_logger.info(
            f"✅ [Worker-{worker_id}] 🏁 ЗАВЕРШЁН: {file_path.name} - "
            f"{local_stats['events']:,} событий, {local_stats['lines']:,} строк, "
            f"{local_stats['errors']} ошибок, {elapsed:.1f} сек, {rate:,.0f} событий/сек"
        )

        return {
            'file': file_display,
            'directory': directory_name,
            'events': local_stats['events'],
            'lines': local_stats['lines'],
            'errors': local_stats['errors'],
            'status': 'completed' if local_stats['events'] > 0 else 'empty',
            'elapsed': elapsed,
            'rate': rate,
            'early_terminate': reached_end
        }

    except PermissionError as e:
        worker_logger.error(f"❌ [Worker-{worker_id}] Ошибка прав доступа: {e}")
        _clear_session_current_file(session_id, state_db_path)
        return {'file': str(file_path), 'events': 0, 'status': 'permission_error', 'error': str(e)}
    except FileNotFoundError as e:
        worker_logger.error(f"❌ [Worker-{worker_id}] Файл не найден во время чтения: {e}")
        _clear_session_current_file(session_id, state_db_path)
        return {'file': str(file_path), 'events': 0, 'status': 'deleted', 'error': str(e)}
    except Exception as e:
        worker_logger.exception(f"❌ [Worker-{worker_id}] КРИТИЧЕСКАЯ ОШИБКА: {e}")
        if ch_loader:
            ch_loader.close()
        _clear_session_current_file(session_id, state_db_path)
        return {'file': str(file_path), 'events': 0, 'status': 'error', 'error': str(e)}


# ============================================================================
# КЛАСС ПАРСЕРА
# ============================================================================

class ParsingService:
    """Сервис для парсинга технологических журналов 1С"""

    def __init__(self):
        self.log_directory = Path(settings.parser.log_directory)
        self.batch_size = settings.parser.batch_size
        self._active_executors: Dict[str, ProcessPoolExecutor] = {}
        logger.info(f"🔧 ParsingService инициализирован: log_directory={self.log_directory}")

    async def start_parsing(
            self,
            db: Session,
            start_date: datetime,
            end_date: datetime
    ) -> str:
        """Создать сессию парсинга с валидацией дат"""

        today = datetime.now().date()
        if start_date.date() != today or end_date.date() != today:
            raise ValueError(
                f"Парсинг разрешён только для текущей даты ({today}). "
                f"Получено: {start_date.date()} - {end_date.date()}"
            )

        if start_date.hour != end_date.hour or start_date.date() != end_date.date():
            raise ValueError(
                "Интервал должен укладываться в пределах одного часа текущего дня. "
                f"Получено: {start_date} - {end_date}"
            )

        target_date_str = start_date.strftime("%y%m%d")
        target_hour = start_date.hour

        session_id = str(uuid.uuid4())

        parsing_session = ParsingSession(
            session_id=session_id,
            start_date=start_date,
            end_date=end_date,
            status="pending",
            total_files=0,
            processed_files=0,
            total_events=0,
            progress_percent=0.0,
            error_message=None
        )
        db.add(parsing_session)
        db.commit()

        logger.info(f"Session {session_id}: parsing {target_date_str} hour {target_hour:02d}")
        return session_id

    async def run_parsing_task(
            self,
            db: Session,
            session_id: str,
            start_date: datetime,
            end_date: datetime
    ):
        """Фоновая задача парсинга — НЕБЛОКИРУЮЩАЯ версия с ProcessPoolExecutor"""
        logger.info(f"Starting parsing task: {session_id}")

        loop = asyncio.get_event_loop()

        try:
            session = db.query(ParsingSession).filter(
                ParsingSession.session_id == session_id
            ).first()
            if not session:
                logger.error(f"Session {session_id} not found")
                return

            session.status = "running"
            session.started_at = datetime.utcnow()
            db.commit()

            target_date_str = start_date.strftime("%y%m%d")
            target_hour = start_date.hour
            log_files = self._find_log_files_for_hour(target_date_str, target_hour)

            session.total_files = len(log_files)
            db.commit()

            logger.info(f"Found {len(log_files)} files for {target_date_str}{target_hour:02d}")

            tasks = []
            ch_config = {
                'host': settings.database.clickhouse.host,
                'port': settings.database.clickhouse.port,
                'username': settings.database.clickhouse.user,
                'password': settings.database.clickhouse.password,
                'database': settings.database.clickhouse.database
            }

            for idx, log_file in enumerate(log_files, 1):
                tasks.append((
                    str(log_file),
                    idx,
                    start_date,
                    end_date,
                    ch_config,
                    str(self.log_directory),
                    self.batch_size,
                    session_id,
                    settings.database.sqlite_path  # 🔧 Передаём путь к БД для обновления из воркера
                ))

            logger.info(f"🔧 Запуск пула из {NUM_PROCESSES} процессов")

            total_events = 0
            processed_files = 0
            early_terminated_files = 0

            with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
                futures = [loop.run_in_executor(executor, _parse_file_worker, task) for task in tasks]

                for future in asyncio.as_completed(futures):
                    result = await future

                    processed_files += 1
                    events_in_file = result.get('events', 0)
                    total_events += events_in_file

                    if result.get('early_terminate'):
                        early_terminated_files += 1

                    if processed_files % 10 == 0 or processed_files == len(log_files):
                        session.processed_files = processed_files
                        session.total_events = total_events
                        session.progress_percent = round((processed_files / max(len(log_files), 1)) * 100, 2)
                        db.commit()

                    status = result.get('status', 'unknown')
                    early_msg = " (раннее завершение)" if result.get('early_terminate') else ""
                    elapsed = result.get('elapsed', 0)
                    rate = result.get('rate', 0)
                    logger.info(
                        f"Processed {processed_files}/{len(log_files)}: {result.get('file')} "
                        f"({events_in_file} events{early_msg}, {elapsed:.1f} сек, {rate:,.0f} событий/сек)"
                    )

                    await asyncio.sleep(0)

            session.status = "completed"
            session.completed_at = datetime.utcnow()
            db.commit()

            logger.info(
                f"Session {session_id} completed: {total_events:,} events, {early_terminated_files} files early terminated"
            )

        except Exception as e:
            logger.exception(f"Fatal error in {session_id}: {e}")
            session = db.query(ParsingSession).filter(
                ParsingSession.session_id == session_id
            ).first()
            if session:
                session.status = "failed"
                session.error_message = str(e)
                db.commit()

    def _find_log_files_for_hour(self, date_str: str, hour: int) -> List[Path]:
        """Найти файлы логов для конкретной даты и часа"""
        log_files = []

        if not self.log_directory.exists():
            logger.error(f"❌ Директория не найдена: {self.log_directory}")
            return []

        logger.info(f"🔍 Поиск в: {self.log_directory}")
        logger.info(f"🔍 Ожидаемый префикс: {date_str}{hour:02d}")

        expected_prefix = f"{date_str}{hour:02d}"

        for log_file in self.log_directory.rglob(f"{expected_prefix}*.log"):
            logger.info(f"📄 Найдено: {log_file}")
            log_files.append(log_file)

        logger.info(f"📊 Всего найдено файлов: {len(log_files)}")
        return sorted(log_files)

    async def cancel_parsing(self, session_id: str):
        """Отменить парсинг"""
        logger.info(f"Cancelling session: {session_id}")
        pass


# ============================================================================
# 🔧 ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ ANALYSIS ENDPOINTS
# ============================================================================

def get_clickhouse_client():
    """
    Получить клиент ClickHouse для запросов аналитики

    Returns:
        clickhouse_connect.driver.client.Client: подключённый клиент
    """
    return clickhouse_connect.get_client(
        host=settings.database.clickhouse.host,
        port=int(settings.database.clickhouse.port),
        username=settings.database.clickhouse.user,
        password=settings.database.clickhouse.password,
        database=settings.database.clickhouse.database,
        compress=True,
        send_receive_timeout=300
    )


# app/services/parsing_service.py — добавьте новую функцию

def _extract_mssql_error_code(text: str) -> Optional[int]:
    """
    Извлечь код ошибки MSSQL из текста исключения

    Примеры:
    - "native=1205" → 1205
    - "Error: 2627" → 2627
    - "SQLState=40001" → 40001

    Returns:
        int или None если код не найден
    """
    if not text:
        return None

    # 🔍 Паттерн 1: native=XXXX (наиболее частый для 1С)
    import re
    native_match = re.search(r'native=(\d+)', text, re.IGNORECASE)
    if native_match:
        return int(native_match.group(1))

    # 🔍 Паттерн 2: Error: XXXX или Error XXXX
    error_match = re.search(r'Error[:\s]+(\d+)', text, re.IGNORECASE)
    if error_match:
        return int(error_match.group(1))

    # 🔍 Паттерн 3: SQLState=XXXXX
    state_match = re.search(r'SQLState[=:]?(\d{5})', text, re.IGNORECASE)
    if state_match:
        return int(state_match.group(1))

    return None