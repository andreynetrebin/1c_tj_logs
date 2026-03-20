import os
import re
from datetime import datetime
from pathlib import Path
from typing import Generator, Dict, Any, Optional
import json


class TJLogParser:
    """Парсер технологических журналов 1С"""

    def __init__(self, log_directory: str):
        self.log_directory = Path(log_directory)
        self.timestamp_pattern = re.compile(
            r'^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+)'
        )

    def find_log_files(self, start_date: datetime, end_date: datetime) -> list:
        """Найти все лог-файлы в заданном диапазоне"""
        log_files = []

        for root, dirs, files in os.walk(self.log_directory):
            for file in files:
                if file.endswith('.log'):
                    file_path = Path(root) / file
                    file_date = self._extract_date_from_filename(file)

                    if file_date and start_date.date() <= file_date <= end_date.date():
                        log_files.append(file_path)

        return sorted(log_files)

    def _extract_date_from_filename(self, filename: str) -> Optional[datetime]:
        """Извлечь дату из имени файла (формат: YYMMDDHH.log)"""
        try:
            match = re.search(r'(\d{6})\d{2}\.log$', filename)
            if match:
                date_str = match.group(1)
                return datetime.strptime(date_str, '%y%m%d')
        except:
            pass
        return None

    def parse_file(self, file_path: Path, start_date: datetime, end_date: datetime) -> Generator[
        Dict[str, Any], None, None]:
        """Парсинг одного файла с генератором"""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                current_event = {}

                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    # Проверка на новую запись (начинается с timestamp)
                    timestamp_match = self.timestamp_pattern.match(line)
                    if timestamp_match:
                        # Если есть предыдущее событие, возвращаем его
                        if current_event:
                            event = self._parse_event(current_event)
                            if event and self._is_in_range(event, start_date, end_date):
                                yield event

                        # Начинаем новое событие
                        current_event = {'timestamp_str': timestamp_match.group(1)}
                        line = line[timestamp_match.end():].strip()

                    # Парсинг полей события
                    self._parse_line(line, current_event)

                # Последнее событие
                if current_event:
                    event = self._parse_event(current_event)
                    if event and self._is_in_range(event, start_date, end_date):
                        yield event

        except Exception as e:
            print(f"Error parsing {file_path}: {e}")

    def _parse_line(self, line: str, event: Dict[str, Any]):
        """Парсинг одной строки лога"""
        # Формат: "field";"value" или field;value
        parts = line.split(';', 1)
        if len(parts) == 2:
            field = parts[0].strip().strip('"')
            value = parts[1].strip()

            # Убираем кавычки
            if value.startswith('"') and value.endswith('"'):
                value = value[1:-1]

            event[field] = value

    def _parse_event(self, raw_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Преобразование сырых данных в структурированное событие"""
        try:
            timestamp_str = raw_event.get('timestamp_str', '')
            if not timestamp_str:
                return None

            # Парсинг timestamp
            timestamp = datetime.strptime(timestamp_str.split('.')[0], '%Y-%m-%d %H:%M:%S')

            return {
                'timestamp': timestamp,
                'directory_name': raw_event.get('directory_name', ''),
                'source_file': raw_event.get('source_file', ''),
                'line_number': int(raw_event.get('line_number', 0) or 0),
                'event_name': raw_event.get('event_name', ''),
                'level': int(raw_event.get('level', 0) or 0),
                'duration_ms': float(raw_event.get('duration_ms', 0) or 0),
                'process_name': raw_event.get('p_processName', ''),
                'computer_name': raw_event.get('t_computerName', ''),
                'connect_id': raw_event.get('t_connectID', ''),
                'user_name': raw_event.get('usr', ''),
                'db_pid': raw_event.get('dbpid', ''),
                'os_thread': raw_event.get('osThread', ''),
                'session_id_1c': raw_event.get('sessionID', ''),
                'transaction': raw_event.get('trans', ''),
                'function': raw_event.get('func', ''),
                'table_name': raw_event.get('tableName', ''),
                'context': raw_event.get('context', ''),
                'sql_query': raw_event.get('sql', ''),
                'sdbl': raw_event.get('sdbl', ''),
                'plan_sql_text': raw_event.get('planSQLText', ''),
                'exception': raw_event.get('exception', ''),
                'sql_error_code': raw_event.get('sql_error_code', ''),
                'locks': raw_event.get('locks', ''),
                'wait_connections': raw_event.get('waitConnections', ''),
                'deadlock_intersections': raw_event.get('deadlockConnectionIntersections', ''),
                'rows': int(raw_event.get('rows', 0) or 0),
                'rows_affected': int(raw_event.get('rowsAffected', 0) or 0),
                'description': raw_event.get('description', ''),
                'data': raw_event.get('data', '')
            }
        except Exception as e:
            return None

    def _is_in_range(self, event: Dict[str, Any], start_date: datetime, end_date: datetime) -> bool:
        """Проверка попадания события в диапазон"""
        timestamp = event['timestamp']
        return start_date <= timestamp <= end_date

    def categorize_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Категоризация события"""
        event_name = event.get('event_name', '').upper()
        level = event.get('level', 0)
        duration = event.get('duration_ms', 0)
        has_exception = bool(event.get('exception', ''))
        has_sql_error = bool(event.get('sql_error_code', ''))
        has_locks = bool(event.get('locks', ''))

        # Определение категории
        if 'DBMSSQL' in event_name or 'SDBL' in event_name:
            category = 'database'
        elif 'LOCK' in event_name or has_locks:
            category = 'lock'
        elif 'ERROR' in event_name or has_exception or has_sql_error:
            category = 'error'
        elif duration > 1000:  # > 1 секунда
            category = 'performance'
        else:
            category = 'info'

        # Определение severity
        if has_exception or has_sql_error or level >= 4:
            severity = 'critical'
        elif duration > 5000 or level >= 3:  # > 5 секунд
            severity = 'high'
        elif duration > 1000 or level >= 2:  # > 1 секунда
            severity = 'medium'
        else:
            severity = 'low'

        event['category'] = category
        event['severity'] = severity

        return event