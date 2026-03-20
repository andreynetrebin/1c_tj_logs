# app/schemas.py
"""
Pydantic схемы для API парсера технологических журналов 1С
"""

from pydantic import BaseModel, Field, field_validator, ConfigDict
from datetime import datetime
from typing import Optional, List, Dict, Any


# ============================================================================
# ЗАПРОСЫ (Requests)
# ============================================================================

class ParsingRequest(BaseModel):
    """Запрос на запуск парсинга"""
    start_date: datetime = Field(..., description="Начальная дата диапазона")
    end_date: datetime = Field(..., description="Конечная дата диапазона")
    directory_name: Optional[str] = Field(None, description="Фильтр по каталогу (инфобазе)")
    
    @field_validator('end_date')
    @classmethod
    def end_must_be_after_start(cls, v, info):
        start = info.data.get('start_date')
        if start and v <= start:
            raise ValueError('end_date должен быть больше start_date')
        return v


class EventFilter(BaseModel):
    """Фильтры для поиска событий"""
    session_id: str
    event_name: Optional[str] = None
    level_min: Optional[int] = None
    level_max: Optional[int] = None
    duration_min: Optional[float] = None
    duration_max: Optional[float] = None
    category: Optional[str] = None
    severity: Optional[str] = None
    search_text: Optional[str] = None
    page: int = Field(1, ge=1)
    page_size: int = Field(50, ge=1, le=1000)


# ============================================================================
# ОТВЕТЫ (Responses)
# ============================================================================

# app/schemas.py

class ParsingResponse(BaseModel):
    """Ответ после запуска парсинга"""
    session_id: str = Field(..., description="UUID сессии парсинга")
    status: str = Field(..., description="started, pending, failed, already_running, already_completed")
    message: str = Field(..., description="Сообщение для пользователя")
    redirect_url: Optional[str] = Field(None, description="URL для редиректа при перекрытии интервала")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "session_id": "550e8400-e29b-41d4-a716-446655440000",
                "status": "started",
                "message": "Парсинг запущен"
            }
        }
    )


class ParsingProgress(BaseModel):
    """Прогресс выполнения парсинга"""
    session_id: str
    status: str  # pending, running, completed, failed
    progress_percent: float = Field(ge=0, le=100)
    total_files: int = 0
    processed_files: int = 0
    total_events: int = 0
    error_message: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    elapsed_seconds: Optional[float] = None
    eta_seconds: Optional[float] = None

    # 🔧 НОВОЕ: Информация о текущем файле
    current_file: Optional[Dict[str, Any]] = Field(default=None,
                                                   description="Информация о текущем обрабатываемом файле")

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "session_id": "550e8400-e29b-41d4-a716-446655440000",
                "status": "running",
                "progress_percent": 45.5,
                "total_files": 270,
                "processed_files": 120,
                "total_events": 12345,
                "current_file": {
                    "name": "26031005.log",
                    "path": "rphost_3472/26031005.log",
                    "size_bytes": 125430000,
                    "size_mb": 119.6,
                    "events_count": 2345,
                    "started_at": "2026-03-10T09:02:00"
                }
            }
        }
    )


class ParsingSessionInfo(BaseModel):
    """Информация о сессии для списка"""
    session_id: str
    status: str
    start_date: datetime
    end_date: datetime
    directory_name: Optional[str] = None
    progress_percent: float = Field(ge=0, le=100, default=0.0)
    total_events: int = Field(default=0)
    total_files: int = Field(default=0)
    processed_files: int = Field(default=0)
    created_at: datetime
    completed_at: Optional[datetime] = None
    elapsed_seconds: Optional[float] = None  # 🔧 Для отображения времени выполнения
    error_message: Optional[str] = None  # 🔧 Для отображения ошибок

    model_config = ConfigDict(
        from_attributes=True,  # 🔧 Важно для SQLAlchemy моделей
        json_schema_extra={
            "example": {
                "session_id": "550e8400-e29b-41d4-a716-446655440000",
                "status": "running",
                "start_date": "2026-03-06T15:00:00",
                "end_date": "2026-03-06T15:04:59",
                "directory_name": "1C_Culprits",
                "progress_percent": 45.5,
                "total_events": 12345,
                "total_files": 270,
                "processed_files": 120,
                "created_at": "2026-03-06T15:00:01",
                "elapsed_seconds": 180.5
            }
        }
    )
# ============================================================================
# АНАЛИЗ (Analysis)
# ============================================================================

class AnalysisResult(BaseModel):
    """Результаты анализа проблем за период"""
    session_id: str
    period_start: datetime
    period_end: datetime
    duration_minutes: float
    
    # Общая статистика
    total_events: int
    total_duration_ms: float
    average_duration_ms: float
    max_duration_ms: float
    
    # События по уровням
    events_by_level: Dict[str, int] = Field(default_factory=dict)
    
    # События по категориям
    events_by_category: Dict[str, int] = Field(default_factory=dict)
    
    # Топ медленных запросов
    top_slow_queries: List[Dict[str, Any]] = Field(default_factory=list)
    
    # Топ ошибок
    top_errors: List[Dict[str, Any]] = Field(default_factory=list)
    
    # Проблемные таблицы
    problem_tables: List[Dict[str, Any]] = Field(default_factory=list)
    
    # Проблемные пользователи
    problem_users: List[Dict[str, Any]] = Field(default_factory=list)
    
    # Временная шкала
    timeline: List[Dict[str, Any]] = Field(default_factory=list)
    
    # Рекомендации
    recommendations: List[str] = Field(default_factory=list)
    
    class Config:
        json_schema_extra = {
            "example": {
                "session_id": "550e8400-e29b-41d4-a716-446655440000",
                "total_events": 15000,
                "average_duration_ms": 45.5,
                "top_errors": [
                    {
                        "error_type": "Lock Timeout",
                        "count": 25,
                        "sample": "Превышено время ожидания запроса на блокировку"
                    }
                ],
                "recommendations": [
                    "Обнаружено 25 таймаутов блокировок. Проверьте индексы.",
                    "Средняя длительность запросов 45мс - в норме."
                ]
            }
        }


# ============================================================================
# СОБЫТИЯ (Events)
# ============================================================================

class EventDetail(BaseModel):
    """Детали события"""
    id: int
    session_id: str
    timestamp: datetime
    directory_name: str
    source_file: str
    event_name: str
    level: int
    duration_ms: float
    process_name: Optional[str] = None
    user_name: Optional[str] = None
    table_name: Optional[str] = None
    context: Optional[str] = None
    sql_query: Optional[str] = None
    sdbl: Optional[str] = None
    exception: Optional[str] = None
    severity: str
    category: str


class EventListResponse(BaseModel):
    """Список событий с пагинацией"""
    events: List[EventDetail]
    total: int
    page: int
    page_size: int
    total_pages: int


# ============================================================================
# ОШИБКИ (Errors)
# ============================================================================

class ErrorResponse(BaseModel):
    """Стандартный ответ об ошибке"""
    detail: str
    status_code: int = 500
    
    class Config:
        json_schema_extra = {
            "example": {
                "detail": "Внутренняя ошибка сервера",
                "status_code": 500
            }
        }


# app/schemas.py — ДОБАВИТЬ В КОНЕЦ ФАЙЛА

# ============================================================================
# СХЕМЫ ДЛЯ АНАЛИЗА
# ============================================================================

class AnalysisSummary(BaseModel):
    """Сводная статистика по сессии"""
    session_id: str
    total_events: int
    total_filtered: int
    filter_percent: float
    time_range: Dict[str, str]  # {"start": "...", "end": "..."}
    directories: List[str]  # Список инфобаз
    event_types: Dict[str, int]  # event_name -> count
    severity_distribution: Dict[str, int]  # severity -> count
    top_errors: List[Dict[str, Any]]  # Топ ошибок
    top_slow: List[Dict[str, Any]]  # Топ медленных операций
    timeline: List[Dict[str, Any]]  # Временная шкала (агрегация по минутам)


class AnalysisFilter(BaseModel):
    """Фильтры для аналитики"""
    directory_name: Optional[str] = None
    event_name: Optional[str] = None
    severity: Optional[str] = None
    min_duration_ms: Optional[float] = None
    max_duration_ms: Optional[float] = None
    search_text: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None


class EventTimelinePoint(BaseModel):
    """Точка на временной шкале"""
    timestamp: str
    count: int
    avg_duration_ms: float


class TopEvent(BaseModel):
    """Топ событие (ошибка или медленная операция)"""
    timestamp: str
    event_name: str
    duration_ms: float
    directory_name: str
    source_file: str
    usr: Optional[str]
    context: Optional[str]
    sql: Optional[str]
    exception: Optional[str]
    severity: str


class AnalysisResponse(BaseModel):
    """Ответ с аналитикой"""
    summary: AnalysisSummary
    filters_applied: Optional[AnalysisFilter] = None