# app/models.py
"""
SQLAlchemy модели для хранения МЕТАДАННЫХ парсинга в SQLite
ВАЖНО: События ТЖ хранятся в ClickHouse (таблица tj_events),
       SQLite используется ТОЛЬКО для метаданных сессий и файлов.
"""

from sqlalchemy import (
    Column, Integer, String, DateTime, Float, Text,
    ForeignKey, func, BigInteger
)
from sqlalchemy.orm import relationship, declarative_base
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
from typing import Optional, List, Dict, Any
import uuid

Base = declarative_base()


# ============================================================================
# SQLITE МОДЕЛИ (только метаданные)
# ============================================================================

class ParsingSession(Base):
    """Сессия парсинга — метаданные"""
    __tablename__ = "parsing_sessions"

    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(String(36), unique=True, index=True)

    # Параметры запроса
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    directory_name = Column(String(255), nullable=True)

    # Статус выполнения
    status = Column(String(20), default="pending")
    progress_percent = Column(Float, default=0.0)
    error_message = Column(Text, nullable=True)

    # Статистика
    total_files = Column(Integer, default=0)
    processed_files = Column(Integer, default=0)
    total_events = Column(Integer, default=0)

    # 🔧 НОВОЕ: Информация о текущем файле
    current_file_name = Column(String(255), nullable=True)
    current_file_path = Column(String(500), nullable=True)
    current_file_size = Column(BigInteger, nullable=True)  # Размер в байтах
    current_file_events = Column(Integer, default=0)  # Событий в текущем файле

    # Временные метки
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Связи
    files = relationship("ParsingFile", back_populates="session", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<ParsingSession(session_id={self.session_id}, status={self.status})>"


class ParsingFile(Base):
    """Файл лога — метаданные (SQLite)"""
    __tablename__ = "parsing_files"

    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(String(36), ForeignKey("parsing_sessions.session_id"), index=True)

    # Информация о файле
    file_name = Column(String(255), nullable=False)  # например: 26030613.log
    file_path = Column(String(500), nullable=False)

    # Статус обработки
    status = Column(String(20), default="pending")
    events_count = Column(Integer, default=0)  # событий из этого файла в ClickHouse
    error_message = Column(Text, nullable=True)

    # Временные метки
    processed_at = Column(DateTime, nullable=True)

    # 🔧 Связь с сессией
    session = relationship("ParsingSession", back_populates="files")

    def __repr__(self):
        return f"<ParsingFile(file_name={self.file_name}, status={self.status})>"


# ============================================================================
# PYDANTIC СХЕМЫ (для API валидации)
# ============================================================================

class ParsingRequest(BaseModel):
    """Запрос на запуск парсинга"""
    start_date: datetime = Field(..., description="Начальная дата диапазона")
    end_date: datetime = Field(..., description="Конечная дата диапазона")
    directory_name: Optional[str] = Field(None, description="Фильтр по каталогу (инфобазе)")

    @classmethod
    def end_must_be_after_start(cls, v, info):
        start = info.data.get('start_date')
        if start and v <= start:
            raise ValueError('end_date должен быть больше start_date')
        return v

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "start_date": "2026-03-06T13:00:00",
                "end_date": "2026-03-06T13:04:59",
                "directory_name": "1C_Culprits"
            }
        }
    )


class ParsingResponse(BaseModel):
    """Ответ после запуска парсинга"""
    session_id: str = Field(..., description="UUID сессии парсинга")
    status: str = Field(..., description="Статус: started, pending, failed")
    message: str = Field(..., description="Сообщение для пользователя")

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
    current_file: Optional[Dict[str, Any]] = None


class ParsingSessionInfo(BaseModel):
    """Информация о сессии для списка"""
    session_id: str
    status: str
    start_date: datetime
    end_date: datetime
    directory_name: Optional[str] = None
    progress_percent: float
    total_events: int
    total_files: int
    processed_files: int
    created_at: datetime
    completed_at: Optional[datetime] = None


# ============================================================================
# СХЕМЫ ДЛЯ СОБЫТИЙ (только для API, не для БД!)
# ============================================================================

class EventDetail(BaseModel):
    """Детали события из ClickHouse (только для API ответа)"""
    id: Optional[int] = None
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