# app/database.py
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Text, ForeignKey, func
from sqlalchemy.orm import relationship, declarative_base, sessionmaker
from app.config import settings
import clickhouse_connect
import logging

Base = declarative_base()


# ============================================================================
# SQLITE МОДЕЛИ (только метаданные парсинга)
# ============================================================================

class ParsingSession(Base):
    """Сессия парсинга — метаданные"""
    __tablename__ = "parsing_sessions"

    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(String(36), unique=True, index=True)

    # Параметры запроса
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    directory_name = Column(String(255), nullable=True)  # фильтр по инфобазе

    # Статус
    status = Column(String(20), default="pending")  # pending, running, completed, failed
    progress_percent = Column(Float, default=0.0)
    error_message = Column(Text, nullable=True)

    # Статистика
    total_files = Column(Integer, default=0)
    processed_files = Column(Integer, default=0)
    total_events = Column(Integer, default=0)  # счётчик событий в ClickHouse

    # Временные метки
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    comment = Column(Text, nullable=True, default="")
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Связи
    files = relationship("ParsingFile", back_populates="session", cascade="all, delete-orphan")


class ParsingFile(Base):
    """Файл лога — метаданные"""
    __tablename__ = "parsing_files"

    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(String(36), ForeignKey("parsing_sessions.session_id"), index=True)

    file_name = Column(String(255), nullable=False)  # 26030610.log
    file_path = Column(String(500), nullable=False)

    status = Column(String(20), default="pending")
    events_count = Column(Integer, default=0)  # событий из этого файла в ClickHouse
    error_message = Column(Text, nullable=True)
    processed_at = Column(DateTime, nullable=True)

    session = relationship("ParsingSession", back_populates="files")


# ============================================================================
# SQLITE ENGINE & SESSION
# ============================================================================

engine = create_engine(
    f"sqlite:///{settings.database.sqlite_path}",
    connect_args={"check_same_thread": False}
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """Dependency для FastAPI — сессия SQLite"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def create_tables():
    """Создать таблицы в SQLite (только метаданные!)"""
    Base.metadata.create_all(bind=engine)


# ============================================================================
# CLICKHOUSE CLIENT
# ============================================================================

def get_clickhouse_client():
    """Получить клиент ClickHouse"""
    return clickhouse_connect.get_client(
        host=settings.database.clickhouse.host,
        port=int(settings.database.clickhouse.port),
        database=settings.database.clickhouse.database,
        user=settings.database.clickhouse.user,
        password=settings.database.clickhouse.password,
        secure=False,
        verify=False,
        send_receive_timeout=300
    )


def create_clickhouse_table_if_not_exists():
    """Создать таблицу событий в ClickHouse если не существует"""
    client = get_clickhouse_client()

    create_query = """
    CREATE TABLE IF NOT EXISTS {database}.tj_events
    (
        id UInt64,
        session_id String,
        file_id UInt64,

        -- Основные поля
        timestamp DateTime64(6, 'Europe/Moscow'),
        directory_name LowCardinality(String),
        source_file String,
        line_number UInt64,
        loaded_at DateTime64(6, 'Europe/Moscow'),

        -- Событие
        event_name LowCardinality(String),
        level UInt8,
        duration_ms Float32,

        -- Контекст
        p_processName LowCardinality(String),
        t_computerName String,
        t_connectID String,
        usr LowCardinality(String),
        dbpid String,
        osThread String,
        sessionID String,
        trans String,
        func_name String CODEC(ZSTD(3)),
        table_name LowCardinality(String),
        context String CODEC(ZSTD(3)),

        -- SQL
        sql String CODEC(ZSTD(3)),
        sdbl String CODEC(ZSTD(3)),
        plan_sql_text String CODEC(ZSTD(3)),

        -- Ошибки
        exception String CODEC(ZSTD(3)),
        sql_error_code Nullable(UInt32),

        -- Блокировки
        locks String,
        wait_connections String,
        deadlock_connection_intersections String,

        -- Детали блокировок
        lkaid String, lka String, lkp String, lkpid String, lksrc String,

        -- Результаты
        rows String, rows_affected String,

        -- Дополнительно
        description String, data String CODEC(ZSTD(3)),

        -- Аналитика
        severity LowCardinality(String),
        category LowCardinality(String),

        -- Индексы
        INDEX idx_timestamp timestamp TYPE minmax GRANULARITY 8192,
        INDEX idx_session session_id TYPE bloom_filter GRANULARITY 8192,
        INDEX idx_event_name event_name TYPE bloom_filter GRANULARITY 8192,
        INDEX idx_severity severity TYPE bloom_filter GRANULARITY 8192,
        INDEX idx_category category TYPE bloom_filter GRANULARITY 8192,
        INDEX idx_directory directory_name TYPE bloom_filter GRANULARITY 8192
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(toDate(timestamp))
    ORDER BY (timestamp, directory_name, event_name, session_id)
    TTL toDate(timestamp) + INTERVAL 90 DAY
    SETTINGS 
        index_granularity = 8192,
        compress_marks = true,
        enable_mixed_granularity_parts = true
    """.format(database=settings.database.clickhouse.database)

    client.command(create_query)
    logging.info("✓ ClickHouse table tj_events verified")