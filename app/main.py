# app/main.py — НАЧАЛО ФАЙЛА

"""
Главный модуль веб-приложения для парсинга и анализа технологических журналов 1С
FastAPI + SQLite + ClickHouse
"""

# ============================================================================
# 🔧 ИМПОРТЫ
# ============================================================================

# Стандартные библиотеки
import logging
import logging.handlers
from pathlib import Path
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from zoneinfo import ZoneInfo  # Python 3.9+
from typing import Optional, List  # or other needed types


# FastAPI
from fastapi import FastAPI, Request, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse


# 🔧 SQLAlchemy — ОБЯЗАТЕЛЬНО для type hint Session!
from sqlalchemy.orm import Session  # ← 🔧 ДОБАВИТЬ ЭТУ СТРОКУ!

# Локальные модули
from app.config import settings
from app.database import engine, create_tables, SessionLocal, get_db  # ← get_db уже здесь
from app.api.routes import api_router
from app.models import ParsingSession
# ============================================================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
# ============================================================================


_logging_configured = False

def setup_logging():
    """
    Настроить логирование в файл и консоль (только один раз!)
    """
    global _logging_configured
    if _logging_configured:
        return  # Не настраивать повторно при reload!
    
    # Директория для логов
    log_dir = Path(__file__).resolve().parent.parent / "logs"
    log_dir.mkdir(exist_ok=True, parents=True)
    
    log_file = log_dir / "app.log"
    
    # Формат сообщений
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File handler с ротацией (10 MB, 5 backup файлов)
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding='utf-8',
        delay=True  # Отложенное создание файла
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(getattr(logging, settings.logging.level.upper(), logging.INFO))
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(getattr(logging, settings.logging.level.upper(), logging.INFO))
    
    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, settings.logging.level.upper(), logging.INFO))
    
    # Очищаем старые хендлеры (критично при reload=True!)
    root_logger.handlers.clear()
    
    # Добавляем новые хендлеры
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Настройка логгера uvicorn
    uvicorn_logger = logging.getLogger("uvicorn")
    uvicorn_logger.handlers.clear()
    uvicorn_logger.addHandler(file_handler)
    uvicorn_logger.addHandler(console_handler)
    
    # Отключаем дублирование логов от uvicorn.access
    logging.getLogger("uvicorn.access").propagate = False
    
    _logging_configured = True
    logging.info(f"✓ Logging configured. Log file: {log_file}")

def to_msk(dt):
    """Конвертирует datetime в московский часовой пояс (UTC+3)"""
    if dt is None:
        return None
    # Если дата без таймзоны — считаем что это UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    # Конвертируем в МСК
    msk_tz = ZoneInfo('Europe/Moscow')
    return dt.astimezone(msk_tz)

# ============================================================================
# LIFESPAN: Управление жизненным циклом приложения
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Современный способ обработки startup/shutdown событий
    """
    # === STARTUP ===
    logging.info("🚀 Application startup")
    
    try:
        # Инициализация БД
        create_tables()
        logging.info("✓ Database tables created/verified")
        
        # Проверка подключения к ClickHouse (опционально)
        try:
            from app.database import get_clickhouse_client
            ch_client = get_clickhouse_client()
            ch_client.command("SELECT 1")
            logging.info("✓ ClickHouse connection verified")
        except Exception as e:
            logging.warning(f"⚠ ClickHouse connection check failed: {e}")
        
        yield  # Приложение работает
        
    except Exception as e:
        logging.error(f"❌ Startup error: {e}")
        raise
    
    finally:
        # === SHUTDOWN ===
        logging.info("🛑 Application shutdown")
        # Здесь можно добавить очистку ресурсов, закрытие соединений и т.д.


# ============================================================================
# СОЗДАНИЕ ПРИЛОЖЕНИЯ
# ============================================================================

def create_application() -> FastAPI:
    """
    Создать и настроить FastAPI приложение
    """
    # Настроить логирование ПЕРВЫМ
    setup_logging()
    
    application = FastAPI(
        title="1C TJ Log Analyzer",
        description="Парсинг и анализ технологических журналов 1С",
        version="1.0.0",
        lifespan=lifespan,  # Современный способ вместо @app.on_event()
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json"
    )
    
    # ========================================================================
    # MIDDLEWARE
    # ========================================================================
    
    # CORS - разрешаем запросы с любых источников (для разработки)
    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # ========================================================================
    # STATIC FILES & TEMPLATES
    # ========================================================================
    
    # Директория проекта
    BASE_DIR = Path(__file__).resolve().parent.parent
    
    # Статические файлы (CSS, JS, изображения)
    static_dir = BASE_DIR / "app" / "static"
    if static_dir.exists():
        application.mount("/static", StaticFiles(directory=static_dir), name="static")
        logging.info(f"✓ Static files mounted: {static_dir}")
    
    # Шаблоны Jinja2
    # app/main.py — в функции create_application(), после создания templates

    # Шаблоны Jinja2
    templates_dir = BASE_DIR / "templates"
    if templates_dir.exists():
        templates = Jinja2Templates(directory=templates_dir)

        # 🔧 РЕГИСТРАЦИЯ КАСТОМНЫХ ФИЛЬТРОВ
        templates.env.filters['to_msk'] = to_msk
        templates.env.filters['localized_status'] = lambda s: {
            'pending': 'Ожидает',
            'running': 'Выполняется',
            'completed': 'Завершено',
            'failed': 'Ошибка'
        }.get(s, s)
        # app/main.py — в create_application(), после регистрации to_msk и localized_status

        # 🔧 Дополнительные фильтры для таблицы сессий
        templates.env.filters['status_color'] = lambda s: {
            'pending': 'secondary',
            'running': 'primary',
            'completed': 'success',
            'failed': 'danger'
        }.get(s, 'secondary')

        templates.env.filters['format_number'] = lambda n: f"{int(n):,}".replace(",", " ") if n else "0"

        application.state.templates = templates
        logging.info(f"✓ Templates mounted: {templates_dir}")
    # ========================================================================
    # API ROUTES
    # ========================================================================
    
    # Подключаем API роутеры с префиксом /api
    application.include_router(api_router, prefix="/api")
    
    # ========================================================================
    # WEB PAGES (HTML)
    # ========================================================================
    
    @application.get("/", tags=["pages"])
    async def index(request: Request):
        """Главная страница"""
        templates = request.app.state.templates
        return templates.TemplateResponse("index.html", {"request": request})
    
    @application.get("/interval", tags=["pages"])
    async def interval_page(request: Request):
        """
        Страница выбора временного интервала для парсинга
        Автозаполнение: [текущее время - 5 минут, текущее время]
        """
        templates = request.app.state.templates
        return templates.TemplateResponse("interval.html", {"request": request})

    @application.get("/parsing", tags=["pages"])
    async def parsing_page(request: Request):
        """Страница запуска парсинга (алиас на /interval)"""
        templates = request.app.state.templates
        return templates.TemplateResponse("interval.html", {"request": request})

    @application.get("/parsing/progress/{session_id}", tags=["pages"])
    async def parsing_progress_page(request: Request, session_id: str, db: Session = Depends(get_db)):
        """Страница прогресса парсинга"""
        templates = request.app.state.templates

        # 🔧 Получаем прогресс из БД
        from app.models import ParsingSession
        session = db.query(ParsingSession).filter(
            ParsingSession.session_id == session_id
        ).first()

        if not session:
            raise HTTPException(status_code=404, detail="Сессия не найдена")

        # 🔧 Формируем объект progress для шаблона
        from datetime import datetime
        elapsed_seconds = None
        if session.started_at:
            end_time = session.completed_at or datetime.utcnow()
            elapsed_seconds = (end_time - session.started_at).total_seconds()

        progress = {
            "session_id": session.session_id,
            "status": session.status,
            "progress_percent": session.progress_percent or 0.0,
            "total_files": session.total_files or 0,
            "processed_files": session.processed_files or 0,
            "total_events": session.total_events or 0,
            "error_message": session.error_message,
            "started_at": session.started_at.isoformat() if session.started_at else None,
            "completed_at": session.completed_at.isoformat() if session.completed_at else None,
            "elapsed_seconds": elapsed_seconds,
            # 🔧 Информация о текущем файле
            "current_file": {
                "name": session.current_file_name,
                "path": session.current_file_path,
                "size_bytes": session.current_file_size,
                "size_mb": round(session.current_file_size / 1024 / 1024, 2) if session.current_file_size else None,
                "events_count": session.current_file_events or 0
            } if session.status == "running" and session.current_file_name else None
        }

        return templates.TemplateResponse("parsing_progress.html", {
            "request": request,
            "session_id": session_id,
            "status": session.status,  # 🔧 Добавить статус для шаблона
            "progress": progress  # 🔧 ПЕРЕДАЁМ progress в шаблон!
        })

    @application.get("/analysis/{session_id}", tags=["pages"])
    async def analysis_page(request: Request, session_id: str):
        """Страница анализа результатов парсинга"""
        templates = request.app.state.templates

        # Проверить что сессия существует
        db = SessionLocal()
        session = db.query(ParsingSession).filter(
            ParsingSession.session_id == session_id
        ).first()
        db.close()

        if not session:
            raise HTTPException(status_code=404, detail="Сессия не найдена")

        return templates.TemplateResponse("analysis.html", {
            "request": request,
            "session_id": session_id,
            "session": session
        })
    
    @application.get("/events/{session_id}", tags=["pages"])
    async def events_page(request: Request, session_id: str):
        """Страница просмотра событий"""
        templates = request.app.state.templates
        return templates.TemplateResponse(
            "events.html",
            {"request": request, "session_id": session_id}
        )
    
    # ========================================================================
    # HEALTH & UTILITY ENDPOINTS
    # ========================================================================
    
    @application.get("/health", tags=["system"])
    async def health_check():
        """
        Проверка работоспособности сервиса
        Используется для monitoring/load balancer health checks
        """
        health_status = {
            "status": "ok",
            "service": "1c-tj-analyzer",
            "version": "1.0.0",
            "timestamp": datetime.utcnow().isoformat(),
            "database": {
                "sqlite": "connected" if engine else "disconnected"
            }
        }
        
        # Проверка ClickHouse (опционально)
        try:
            from app.database import get_clickhouse_client
            ch_client = get_clickhouse_client()
            ch_client.command("SELECT 1")
            health_status["database"]["clickhouse"] = "connected"
        except Exception as e:
            health_status["database"]["clickhouse"] = f"error: {str(e)}"
            health_status["status"] = "degraded"
        
        return health_status
    
    @application.get("/api/status", tags=["system"])
    async def api_status():
        """Статус API (упрощённый health check)"""
        return {
            "ok": True,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    @application.get("/api/config", tags=["system"])
    async def get_config():
        """
        Возвращает конфигурацию приложения (без чувствительных данных)
        """
        return {
            "app_name": "1C TJ Log Analyzer",
            "version": "1.0.0",
            "server": {
                "host": settings.server.host,
                "port": settings.server.port,
                "reload": settings.server.reload
            },
            "parser": {
                "log_directory": settings.parser.log_directory,
                "batch_size": settings.parser.batch_size,
                "max_processes": settings.parser.max_processes
            },
            "logging": {
                "level": settings.logging.level,
                "file": settings.logging.file
            }
        }
    
    # ========================================================================
    # ERROR HANDLERS
    # ========================================================================

    @application.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        """Обработчик HTTP ошибок"""
        logging.warning(f"HTTP {exc.status_code}: {exc.detail} - {request.url}")
        return JSONResponse(  # ← Возвращаем JSONResponse, не dict!
            status_code=exc.status_code,
            content={"detail": exc.detail, "status_code": exc.status_code}
        )

    @application.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        """Обработчик необработанных исключений"""
        logging.error(f"Unhandled exception: {exc}", exc_info=True)
        return JSONResponse(  # ← Возвращаем JSONResponse, не dict!
            status_code=500,
            content={"detail": "Внутренняя ошибка сервера", "status_code": 500}
        )

    # app/main.py — добавьте после других @app.get() маршрутов

    # === Страница списка сессий ===
    @application.get("/sessions")
    async def sessions_page(
            request: Request,
            db: Session = Depends(get_db),
            status: Optional[str] = Query(None),
            date_from: Optional[str] = Query(None),
            date_to: Optional[str] = Query(None)
    ):
        """Страница списка сессий парсинга с фильтрами"""
        templates = request.app.state.templates

        from app.database import ParsingSession
        from datetime import datetime

        # 🔧 Построение запроса с фильтрами
        query = db.query(ParsingSession)

        if status and status != 'all':
            query = query.filter(ParsingSession.status == status)

        if date_from:
            try:
                dt_from = datetime.fromisoformat(date_from)
                query = query.filter(ParsingSession.created_at >= dt_from)
            except:
                pass

        if date_to:
            try:
                dt_to = datetime.fromisoformat(date_to)
                query = query.filter(ParsingSession.created_at <= dt_to)
            except:
                pass

        # Запрос сессий
        sessions = query.order_by(
            ParsingSession.created_at.desc()
        ).limit(100).all()

        # Подготовка данных для шаблона
        sessions_data = []
        for s in sessions:
            elapsed = None
            if s.started_at:
                end_time = s.completed_at or datetime.utcnow()
                elapsed = (end_time - s.started_at).total_seconds()

            sessions_data.append({
                "session_id": s.session_id,
                "status": s.status,
                "start_date": s.start_date,
                "end_date": s.end_date,
                "directory_name": s.directory_name,
                "comment": s.comment or "",
                "progress_percent": s.progress_percent or 0,
                "total_events": s.total_events or 0,
                "total_files": s.total_files or 0,
                "processed_files": s.processed_files or 0,
                "started_at": s.started_at,
                "completed_at": s.completed_at,
                "created_at": s.created_at,
                "elapsed_seconds": elapsed,
                "error_message": s.error_message
            })

        return templates.TemplateResponse("sessions.html", {
            "request": request,
            "sessions": sessions_data,
            # 🔧 Передаём текущие фильтры обратно в шаблон для сохранения состояния
            "filter_status": status,
            "filter_date_from": date_from,
            "filter_date_to": date_to
        })

    # app/main.py — добавьте после других маршрутов

    # === Админ-панель ===
    @application.get("/admin")
    async def admin_page(request: Request):
        """Страница админ-панели для управления сессиями"""
        templates = request.app.state.templates
        return templates.TemplateResponse("admin.html", {"request": request})


    # ========================================================================
    # LOGGING STARTUP MESSAGE
    # ========================================================================
    
    logging.info(f"🔧 FastAPI app created. Routes registered.")
    
    return application


# ============================================================================
# ГЛОБАЛЬНЫЙ ЭКЗЕМПЛЯР ПРИЛОЖЕНИЯ
# ============================================================================

app = create_application()

# ============================================================================
# ЗАПУСК ПРИЛОЖЕНИЯ (python -m app.main)
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    logging.info(f"🚀 Starting server on {settings.server.host}:{settings.server.port}")
    
    uvicorn.run(
        "app.main:app",
        host=settings.server.host,
        port=settings.server.port,
        reload=settings.server.reload,
        log_level=settings.logging.level.lower(),
        access_log=True,
    )
