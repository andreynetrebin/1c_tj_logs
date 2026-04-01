# 📊 1C TJ Logs Analyzer

> **Статус**: 🟡 Active Development | **Версия**: 0.3.1 | **Обновлено**: {{ git_revision_date_localized }}

[![GitHub stars](https://img.shields.io/github/stars/andreynetrebin/1c_tj_logs?style=flat)](https://github.com/andreynetrebin/1c_tj_logs)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.109.0-009688?logo=fastapi)](https://fastapi.tiangolo.com)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-Analytics-FFCC01?logo=clickhouse)](https://clickhouse.com)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python)](https://python.org)

## 🎯 Бизнес-контекст

| Параметр | Значение |
|----------|----------|
| **Домен** | 1С:Предприятие, DevOps, Observability |
| **Цель** | Автоматизация анализа технологических журналов 1С для выявления проблем производительности |
| **Задачи** | • Парсинг `.log` файлов с продолжением с последней позиции<br>• Структурирование событий в ClickHouse<br>• Визуализация метрик: длительные запросы, блокировки, ошибки MSSQL<br>• Экспорт результатов в CSV/XLSX для отчётности |
| **Целевая аудитория** | Администраторы 1С, DevOps-инженеры, разработчики платформы |
| **Бизнес-ценность** | Сокращение времени диагностики инцидентов с часов до минут, проактивное выявление узких мест |

## 🔍 Ключевые возможности

```mermaid
graph LR
    A[Тех. журналы 1С] --> B[Парсер TJLogParser]
    B --> C{Фильтрация по времени}
    C --> D[ClickHouse: tj_events]
    D --> E[FastAPI REST]
    D --> F[Web UI: Bootstrap 5]
    E & F --> G[Аналитика]
    G --> H[Топ ошибок]
    G --> I[Медленные запросы]
    G --> J[Блокировки/Deadlocks]
    G --> K[Экспорт CSV/XLSX]
    
    style D fill:#4ECDC4,stroke:#333
    style E fill:#45B7D1,stroke:#333


✨ Функционал
Модуль
Возможности
Парсер
• Чтение .log с кодировкой UTF-8
• Генератор для потоковой обработки
• Фильтрация по диапазону времени с точностью до миллисекунд
Хранение
• ClickHouse: колоночное хранение + сжатие ZSTD
• SQLite: метаданные сессий парсинга (state tracking)
• TTL: авто-удаление данных старше 90 дней
API
• REST endpoints с валидацией Pydantic v2
• Swagger UI: /docs
• Асинхронные запросы, пагинация, фильтрация
Web UI
• Интерфейс на Bootstrap 5 + Jinja2
• Фильтры: сессия, пользователь, уровень, длительность, MSSQL код ошибки
• Модальные окна с деталями события + SQL-запросом
Экспорт
• Выгрузка отфильтрованных данных в CSV / XLSX
• Поддержка больших объёмов через streaming


📈 Метрики проекта

performance:
  parsing_speed: "~50K событий/мин на ядро"
  clickhouse_insert: "batch=50000, ~2 сек/пакет"
  query_latency: "< 500ms для фильтрованных выборок"
  
data_volume:
  avg_event_size: "~2 KB"
  daily_ingest: "1-10 GB в зависимости от нагрузки 1С"
  retention: "90 дней (TTL в ClickHouse)"
  
reliability:
  resume_support: "продолжение с последней позиции файла"
  error_handling: "логирование + retry для ClickHouse"
  state_persistence: "SQLite для отслеживания прогресса"


🚀 Быстрый старт

git clone https://github.com/andreynetrebin/1c_tj_logs
cd 1c_tj_logs
pip install -r requirements.txt
cp .env.example .env
# Отредактируйте .env под ваше окружение
uvicorn app.main:app --reload

📚 Полная документация: GitHub Repository


### 1.4 Создайте `docs/portfolio/architecture.md`

```markdown
# 🏗️ Архитектура решения

## 🗺️ Компонентная диаграмма

```mermaid
flowchart TB
    subgraph Sources ["📁 Источники данных"]
        A[1С:Предприятие] -->|тех. журналы .log| B[Файловая система]
    end

    subgraph Parser ["🔄 Парсер (Python)"]
        B --> C[TJLogParser]
        C -->|generator| D[Фильтр по времени]
        D --> E[Категоризация: error/perf/lock]
        E --> F[Enrich: severity, category]
    end

    subgraph Storage ["💾 Хранение"]
        F -->|batch insert| G[ClickHouse: tj_events]
        F -->|metadata| H[SQLite: parsing_sessions]
        G -->|TTL 90d| I[Авто-очистка]
    end

    subgraph Serving ["🌐 API + UI"]
        G --> J[FastAPI: /api/events]
        H --> J
        J --> K[Swagger UI: /docs]
        J --> L[Web UI: Bootstrap 5]
        L --> M[Фильтры + экспорт CSV]
    end

    style G fill:#4ECDC4,stroke:#333,stroke-width:2px
    style J fill:#45B7D1,stroke:#333


🧩 Модульная структура
1c_tj_logs/
├── app/
│   ├── main.py              # FastAPI app factory + lifespan
│   ├── config.py            # Pydantic Settings из .env
│   ├── database.py          # SQLAlchemy + clickhouse-connect
│   ├── models.py            # SQLite ORM: ParsingSession, ParsingFile
│   ├── schemas.py           # Pydantic: Request/Response схемы
│   ├── api/
│   │   ├── routes.py        # APIRouter с префиксом /api
│   │   └── endpoints/
│   │       ├── events.py    # GET /events, POST /export
│   │       ├── parsing.py   # POST /parsing/start, GET /progress/{id}
│   │       └── analysis.py  # GET /analysis/{id} — агрегации
│   ├── parser/
│   │   └── tj_parser.py     # TJLogParser: streaming parsing
│   └── services/
│       └── clickhouse_service.py  # Batch insert, query helpers
├── templates/               # Jinja2: index, events, analysis
├── static/                  # CSS/JS assets
├── data/                    # SQLite: state.db
├── logs/                    # RotatingFileHandler: app.log
└── .env                     # 🔐 Конфигурация (не в git!)


🔁 Жизненный цикл сессии парсинга
sequenceDiagram
    participant User as 👤 Пользователь
    participant API as ⚡ FastAPI
    participant DB as 💾 SQLite
    participant Parser as 🔄 TJLogParser
    participant CH as 🗄️ ClickHouse

    User->>API: POST /api/parsing/start {start_date, end_date}
    API->>DB: INSERT parsing_session (status=pending)
    API-->>User: 202 {session_id, status: "started"}
    
    API->>Parser: Запуск в background task
    Parser->>DB: UPDATE status=running, started_at=NOW()
    
    loop Для каждого .log файла
        Parser->>Parser: Чтение с последней позиции
        Parser->>Parser: Парсинг + фильтрация по времени
        Parser->>CH: INSERT batch (50K событий)
        Parser->>DB: UPDATE processed_files, total_events
    end
    
    Parser->>DB: UPDATE status=completed, completed_at=NOW()
    
    User->>API: GET /api/parsing/progress/{session_id}
    API->>DB: SELECT progress metrics
    API-->>User: 200 {progress_percent, current_file, eta}



### 1.5 Создайте GitHub Action для уведомления: `.github/workflows/notify-portfolio.yml`

```yaml
# .github/workflows/notify-portfolio.yml
name: 🔄 Notify Portfolio on Docs Change

on:
  push:
    branches: [main]
    paths:
      - 'docs/portfolio/**'
  workflow_dispatch:

jobs:
  trigger-sync:
    runs-on: ubuntu-latest
    steps:
      - name: 📡 Отправить webhook в портфолио
        run: |
          curl -X POST \
            -H "Authorization: token ${{ secrets.PORTFOLIO_SYNC_TOKEN }}" \
            -H "Content-Type: application/json" \
            -d "{
              \"event_type\": \"docs_updated\",
              \"client_payload\": {
                \"project\": \"1c_tj_logs\",
                \"commit\": \"${{ github.sha }}\",
                \"ref\": \"${{ github.ref }}\"
              }
            }" \
            https://api.github.com/repos/andreynetrebin/portfolio/dispatches