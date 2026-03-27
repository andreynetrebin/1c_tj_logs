# 📊 1C TJ Logs Analyzer

> Веб-приложение для парсинга, анализа и визуализации технологических журналов 1С:Предприятие с хранением данных в ClickHouse.

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.109+-green.svg)](https://fastapi.tiangolo.com/)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-24.1+-orange.svg)](https://clickhouse.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 📋 Оглавление

- [О проекте](#-о-проекте)
- [Возможности](#-возможности)
- [Технологический стек](#-технологический-стек)
- [Структура проекта](#-структура-проекта)
- [Быстрый старт](#-быстрый-старт)
- [Конфигурация](#-конфигурация)
- [API Endpoints](#-api-endpoints)
- [Разработка](#-разработка)
- [Data Contract](#-data-contract)
- [Troubleshooting](#-troubleshooting)
- [Лицензия](#-лицензия)

---

## 🔍 О проекте

**1C TJ Logs Analyzer** — это инструмент для DevOps и разработчиков 1С, который позволяет:

- 🔄 Автоматически парсить технологические журналы 1С из указанной директории
- 💾 Сохранять структурированные данные в **ClickHouse** для быстрого анализа
- 📈 Визуализировать метрики производительности, ошибки и длительные операции
- 🔎 Искать и фильтровать события по параметрам: сессия, пользователь, время, тип события, код ошибки MSSQL
- 📤 Экспортировать результаты в CSV/XLSX с учётом применённых фильтров
- ⏱ Фильтровать события по диапазону времени с точностью до миллисекунд

Проект разработан в рамках инициативы по построению наблюдаемости (observability) для 1С-систем.

---

## ✨ Возможности

| Функция | Описание |
|---------|----------|
| **Парсинг логов** | Чтение `.log` файлов 1С с поддержкой продолжения с последней позиции |
| **ClickHouse интеграция** | Быстрая загрузка и аналитика через колоночное хранилище |
| **SQLite для состояния** | Отслеживание прогресса парсинга и метаданных |
| **REST API** | Полноценный API на FastAPI с документацией Swagger |
| **Web UI** | Интерактивный интерфейс с фильтрами, сортировкой и модальными окнами |
| **Асинхронность** | Неблокирующая обработка запросов и фоновые задачи |
| **Гибкая конфигурация** | Все настройки через `.env`, поддержка разных окружений |
| **Экспорт данных** | Выгрузка отфильтрованных событий в CSV и XLSX |
| **Фильтр по времени** | Поиск событий в диапазоне с точностью до миллисекунд |
| **Авто-заполнение фильтров** | Границы сессии автоматически подставляются в поля времени |

---

## 🛠 Технологический стек
┌─────────────────────────────────────┐
│ Frontend │
│ • Bootstrap 5 + Jinja2 templates │
│ • Vanilla JavaScript (ES6+) │
│ • Chart.js для визуализации │
└─────────────────────────────────────┘
│
┌─────────────────────────────────────┐
│ Backend (FastAPI) │
│ • Pydantic v2 для валидации │
│ • Async/await обработка │
│ • Structured logging │
└─────────────────────────────────────┘
│
┌──────────┴──────────┐
▼ ▼
┌───────────────┐ ┌─────────────────┐
│ ClickHouse │ │ SQLite │
│ • Аналитика │ │ • State DB │
│ • Хранение │ │ • Миграции │
└───────────────┘ └─────────────────┘


**Зависимости** (`requirements.txt`):
```txt
fastapi==0.109.0
uvicorn[standard]==0.27.0
clickhouse-connect==0.7.1
pydantic==2.5.3
pydantic-settings==2.1.0
pandas==2.2.0
sqlalchemy==2.0.25
aiosqlite==0.19.0
python-multipart==0.0.6
jinja2==3.1.3
aiofiles==23.2.1
python-dateutil==2.8.2
openpyxl==3.1.2

1c_tj_logs/
├── app/
│   ├── __init__.py
│   ├── main.py              # Точка входа, инициализация FastAPI
│   ├── config.py            # Конфигурация через .env (Pydantic)
│   ├── database.py          # Подключения к ClickHouse и SQLite
│   ├── models.py            # SQLAlchemy модели для SQLite
│   ├── schemas.py           # Pydantic схемы для API
│   ├── api/
│   │   ├── __init__.py
│   │   ├── routes.py        # Роутер для подключения эндпоинтов
│   │   ├── endpoints/
│   │   │   ├── __init__.py
│   │   │   ├── events.py    # HTTP роуты для событий (/events, /export)
│   │   │   ├── parsing.py   # Эндпоинты парсинга
│   │   │   ├── analysis.py  # Эндпоинты аналитики
│   │   │   ├── sessions.py  # Управление сессиями
│   │   │   └── admin.py     # Административные функции
│   │   └── dependencies.py  # Depends для аутентификации и т.д.
│   ├── parser/
│   │   ├── __init__.py
│   │   ├── tj_parser.py     # Логика парсинга 1С логов
│   │   └── utils.py         # Вспомогательные функции
│   ├── services/
│   │   ├── __init__.py
│   │   ├── parsing_service.py  # Бизнес-логика парсинга
│   │   └── clickhouse_service.py  # Работа с ClickHouse
│   ├── static/
│   │   └── css/
│   │       └── style.css    # Стили веб-интерфейса
│   └── templates/
│       ├── base.html        # Базовый шаблон Jinja2
│       ├── index.html       # Главная страница
│       ├── events.html      # Просмотр событий с фильтрами и экспортом
│       ├── analysis.html    # Страница аналитики
│       └── sessions.html    # Список сессий парсинга
├── logs/                    # Логи приложения (авто-создаётся)
├── data/                    # SQLite базы данных
├── .env                     # 🔐 Секреты и настройки (НЕ в git!)
├── .env.example             # Шаблон для копирования
├── .gitignore               # Исключения для git
├── requirements.txt         # Зависимости Python
└── README.md                # Этот файл


git clone https://github.com/andreynetrebin/1c_tj_logs.git
cd 1c_tj_logs


# Windows
python -m venv venv
venv\Scripts\activate

# Linux/Mac
python3 -m venv venv
source venv/bin/activate


pip install -r requirements.txt


# Скопируйте шаблон
cp .env.example .env

# Отредактируйте .env под ваше окружение
# (см. раздел Конфигурация ниже)


# Режим разработки (с авто-перезагрузкой)
uvicorn app.main:app --reload --host 0.0.0.0 --port 9188

# Продакшен-режим
uvicorn app.main:app --host 0.0.0.0 --port 9188 --workers 4

6. Проверка
🌐 Веб-интерфейс: http://localhost:9188
📚 Swagger API docs: http://localhost:9188/docs
🔧 ReDoc: http://localhost:9188/redoc

⚙️ Конфигурация
Все настройки задаются только через файл .env в корне проекта.
📄 Шаблон .env.example

# ==========================================
# DATABASE - ClickHouse
# ==========================================
DATABASE_CLICKHOUSE_HOST="192.168.2.173"
DATABASE_CLICKHOUSE_PORT=8123
DATABASE_CLICKHOUSE_DATABASE="NetrebinAA"
DATABASE_CLICKHOUSE_USER="ANetrebin"
DATABASE_CLICKHOUSE_PASSWORD="your_secure_password"

# ==========================================
# DATABASE - SQLite (state tracking)
# ==========================================
DATABASE_SQLITE_PATH="C:/projects/1c_tj_logs/data/state.db"

# ==========================================
# PARSER SETTINGS
# ==========================================
PARSER_LOG_DIRECTORY="Y:/Monitor/Happywear_New/"
PARSER_BATCH_SIZE=50000
PARSER_MAX_PROCESSES=4
PARSER_LOG_FILE="C:/projects/1c_tj_logs/logs/app.log"

# ==========================================
# SERVER
# ==========================================
SERVER_HOST="0.0.0.0"
SERVER_PORT=9188
SERVER_RELOAD=true

# ==========================================
# LOGGING
# ==========================================
LOGGING_LEVEL="INFO"
LOGGING_FILE="C:/projects/1c_tj_logs/logs/app.log"

📄 Лицензия
Распространяется под лицензией MIT. См. файл LICENSE для деталей.
