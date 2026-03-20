# app/config.py
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel, Field
from typing import Optional


# --- Вложенные модели (Просто данные, не читают .env сами) ---

class ClickHouseSettings(BaseModel):
    host: str
    port: int
    database: str
    user: str
    password: str


class DatabaseSettings(BaseModel):
    clickhouse: ClickHouseSettings
    sqlite_path: str


class ParserSettings(BaseModel):
    log_directory: str
    batch_size: int
    max_processes: int
    log_file: Optional[str] = None


class ServerSettings(BaseModel):
    host: str
    port: int
    reload: bool


class LoggingSettings(BaseModel):
    level: str
    file: str


# --- Главная настройка (Единственная читает .env) ---

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        # strict=True заставит программу упасть, если тип не совпадает или поле отсутствует
        extra="ignore"
    )

    # Поля с префиксами, соответствующими вашему .env
    database_clickhouse_host: str = Field(..., alias="DATABASE_CLICKHOUSE_HOST")
    database_clickhouse_port: int = Field(..., alias="DATABASE_CLICKHOUSE_PORT")
    database_clickhouse_database: str = Field(..., alias="DATABASE_CLICKHOUSE_DATABASE")
    database_clickhouse_user: str = Field(..., alias="DATABASE_CLICKHOUSE_USER")
    database_clickhouse_password: str = Field(..., alias="DATABASE_CLICKHOUSE_PASSWORD")

    database_sqlite_path: str = Field(..., alias="DATABASE_SQLITE_PATH")

    parser_log_directory: str = Field(..., alias="PARSER_LOG_DIRECTORY")
    parser_batch_size: int = Field(..., alias="PARSER_BATCH_SIZE")
    parser_max_processes: int = Field(..., alias="PARSER_MAX_PROCESSES")
    parser_log_file: Optional[str] = Field(None, alias="PARSER_LOG_FILE")

    server_host: str = Field(..., alias="SERVER_HOST")
    server_port: int = Field(..., alias="SERVER_PORT")
    server_reload: bool = Field(..., alias="SERVER_RELOAD")

    logging_level: str = Field(..., alias="LOGGING_LEVEL")
    logging_file: str = Field(..., alias="LOGGING_FILE")

    # Метод для сбора плоских полей в удобную структуру
    @property
    def database(self) -> DatabaseSettings:
        return DatabaseSettings(
            clickhouse=ClickHouseSettings(
                host=self.database_clickhouse_host,
                port=self.database_clickhouse_port,
                database=self.database_clickhouse_database,
                user=self.database_clickhouse_user,
                password=self.database_clickhouse_password,
            ),
            sqlite_path=self.database_sqlite_path,
        )

    @property
    def parser(self) -> ParserSettings:
        return ParserSettings(
            log_directory=self.parser_log_directory,
            batch_size=self.parser_batch_size,
            max_processes=self.parser_max_processes,
            log_file=self.parser_log_file,
        )

    @property
    def server(self) -> ServerSettings:
        return ServerSettings(
            host=self.server_host,
            port=self.server_port,
            reload=self.server_reload,
        )

    @property
    def logging(self) -> LoggingSettings:
        return LoggingSettings(
            level=self.logging_level,
            file=self.logging_file,
        )


# Глобальный экземпляр
settings = Settings()