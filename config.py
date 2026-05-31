from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://postgres:postgres@postgres:5432/jobqueue"
    redis_url: str = "redis://redis:6379"
    worker_count: int = 2
    default_max_retries: int = 3
    retry_backoff_base: int = 60
    job_default_timeout: int = 300
    scheduler_interval: float = 1.0
    worker_liveness_interval: float = 30.0
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    log_level: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
