from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Comparative Analysis Backend"
    app_env: str = "dev"
    debug: bool = True
    api_prefix: str = "/api/v1"
    log_level: str = "INFO"
    log_file: str = "./logs/backend.log"
    log_max_bytes: int = 5_242_880
    log_backup_count: int = 5
    database_url: str = "postgresql+psycopg://postgres:postgres@backend-db:5432/analysis"
    cors_origins: str = "http://localhost:5173,http://127.0.0.1:5173"
    jwt_secret: str = "local-dev-secret"
    jwt_expires_seconds: int = 86400
    bootstrap_admin_email: str = "admin@example.com"
    bootstrap_admin_password: str = "admin12345"
    s3_endpoint_url: str | None = None
    s3_region: str = "us-east-1"
    s3_access_key_id: str = "minioadmin"
    s3_secret_access_key: str = "minioadmin"
    s3_bucket_name: str = "uploads"
    local_storage_dir: str = "./local_storage"

    model_config = SettingsConfigDict(env_file=".env", env_prefix="BACKEND_", extra="ignore")

    @property
    def cors_origin_list(self) -> list[str]:
        if self.cors_origins.strip() == "*":
            return ["*"]
        return [item.strip() for item in self.cors_origins.split(",") if item.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
