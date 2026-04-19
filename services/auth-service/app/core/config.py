from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Auth Service"
    debug: bool = True
    database_url: str = "sqlite:///./auth.db"
    cors_origins: str = "http://localhost:5173,http://127.0.0.1:5173"
    jwt_secret: str = "change-me-in-production"
    jwt_expires_seconds: int = 86400
    bootstrap_admin_email: str = "admin@example.com"
    bootstrap_admin_password: str = "admin12345"

    model_config = SettingsConfigDict(env_file=".env", env_prefix="AUTH_", extra="ignore")

    @property
    def cors_origin_list(self) -> list[str]:
        return [item.strip() for item in self.cors_origins.split(",") if item.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
