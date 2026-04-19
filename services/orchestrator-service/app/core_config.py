from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Orchestrator Service"
    debug: bool = True
    cors_origins: str = "http://localhost:5173,http://127.0.0.1:5173"

    import_service_url: str = "http://localhost:8060"
    preprocessing_service_url: str = "http://localhost:8090"
    analysis_service_url: str = "http://localhost:8080"
    storage_service_url: str = "http://localhost:8070"
    auth_service_url: str = "http://localhost:8040"
    request_timeout_seconds: float = 60.0

    model_config = SettingsConfigDict(env_file=".env", env_prefix="ORCHESTRATOR_", extra="ignore")

    @property
    def cors_origin_list(self) -> list[str]:
        return [item.strip() for item in self.cors_origins.split(",") if item.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
