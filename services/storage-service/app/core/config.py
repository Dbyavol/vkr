from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Storage Service"
    app_env: str = "dev"
    debug: bool = True
    api_prefix: str = "/api/v1"

    database_url: str = "sqlite:///./storage.db"

    s3_endpoint_url: str | None = None
    s3_region: str = "us-east-1"
    s3_access_key_id: str = "minioadmin"
    s3_secret_access_key: str = "minioadmin"
    s3_bucket_name: str = "uploads"
    local_storage_dir: str = "./local_storage"

    cors_origins: str = "*"

    model_config = SettingsConfigDict(env_file=".env", env_prefix="STORAGE_", extra="ignore")

    @property
    def cors_origin_list(self) -> list[str]:
        if self.cors_origins.strip() == "*":
            return ["*"]
        return [item.strip() for item in self.cors_origins.split(",") if item.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
