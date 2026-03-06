"""Processor configuration via environment variables."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redis_url: str = "redis://redis:6379"
    qdrant_url: str = "http://localhost:6333"
    trilium_url: str = "http://localhost:8080"
    trilium_etapi_token: str = ""
    voyage_api_key: str = ""
    deepseek_api_key: str = ""
    pdf_cache_dir: str = "/app/data/pdfs"
    collection_name: str = "papers"

    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"


settings = Settings()
