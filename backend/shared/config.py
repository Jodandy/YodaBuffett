"""
Shared configuration for YodaBuffett backend services
"""
import os
from functools import lru_cache
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Application settings"""
    
    # Application
    app_name: str = "YodaBuffett Backend"
    debug: bool = Field(default=False, env="DEBUG")
    environment: str = Field(default="development", env="ENVIRONMENT")
    
    # Database
    database_url: str = Field(
        default="postgresql://user:password@localhost:5432/yodabuffett",
        env="DATABASE_URL"
    )
    
    # Storage
    storage_type: str = Field(default="local", env="STORAGE_TYPE")  # "local" or "s3"
    s3_bucket: str = Field(default="yodabuffett-documents", env="S3_BUCKET")
    s3_region: str = Field(default="us-east-1", env="S3_REGION")
    local_storage_path: str = Field(default="./data/documents", env="LOCAL_STORAGE_PATH")
    
    # Cache
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        env="REDIS_URL"
    )
    
    # External APIs
    openai_api_key: str = Field(default="", env="OPENAI_API_KEY")
    github_token: str = Field(default="", env="GITHUB_TOKEN")
    github_repo: str = Field(default="YodaBuffett/YodaBuffett", env="GITHUB_REPO")
    
    # Email (for Nordic ingestion)
    ir_email: str = Field(default="", env="IR_EMAIL")
    ir_email_password: str = Field(default="", env="IR_EMAIL_PASSWORD")
    
    # Monitoring
    enable_metrics: bool = Field(default=True, env="ENABLE_METRICS")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    
    # Nordic Ingestion specific
    max_concurrent_downloads: int = Field(default=5, env="MAX_CONCURRENT_DOWNLOADS")
    download_timeout_seconds: int = Field(default=30, env="DOWNLOAD_TIMEOUT_SECONDS")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()


# Global settings instance
settings = get_settings()