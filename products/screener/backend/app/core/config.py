"""
Configuration management for YodaBuffett Screener
"""
from functools import lru_cache
from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings"""
    
    # App configuration
    DEBUG: bool = Field(default=False, description="Debug mode")
    ENVIRONMENT: str = Field(default="development", description="Application environment")
    LOG_LEVEL: str = Field(default="INFO", description="Logging level")
    API_V1_STR: str = Field(default="/api/v1", description="API version prefix")
    PROJECT_NAME: str = Field(default="YodaBuffett Screener Pro", description="Project name")
    
    # Database configuration (leveraging existing YodaBuffett DB)
    DATABASE_URL: str = Field(
        default="postgresql://yodabuffett:password@localhost:5432/yodabuffett",
        description="Database connection URL"
    )
    DATABASE_POOL_SIZE: int = Field(default=20, description="Database connection pool size")
    DATABASE_MAX_OVERFLOW: int = Field(default=30, description="Database max overflow connections")
    
    # Redis configuration (leveraging existing Redis)
    REDIS_URL: str = Field(
        default="redis://localhost:6379/1", 
        description="Redis connection URL (using database 1 for screener cache)"
    )
    CACHE_TTL_SECONDS: int = Field(default=3600, description="Cache TTL in seconds (1 hour)")
    
    # Security
    SECRET_KEY: str = Field(
        default="your-super-secret-key-change-in-production",
        description="Secret key for JWT tokens"
    )
    ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(default=30, description="Access token expiry")
    
    # CORS
    ALLOWED_ORIGINS: List[str] = Field(
        default=["http://localhost:3000", "http://localhost:5173"],
        description="Allowed CORS origins"
    )
    
    # Performance settings
    MAX_QUERY_EXECUTION_TIME: int = Field(
        default=30, 
        description="Maximum query execution time in seconds"
    )
    MAX_BACKTEST_DAYS: int = Field(
        default=1095,  # 3 years
        description="Maximum backtest period in days"
    )
    MAX_BACKTEST_COMPANIES: int = Field(
        default=1000,
        description="Maximum companies to include in backtest"
    )
    
    # Caching configuration
    ENABLE_RESULT_CACHING: bool = Field(
        default=True,
        description="Enable caching of screening results"
    )
    CACHE_RESULTS_TTL: int = Field(
        default=1800,  # 30 minutes
        description="TTL for cached screening results"
    )
    
    # Monitoring
    ENABLE_METRICS: bool = Field(default=True, description="Enable Prometheus metrics")
    METRICS_PORT: int = Field(default=8001, description="Metrics server port")
    
    # Rate limiting
    RATE_LIMIT_PER_MINUTE: int = Field(default=60, description="API rate limit per minute")
    RATE_LIMIT_BURST: int = Field(default=10, description="Rate limit burst allowance")
    
    # Forward returns calculation
    DEFAULT_FORWARD_PERIODS: List[str] = Field(
        default=["1M", "3M", "1Y"],
        description="Default forward return periods"
    )
    
    # Query complexity limits
    MAX_QUERY_CONDITIONS: int = Field(
        default=20,
        description="Maximum conditions per query"
    )
    MAX_QUERY_GROUPS: int = Field(
        default=5,
        description="Maximum groups per query"
    )
    
    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"  # Ignore extra fields in .env file


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()