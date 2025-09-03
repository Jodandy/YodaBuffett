"""
Research Service Configuration
"""
import os
from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Service settings
    service_name: str = "research-service"
    service_version: str = "0.1.0"
    debug: bool = False
    
    # API settings
    api_host: str = "0.0.0.0"
    api_port: int = 8002
    api_prefix: str = "/api/v1/research"
    
    # Database
    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://user:password@localhost/yodabuffett"
    )
    
    # AI/LLM settings
    openai_api_key: Optional[str] = os.getenv("OPENAI_API_KEY")
    anthropic_api_key: Optional[str] = os.getenv("ANTHROPIC_API_KEY")
    default_llm_provider: str = "openai"
    default_llm_model: str = "gpt-4o-mini"
    embedding_model: str = "text-embedding-3-small"
    
    # Processing settings
    max_chunk_size: int = 8000  # tokens
    chunk_overlap: int = 200    # tokens
    max_pdf_size_mb: int = 50
    
    # Vector database
    vector_db_type: str = "pgvector"  # pgvector, pinecone, weaviate
    vector_dimension: int = 1536      # OpenAI embedding dimension
    
    # Caching
    redis_url: Optional[str] = os.getenv("REDIS_URL", "redis://localhost:6379")
    cache_ttl_seconds: int = 3600  # 1 hour
    
    # Analysis settings
    analysis_timeout_seconds: int = 300  # 5 minutes
    max_concurrent_analyses: int = 5
    
    # Cost controls
    max_tokens_per_request: int = 100000
    max_cost_per_analysis: float = 1.0  # USD
    
    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()