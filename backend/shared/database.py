"""
Shared database utilities for YodaBuffett backend
"""
import logging
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import MetaData

from .config import settings

# Database engine and session
engine = create_async_engine(
    settings.database_url.replace("postgresql://", "postgresql+asyncpg://"),
    echo=settings.debug,
    pool_size=10,
    max_overflow=20
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# Base class for all models
class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models"""
    metadata = MetaData()


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency to get database session
    Use in FastAPI endpoints like: db: AsyncSession = Depends(get_db_session)
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def init_database():
    """Initialize database - create all tables"""
    try:
        async with engine.begin() as conn:
            # Import all models to ensure they're registered
            import nordic_ingestion.models  # noqa
            try:
                import research.models  # noqa (if research models exist)
            except ImportError:
                pass  # Research models may not exist yet
            
            # Create all tables
            await conn.run_sync(Base.metadata.create_all)
            
        logging.info("✅ Database initialized successfully")
        
    except Exception as e:
        logging.error(f"❌ Database initialization failed: {e}")
        raise


async def close_database():
    """Close database connections"""
    await engine.dispose()
    logging.info("Database connections closed")