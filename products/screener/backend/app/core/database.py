"""
Database configuration and connection management
Integrates with existing YodaBuffett database infrastructure
"""
import logging
from typing import AsyncGenerator

import asyncpg
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base

from app.core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# SQLAlchemy setup
engine = create_async_engine(
    settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://"),
    pool_size=settings.DATABASE_POOL_SIZE,
    max_overflow=settings.DATABASE_MAX_OVERFLOW,
    echo=settings.DEBUG,
    future=True
)

AsyncSessionLocal = async_sessionmaker(
    engine, 
    class_=AsyncSession, 
    expire_on_commit=False
)

Base = declarative_base()


async def init_db() -> None:
    """Initialize database connections and verify schema"""
    try:
        async with engine.begin() as conn:
            # Test connection
            await conn.execute(text("SELECT 1"))
            logger.info("Database connection successful")
            
            # Verify screener tables exist
            result = await conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('screener_queries', 'metric_definitions', 'screener_results')
            """))
            tables = [row[0] for row in result.fetchall()]
            
            if len(tables) < 3:
                logger.warning(
                    f"Some screener tables missing: {set(['screener_queries', 'metric_definitions', 'screener_results']) - set(tables)}"
                )
                logger.warning("Run migrations to create missing tables")
            else:
                logger.info("All screener tables present")
                
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise


async def close_db() -> None:
    """Close database connections"""
    await engine.dispose()
    logger.info("Database connections closed")


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Get database session dependency"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


class DatabaseManager:
    """Direct database connection manager for complex queries"""
    
    def __init__(self):
        self._pool = None
    
    async def init_pool(self) -> None:
        """Initialize connection pool"""
        if self._pool is None:
            # Parse database URL for asyncpg
            db_url = settings.DATABASE_URL
            if db_url.startswith("postgresql://"):
                db_url = db_url.replace("postgresql://", "")
            
            # Extract components
            if "@" in db_url:
                auth_part, host_part = db_url.split("@", 1)
                if ":" in auth_part:
                    user, password = auth_part.split(":", 1)
                else:
                    user, password = auth_part, ""
            else:
                user, password = "", ""
                host_part = db_url
            
            if "/" in host_part:
                host_port, database = host_part.split("/", 1)
            else:
                host_port, database = host_part, "yodabuffett"
            
            if ":" in host_port:
                host, port = host_port.split(":", 1)
                port = int(port)
            else:
                host, port = host_port, 5432
                
            self._pool = await asyncpg.create_pool(
                user=user,
                password=password,
                host=host,
                port=port,
                database=database,
                min_size=5,
                max_size=settings.DATABASE_POOL_SIZE
            )
            logger.info("Direct database pool initialized")
    
    async def close_pool(self) -> None:
        """Close connection pool"""
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("Direct database pool closed")
    
    async def execute_query(self, query: str, *args) -> list:
        """Execute a query and return results"""
        if not self._pool:
            await self.init_pool()
        
        async with self._pool.acquire() as connection:
            return await connection.fetch(query, *args)
    
    async def execute_query_one(self, query: str, *args):
        """Execute a query and return one result"""
        if not self._pool:
            await self.init_pool()
        
        async with self._pool.acquire() as connection:
            return await connection.fetchrow(query, *args)


# Global database manager instance
db_manager = DatabaseManager()


async def get_db_manager() -> DatabaseManager:
    """Get database manager dependency"""
    if not db_manager._pool:
        await db_manager.init_pool()
    return db_manager


async def check_database_health() -> dict:
    """Check database connectivity and data availability for screener"""
    try:
        manager = await get_db_manager()
        
        # Basic connectivity test
        result = await manager.execute_query_one("SELECT NOW() as current_time, version() as version")
        
        # Check key tables exist and have data
        tables_to_check = [
            'company_master',
            'historical_fundamentals_daily', 
            'daily_price_data',
            'financial_statements',
            'balance_sheet_data'
        ]
        
        data_availability = {}
        
        for table in tables_to_check:
            try:
                # Check if table exists and get row count
                count_result = await manager.execute_query_one(f"SELECT COUNT(*) as count FROM {table}")
                row_count = count_result['count'] if count_result else 0
                
                # For time-series tables, get date range
                if table in ['historical_fundamentals_daily', 'daily_price_data']:
                    date_result = await manager.execute_query_one(f"""
                        SELECT 
                            MIN(date) as min_date,
                            MAX(date) as max_date,
                            COUNT(DISTINCT symbol) as symbol_count
                        FROM {table}
                    """)
                    
                    data_availability[table] = {
                        'exists': True,
                        'row_count': row_count,
                        'min_date': date_result['min_date'] if date_result else None,
                        'max_date': date_result['max_date'] if date_result else None,
                        'symbol_count': date_result['symbol_count'] if date_result else 0
                    }
                else:
                    data_availability[table] = {
                        'exists': True,
                        'row_count': row_count
                    }
                    
            except Exception as e:
                logger.warning(f"Table {table} not available: {e}")
                data_availability[table] = {
                    'exists': False,
                    'error': str(e)
                }
        
        return {
            'status': 'healthy',
            'connected_at': result['current_time'] if result else None,
            'database_version': result['version'] if result else None,
            'data_availability': data_availability
        }
        
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return {
            'status': 'unhealthy',
            'error': str(e)
        }