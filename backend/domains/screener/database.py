"""
Database manager for screener domain - asyncpg-based
"""
import asyncpg
from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'

# Global connection pool
_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    """Get or create connection pool"""
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    return _pool


async def close_pool():
    """Close the connection pool"""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


class DatabaseManager:
    """Database manager wrapper for asyncpg with connection pooling"""

    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def execute_query(self, query: str, *args) -> List[Dict[str, Any]]:
        """Execute query and return all rows as dicts"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
            return [dict(row) for row in rows]

    async def execute_query_one(self, query: str, *args) -> Optional[Dict[str, Any]]:
        """Execute query and return single row as dict"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, *args)
            return dict(row) if row else None

    async def execute(self, query: str, *args) -> str:
        """Execute query without returning rows"""
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)


async def get_db_manager() -> DatabaseManager:
    """FastAPI dependency to get database manager"""
    pool = await get_pool()
    return DatabaseManager(pool)
