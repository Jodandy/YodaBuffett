"""
YodaBuffett Screener Pro - Main FastAPI Application

Professional stock screener with point-in-time backtesting capabilities,
built on the YodaBuffett financial intelligence platform.
"""

import logging
import time
from contextlib import asynccontextmanager
from typing import Dict, Any, AsyncGenerator

from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
import asyncpg
import uvicorn

from app.core.config import get_settings
from app.core.database import init_db, close_db, db_manager, check_database_health
from app.api.v1 import screener, backtest, metrics

# Configure logging
settings = get_settings()
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan manager"""
    # Startup
    logger.info("🚀 Starting YodaBuffett Screener Pro...")
    
    try:
        # Initialize database connections
        await init_db()
        await db_manager.init_pool()
        
        # Check data availability and log summary
        health = await check_database_health()
        if health['status'] == 'healthy':
            logger.info("✅ Database health check passed")
            logger.info(f"📊 Connected to: {health.get('database_version', 'Unknown version')}")
            
            # Log data availability summary
            data_availability = health.get('data_availability', {})
            total_companies = 0
            total_fundamentals = 0
            total_prices = 0
            
            for table, info in data_availability.items():
                if info.get('exists'):
                    row_count = info.get('row_count', 0)
                    logger.info(f"📈 Table {table}: {row_count:,} rows")
                    
                    # Track key metrics
                    if table == 'company_master':
                        total_companies = row_count
                    elif table == 'historical_fundamentals_daily':
                        total_fundamentals = row_count
                    elif table == 'daily_price_data':
                        total_prices = row_count
                else:
                    logger.warning(f"❌ Table {table}: Not available - {info.get('error', 'Unknown error')}")
            
            logger.info(f"🎯 Screener ready with {total_companies:,} companies, {total_fundamentals:,} fundamental records, {total_prices:,} price points")
        else:
            logger.error(f"❌ Database health check failed: {health.get('error')}")
            raise Exception("Database not healthy")
        
        logger.info("🎉 YodaBuffett Screener Pro started successfully!")
        
    except Exception as e:
        logger.error(f"💥 Startup failed: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("🛑 Shutting down YodaBuffett Screener Pro...")
    await close_db()
    await db_manager.close_pool()
    logger.info("👋 Shutdown complete")


# Create FastAPI application
app = FastAPI(
    title=settings.PROJECT_NAME,
    version="1.0.0",
    description="Professional stock screener with point-in-time backtesting capabilities",
    lifespan=lifespan,
    docs_url="/docs" if settings.DEBUG else None,
    redoc_url="/redoc" if settings.DEBUG else None,
    openapi_url="/openapi.json" if settings.DEBUG else None
)

# Add compression middleware
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Add trusted host middleware for production security
if settings.ENVIRONMENT == "production":
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=["*"]  # Configure appropriately for production
    )


# Request logging and timing middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all requests with timing"""
    start_time = time.time()
    
    # Log request
    logger.debug(f"📥 Request: {request.method} {request.url.path}")
    
    try:
        response = await call_next(request)
        
        # Log response
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = str(process_time)
        
        if process_time > 5.0:  # Log slow requests
            logger.warning(
                f"🐌 Slow response: {response.status_code} for {request.method} {request.url.path} "
                f"in {process_time:.3f}s"
            )
        else:
            logger.debug(
                f"📤 Response: {response.status_code} for {request.method} {request.url.path} "
                f"in {process_time:.3f}s"
            )
        
        return response
        
    except Exception as e:
        process_time = time.time() - start_time
        logger.error(f"💥 Request failed: {request.method} {request.url.path} in {process_time:.3f}s - {str(e)}")
        raise


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler with detailed logging"""
    if isinstance(exc, HTTPException):
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": exc.detail,
                "status_code": exc.status_code,
                "request_path": str(request.url.path)
            }
        )
    
    # Database connection errors
    if isinstance(exc, (asyncpg.PostgresError, ConnectionError)):
        logger.error(f"🔌 Database error: {str(exc)}")
        return JSONResponse(
            status_code=503,
            content={
                "error": "Database temporarily unavailable",
                "status_code": 503,
                "request_path": str(request.url.path)
            }
        )
    
    # Generic server errors
    logger.exception(f"💥 Unhandled exception for {request.method} {request.url.path}: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "status_code": 500,
            "request_path": str(request.url.path)
        }
    )


# Health check endpoints
@app.get("/health")
async def health_check():
    """Basic health check"""
    return {"status": "healthy", "service": "YodaBuffett Screener Pro"}


@app.get("/health/detailed")
async def detailed_health_check():
    """Detailed health check including database connectivity"""
    try:
        db_health = await check_database_health()
        return {
            "status": "healthy" if db_health['status'] == 'healthy' else "unhealthy",
            "service": "YodaBuffett Screener Pro",
            "version": "1.0.0",
            "environment": settings.ENVIRONMENT,
            "database": db_health,
            "features": {
                "screening": True,
                "backtesting": True,
                "point_in_time": True,
                "forward_returns": True,
                "complex_queries": True
            }
        }
    except Exception as e:
        logger.exception("Health check failed")
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "service": "YodaBuffett Screener Pro",
                "error": str(e)
            }
        )


# API version info
@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "service": settings.PROJECT_NAME,
        "version": "1.0.0",
        "description": "Professional stock screener with point-in-time backtesting capabilities",
        "features": [
            "Complex query building with AND/OR logic",
            "Point-in-time screening without look-ahead bias", 
            "Historical backtesting with performance metrics",
            "Forward return calculation (1W/1M/3M/6M/1Y/2Y)",
            "Nordic market coverage (787 companies)",
            "325,400+ fundamental data points"
        ],
        "endpoints": {
            "docs": "/docs" if settings.DEBUG else "Contact support",
            "health": "/health",
            "detailed_health": "/health/detailed"
        }
    }


# Include API routers with comprehensive error handling
app.include_router(
    screener.router,
    prefix=f"{settings.API_V1_STR}/screener",
    tags=["Screener"],
    responses={
        400: {"description": "Bad Request - Invalid query parameters"},
        422: {"description": "Validation Error"},
        500: {"description": "Internal Server Error"},
        503: {"description": "Service Unavailable"}
    }
)

app.include_router(
    backtest.router,
    prefix=f"{settings.API_V1_STR}/backtest",
    tags=["Backtest"],
    responses={
        400: {"description": "Bad Request - Invalid backtest parameters"},
        422: {"description": "Validation Error"},
        500: {"description": "Internal Server Error"},
        503: {"description": "Service Unavailable"}
    }
)

app.include_router(
    metrics.router,
    prefix=f"{settings.API_V1_STR}/metrics",
    tags=["Metrics"],
    responses={
        404: {"description": "Metric Not Found"},
        500: {"description": "Internal Server Error"}
    }
)


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
        log_level=settings.LOG_LEVEL.lower(),
        access_log=True,
        workers=1 if settings.DEBUG else 4
    )