"""
YodaBuffett Backend - Main FastAPI Application
Modular monolith serving both Research and Nordic Ingestion services
"""
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Import service routers
from research.api.router import router as research_router
from nordic_ingestion.api.router import router as nordic_router
from shared.database import init_database
from shared.monitoring import setup_monitoring
from shared.config import settings


# Lifespan events
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown events"""
    
    # Startup
    logging.info("🚀 Starting YodaBuffett Backend Services")
    
    # Initialize database
    await init_database()
    
    # Setup monitoring
    await setup_monitoring()
    
    # Log service status
    logging.info("✅ Research Service: Ready")
    logging.info("✅ Nordic Ingestion Service: Ready")
    
    yield
    
    # Shutdown
    logging.info("🛑 Shutting down YodaBuffett Backend")


# Create FastAPI app
app = FastAPI(
    title="YodaBuffett API",
    description="AI-Powered Investment Research Platform",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logging.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )

# Health check endpoint
@app.get("/health")
async def health_check():
    """Global health check for all services"""
    return {
        "status": "healthy",
        "services": {
            "research": "up",
            "nordic_ingestion": "up"
        },
        "version": "1.0.0"
    }

# Include service routers
app.include_router(
    research_router,
    prefix="/api/v1/research",
    tags=["Research Service"]
)

app.include_router(
    nordic_router, 
    prefix="/api/v1/nordic",
    tags=["Nordic Ingestion Service"]
)

# Root endpoint
@app.get("/")
async def root():
    return {
        "message": "YodaBuffett API",
        "services": [
            "Research Service - /api/v1/research",
            "Nordic Ingestion Service - /api/v1/nordic"
        ],
        "docs": "/docs"
    }


if __name__ == "__main__":
    import uvicorn
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )