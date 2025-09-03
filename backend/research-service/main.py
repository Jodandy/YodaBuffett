"""
Research Service Main Application
"""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from .config import settings
from .api.router import router

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    # Startup
    logger.info(f"Starting {settings.service_name} v{settings.service_version}")
    
    # Initialize services if needed
    # e.g., connect to vector DB, warm up models, etc.
    
    yield
    
    # Shutdown
    logger.info("Shutting down research service")


# Create FastAPI app
app = FastAPI(
    title="YodaBuffett Research Service",
    description="AI-powered company research and analysis",
    version=settings.service_version,
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API router
app.include_router(router)

# Root endpoint
@app.get("/")
async def root():
    return {
        "service": settings.service_name,
        "version": settings.service_version,
        "status": "running"
    }


def main():
    """Run the service"""
    uvicorn.run(
        "research-service.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug
    )


if __name__ == "__main__":
    main()