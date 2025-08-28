"""
Shared monitoring and observability for YodaBuffett backend
"""
import logging
import time
from typing import Optional
from prometheus_client import Counter, Histogram, Gauge, generate_latest
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from .config import settings

# Prometheus metrics
request_count = Counter(
    'http_requests_total',
    'Total HTTP requests',
    ['method', 'endpoint', 'status_code', 'service']
)

request_duration = Histogram(
    'http_request_duration_seconds',
    'HTTP request duration in seconds',
    ['method', 'endpoint', 'service']
)

active_connections = Gauge(
    'active_connections',
    'Number of active connections',
    ['service']
)

# Nordic Ingestion specific metrics
documents_processed = Counter(
    'documents_processed_total',
    'Total documents processed',
    ['company', 'document_type', 'status']
)

collection_attempts = Counter(
    'collection_attempts_total',
    'Total collection attempts',
    ['method', 'status', 'company']
)

manual_tasks_created = Counter(
    'manual_tasks_created_total',
    'Total manual collection tasks created',
    ['company', 'reason']
)


class MonitoringMiddleware(BaseHTTPMiddleware):
    """Middleware to collect HTTP metrics"""
    
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        
        # Determine service from path
        service = "unknown"
        if request.url.path.startswith("/api/v1/research"):
            service = "research"
        elif request.url.path.startswith("/api/v1/nordic"):
            service = "nordic_ingestion"
        elif request.url.path in ["/health", "/metrics", "/docs", "/redoc", "/"]:
            service = "system"
        
        try:
            response = await call_next(request)
            
            # Record metrics
            duration = time.time() - start_time
            
            request_count.labels(
                method=request.method,
                endpoint=request.url.path,
                status_code=response.status_code,
                service=service
            ).inc()
            
            request_duration.labels(
                method=request.method,
                endpoint=request.url.path,
                service=service
            ).observe(duration)
            
            return response
            
        except Exception as e:
            # Record error metrics
            request_count.labels(
                method=request.method,
                endpoint=request.url.path,
                status_code=500,
                service=service
            ).inc()
            
            logging.error(f"Request failed: {e}", exc_info=True)
            raise


async def setup_monitoring():
    """Initialize monitoring and metrics collection"""
    if not settings.enable_metrics:
        logging.info("Metrics collection disabled")
        return
    
    logging.info("✅ Monitoring and metrics collection enabled")


def record_document_processed(company: str, document_type: str, status: str):
    """Record document processing metrics"""
    documents_processed.labels(
        company=company,
        document_type=document_type,
        status=status
    ).inc()


def record_collection_attempt(method: str, status: str, company: str):
    """Record collection attempt metrics"""
    collection_attempts.labels(
        method=method,
        status=status,
        company=company
    ).inc()


def record_manual_task_created(company: str, reason: str):
    """Record manual task creation metrics"""
    manual_tasks_created.labels(
        company=company,
        reason=reason
    ).inc()


def get_metrics():
    """Get Prometheus metrics"""
    return generate_latest()