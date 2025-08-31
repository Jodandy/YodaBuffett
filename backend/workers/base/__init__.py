"""
Base Worker Classes for YodaBuffett Multi-Market System

This package provides the foundation for all specialized workers:
- BaseWorker: Common functionality for all workers
- DocumentIngestor: Base for document collection workers
- EventMonitor: Base for calendar/event monitoring
- MarketDataWorker: Base for price and market data
- MaintenanceWorker: Base for housekeeping tasks
"""

from .base_worker import BaseWorker, WorkerStatus, WorkerType
from .document_ingestor import DocumentIngestor, DocumentSource

__all__ = [
    'BaseWorker',
    'WorkerStatus', 
    'WorkerType',
    'DocumentIngestor',
    'DocumentSource'
]