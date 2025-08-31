#!/usr/bin/env python3
"""
Worker Registry - Central registration and discovery of all workers

Provides:
- Dynamic worker discovery
- Worker metadata and capabilities
- Scheduling configurations
- Dependency management
"""

from dataclasses import dataclass
from typing import Dict, List, Type, Optional, Any
from enum import Enum
from datetime import time

from workers.base.base_worker import BaseWorker, WorkerType
from workers.config.market_configs import Market

class ScheduleType(Enum):
    """Worker scheduling types"""
    CONTINUOUS = "continuous"      # Runs continuously
    DAILY = "daily"               # Runs once per day
    WEEKLY = "weekly"             # Runs once per week
    HOURLY = "hourly"             # Runs every hour
    EVENT_DRIVEN = "event_driven" # Triggered by events
    ON_DEMAND = "on_demand"       # Manual trigger only

@dataclass
class WorkerSchedule:
    """Scheduling configuration for a worker"""
    schedule_type: ScheduleType
    run_at: Optional[time] = None           # For daily schedules
    run_on_days: Optional[List[int]] = None # For weekly (0=Monday, 6=Sunday)
    interval_hours: Optional[int] = None     # For hourly schedules
    enabled: bool = True

@dataclass
class WorkerMetadata:
    """Metadata about a worker"""
    name: str
    display_name: str
    description: str
    worker_type: WorkerType
    market: Optional[Market] = None
    worker_class: Optional[Type[BaseWorker]] = None
    
    # Capabilities
    capabilities: List[str] = None
    supported_document_types: List[str] = None
    data_sources: List[str] = None
    
    # Resource requirements
    memory_mb: int = 256
    cpu_cores: float = 0.25
    
    # Scheduling
    schedule: WorkerSchedule = None
    
    # Dependencies
    depends_on: List[str] = None  # Other worker names
    conflicts_with: List[str] = None

# Worker Registry
WORKER_REGISTRY: Dict[str, WorkerMetadata] = {}

def register_worker(metadata: WorkerMetadata):
    """Register a worker in the global registry"""
    WORKER_REGISTRY[metadata.name] = metadata

# Register Document Ingestors
register_worker(WorkerMetadata(
    name="swedish-document-ingestor",
    display_name="Swedish Document Ingestor",
    description="Collects financial documents from Swedish sources (MFN.se, etc.)",
    worker_type=WorkerType.DOCUMENT_INGESTOR,
    market=Market.SWEDISH,
    capabilities=["document_collection", "calendar_events", "swedish_language"],
    supported_document_types=["annual_report", "quarterly_report", "press_release"],
    data_sources=["mfn.se", "nasdaq_stockholm", "company_rss"],
    memory_mb=512,
    cpu_cores=0.5,
    schedule=WorkerSchedule(
        schedule_type=ScheduleType.DAILY,
        run_at=time(6, 0),  # 6 AM
        enabled=True
    )
))

register_worker(WorkerMetadata(
    name="norwegian-document-ingestor",
    display_name="Norwegian Document Ingestor",
    description="Collects financial documents from Norwegian sources (Newsweb, Oslo Børs)",
    worker_type=WorkerType.DOCUMENT_INGESTOR,
    market=Market.NORWEGIAN,
    capabilities=["document_collection", "norwegian_language"],
    supported_document_types=["annual_report", "quarterly_report", "stock_exchange_notice"],
    data_sources=["newsweb", "oslo_bors"],
    memory_mb=512,
    cpu_cores=0.5,
    schedule=WorkerSchedule(
        schedule_type=ScheduleType.DAILY,
        run_at=time(6, 30),  # 6:30 AM
        enabled=True
    )
))

register_worker(WorkerMetadata(
    name="danish-document-ingestor",
    display_name="Danish Document Ingestor",
    description="Collects financial documents from Danish sources",
    worker_type=WorkerType.DOCUMENT_INGESTOR,
    market=Market.DANISH,
    capabilities=["document_collection", "danish_language"],
    supported_document_types=["annual_report", "quarterly_report"],
    data_sources=["nasdaq_copenhagen"],
    memory_mb=256,
    cpu_cores=0.25,
    schedule=WorkerSchedule(
        schedule_type=ScheduleType.WEEKLY,
        run_at=time(7, 0),
        run_on_days=[1, 4],  # Tuesday, Friday
        enabled=False  # Not yet implemented
    )
))

register_worker(WorkerMetadata(
    name="finnish-document-ingestor",
    display_name="Finnish Document Ingestor", 
    description="Collects financial documents from Finnish sources",
    worker_type=WorkerType.DOCUMENT_INGESTOR,
    market=Market.FINNISH,
    capabilities=["document_collection", "finnish_language", "swedish_language"],
    supported_document_types=["annual_report", "quarterly_report"],
    data_sources=["nasdaq_helsinki"],
    memory_mb=256,
    cpu_cores=0.25,
    schedule=WorkerSchedule(
        schedule_type=ScheduleType.WEEKLY,
        run_at=time(7, 0),
        run_on_days=[1, 4],  # Tuesday, Friday
        enabled=False  # Not yet implemented
    )
))

# Register Event Monitors
register_worker(WorkerMetadata(
    name="swedish-event-monitor",
    display_name="Swedish Calendar Event Monitor",
    description="Monitors and extracts calendar events from Swedish companies",
    worker_type=WorkerType.EVENT_MONITOR,
    market=Market.SWEDISH,
    capabilities=["calendar_extraction", "event_detection"],
    memory_mb=256,
    cpu_cores=0.25,
    schedule=WorkerSchedule(
        schedule_type=ScheduleType.DAILY,
        run_at=time(5, 30),  # Before document ingestor
        enabled=True
    )
))

register_worker(WorkerMetadata(
    name="nordic-surprise-scanner",
    display_name="Nordic Surprise News Scanner",
    description="Scans for unexpected news across all Nordic markets",
    worker_type=WorkerType.EVENT_MONITOR,
    capabilities=["surprise_detection", "cross_market"],
    memory_mb=512,
    cpu_cores=0.5,
    schedule=WorkerSchedule(
        schedule_type=ScheduleType.WEEKLY,
        run_at=time(8, 0),
        run_on_days=[6],  # Sunday
        enabled=True
    )
))

# Register Market Data Workers
register_worker(WorkerMetadata(
    name="nordic-price-collector",
    display_name="Nordic Market Price Collector",
    description="Collects real-time and historical price data from Nordic exchanges",
    worker_type=WorkerType.MARKET_DATA,
    capabilities=["price_collection", "real_time", "historical_data"],
    data_sources=["nasdaq_nordic", "oslo_bors"],
    memory_mb=1024,
    cpu_cores=1.0,
    schedule=WorkerSchedule(
        schedule_type=ScheduleType.CONTINUOUS,
        enabled=True
    )
))

register_worker(WorkerMetadata(
    name="dividend-tracker",
    display_name="Nordic Dividend Tracker",
    description="Tracks dividend announcements and payments",
    worker_type=WorkerType.MARKET_DATA,
    capabilities=["dividend_tracking", "corporate_actions"],
    memory_mb=256,
    cpu_cores=0.25,
    schedule=WorkerSchedule(
        schedule_type=ScheduleType.DAILY,
        run_at=time(9, 0),
        enabled=True
    )
))

# Register Maintenance Workers
register_worker(WorkerMetadata(
    name="database-cleanup-worker",
    display_name="Database Cleanup Worker",
    description="Removes old data and optimizes database performance",
    worker_type=WorkerType.MAINTENANCE,
    capabilities=["database_maintenance", "data_archival"],
    memory_mb=512,
    cpu_cores=0.5,
    schedule=WorkerSchedule(
        schedule_type=ScheduleType.WEEKLY,
        run_at=time(2, 0),  # 2 AM
        run_on_days=[0],     # Monday
        enabled=True
    )
))

register_worker(WorkerMetadata(
    name="data-quality-auditor",
    display_name="Data Quality Auditor",
    description="Validates data integrity and quality across the system",
    worker_type=WorkerType.MAINTENANCE,
    capabilities=["data_validation", "quality_metrics"],
    memory_mb=512,
    cpu_cores=0.5,
    schedule=WorkerSchedule(
        schedule_type=ScheduleType.DAILY,
        run_at=time(3, 0),  # 3 AM
        enabled=True
    )
))

# Worker Registry Functions
def get_worker(worker_name: str) -> Optional[WorkerMetadata]:
    """Get worker metadata by name"""
    return WORKER_REGISTRY.get(worker_name)

def get_workers_by_type(worker_type: WorkerType) -> List[WorkerMetadata]:
    """Get all workers of a specific type"""
    return [
        worker for worker in WORKER_REGISTRY.values()
        if worker.worker_type == worker_type
    ]

def get_workers_by_market(market: Market) -> List[WorkerMetadata]:
    """Get all workers for a specific market"""
    return [
        worker for worker in WORKER_REGISTRY.values()
        if worker.market == market
    ]

def get_enabled_workers() -> List[WorkerMetadata]:
    """Get all enabled workers"""
    return [
        worker for worker in WORKER_REGISTRY.values()
        if worker.schedule and worker.schedule.enabled
    ]

def get_scheduled_workers(schedule_type: ScheduleType) -> List[WorkerMetadata]:
    """Get workers with a specific schedule type"""
    return [
        worker for worker in WORKER_REGISTRY.values()
        if worker.schedule and worker.schedule.schedule_type == schedule_type
    ]

def get_worker_dependencies(worker_name: str) -> List[str]:
    """Get list of workers that a worker depends on"""
    worker = get_worker(worker_name)
    return worker.depends_on if worker and worker.depends_on else []

def get_worker_conflicts(worker_name: str) -> List[str]:
    """Get list of workers that conflict with a worker"""
    worker = get_worker(worker_name)
    return worker.conflicts_with if worker and worker.conflicts_with else []

def can_run_together(worker1: str, worker2: str) -> bool:
    """Check if two workers can run simultaneously"""
    w1 = get_worker(worker1)
    w2 = get_worker(worker2)
    
    if not w1 or not w2:
        return False
    
    # Check conflicts
    if w1.conflicts_with and worker2 in w1.conflicts_with:
        return False
    if w2.conflicts_with and worker1 in w2.conflicts_with:
        return False
    
    return True

# Worker Groups for Management
WORKER_GROUPS = {
    "ingestors": [
        "swedish-document-ingestor",
        "norwegian-document-ingestor",
        "danish-document-ingestor",
        "finnish-document-ingestor"
    ],
    "monitors": [
        "swedish-event-monitor",
        "nordic-surprise-scanner"
    ],
    "market-data": [
        "nordic-price-collector",
        "dividend-tracker"
    ],
    "maintenance": [
        "database-cleanup-worker",
        "data-quality-auditor"
    ]
}

def get_worker_group(group_name: str) -> List[str]:
    """Get list of workers in a group"""
    return WORKER_GROUPS.get(group_name, [])