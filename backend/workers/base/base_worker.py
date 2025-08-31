#!/usr/bin/env python3
"""
Base Worker Class - Foundation for All YodaBuffett Workers

Provides common functionality for all specialized workers:
- Configuration management
- Health monitoring
- Progress tracking
- Error handling
- Graceful shutdown
- Logging and metrics
"""

import asyncio
import signal
import sys
import os
import json
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, date
from enum import Enum
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
import logging
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from workers.worker_config import get_config, setup_worker_logging

class WorkerStatus(Enum):
    """Worker execution status"""
    IDLE = "idle"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"
    MAINTENANCE = "maintenance"

class WorkerType(Enum):
    """Types of workers in the system"""
    DOCUMENT_INGESTOR = "document_ingestor"
    EVENT_MONITOR = "event_monitor"
    MARKET_DATA = "market_data"
    NEWS_AGGREGATOR = "news_aggregator"
    MAINTENANCE = "maintenance"
    ANALYTICS = "analytics"

@dataclass
class WorkerMetrics:
    """Common metrics tracked by all workers"""
    start_time: datetime
    end_time: Optional[datetime] = None
    items_processed: int = 0
    items_succeeded: int = 0
    items_failed: int = 0
    errors_encountered: int = 0
    warnings_raised: int = 0
    last_heartbeat: Optional[datetime] = None
    custom_metrics: Dict[str, Any] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary"""
        data = asdict(self)
        # Convert datetime objects
        if data['start_time']:
            data['start_time'] = data['start_time'].isoformat()
        if data['end_time']:
            data['end_time'] = data['end_time'].isoformat()
        if data['last_heartbeat']:
            data['last_heartbeat'] = data['last_heartbeat'].isoformat()
        return data

class BaseWorker(ABC):
    """
    Abstract base class for all YodaBuffett workers
    
    Provides common functionality that all workers share:
    - Configuration and logging
    - Status and health monitoring
    - Progress tracking and persistence
    - Error handling and recovery
    - Graceful shutdown
    """
    
    def __init__(self, 
                 worker_name: str,
                 worker_type: WorkerType,
                 market: Optional[str] = None):
        """
        Initialize base worker
        
        Args:
            worker_name: Unique name for this worker instance
            worker_type: Type of worker from WorkerType enum
            market: Market this worker operates on (e.g., 'swedish', 'norwegian')
        """
        self.worker_name = worker_name
        self.worker_type = worker_type
        self.market = market
        self.worker_id = str(uuid.uuid4())
        
        # Configuration and logging
        self.config = get_config()
        self.logger = setup_worker_logging()
        
        # Status tracking
        self.status = WorkerStatus.IDLE
        self.metrics = WorkerMetrics(start_time=datetime.now())
        self.should_stop = False
        
        # Progress tracking
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.progress_file = self._get_progress_file_path()
        self.progress_data = self._initialize_progress_data()
        
        # Setup signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        self.logger.info(f"🚀 Initialized {self.worker_name} [{self.worker_type.value}]")
        if self.market:
            self.logger.info(f"   Market: {self.market}")
        self.logger.info(f"   Worker ID: {self.worker_id}")
        self.logger.info(f"   Session ID: {self.session_id}")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"📡 Received signal {signum}, initiating graceful shutdown...")
        self.should_stop = True
        self.status = WorkerStatus.STOPPING
    
    def _get_progress_file_path(self) -> Path:
        """Generate path for progress tracking file"""
        data_dir = Path(self.config.data_volume_path)
        data_dir.mkdir(parents=True, exist_ok=True)
        
        # Organize by worker type and market
        if self.market:
            progress_dir = data_dir / self.worker_type.value / self.market
        else:
            progress_dir = data_dir / self.worker_type.value / "global"
        
        progress_dir.mkdir(parents=True, exist_ok=True)
        return progress_dir / f"{self.worker_name}_{self.session_id}.json"
    
    def _initialize_progress_data(self) -> Dict[str, Any]:
        """Initialize progress tracking data structure"""
        return {
            "worker_id": self.worker_id,
            "worker_name": self.worker_name,
            "worker_type": self.worker_type.value,
            "market": self.market,
            "session_id": self.session_id,
            "start_time": datetime.now().isoformat(),
            "status": self.status.value,
            "config": {
                "mode": self.config.mode.value,
                "log_level": self.config.log_level.value
            },
            "metrics": self.metrics.to_dict(),
            "checkpoints": [],
            "errors": []
        }
    
    async def save_progress(self):
        """Save current progress to file"""
        try:
            self.progress_data["status"] = self.status.value
            self.progress_data["metrics"] = self.metrics.to_dict()
            self.progress_data["last_updated"] = datetime.now().isoformat()
            
            with open(self.progress_file, 'w') as f:
                json.dump(self.progress_data, f, indent=2, default=str)
                
        except Exception as e:
            self.logger.error(f"⚠️  Failed to save progress: {e}")
    
    async def add_checkpoint(self, checkpoint_name: str, data: Dict[str, Any] = None):
        """Add a checkpoint to track progress"""
        checkpoint = {
            "name": checkpoint_name,
            "timestamp": datetime.now().isoformat(),
            "data": data or {}
        }
        self.progress_data["checkpoints"].append(checkpoint)
        await self.save_progress()
    
    async def record_error(self, error_type: str, error_message: str, context: Dict[str, Any] = None):
        """Record an error for debugging"""
        error_record = {
            "type": error_type,
            "message": error_message,
            "timestamp": datetime.now().isoformat(),
            "context": context or {}
        }
        self.progress_data["errors"].append(error_record)
        self.metrics.errors_encountered += 1
        await self.save_progress()
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check and return status"""
        self.metrics.last_heartbeat = datetime.now()
        
        health_status = {
            "healthy": self.status not in [WorkerStatus.ERROR, WorkerStatus.STOPPED],
            "worker_id": self.worker_id,
            "worker_name": self.worker_name,
            "worker_type": self.worker_type.value,
            "market": self.market,
            "status": self.status.value,
            "uptime_seconds": (datetime.now() - self.metrics.start_time).total_seconds(),
            "metrics": {
                "items_processed": self.metrics.items_processed,
                "success_rate": (
                    self.metrics.items_succeeded / max(self.metrics.items_processed, 1) * 100
                ),
                "errors": self.metrics.errors_encountered
            },
            "last_heartbeat": self.metrics.last_heartbeat.isoformat()
        }
        
        return health_status
    
    async def start(self):
        """Start the worker"""
        self.logger.info(f"▶️  Starting {self.worker_name}...")
        self.status = WorkerStatus.STARTING
        
        try:
            # Perform startup tasks
            await self.on_startup()
            
            # Update status
            self.status = WorkerStatus.RUNNING
            await self.add_checkpoint("worker_started")
            
            # Run main work
            result = await self.run()
            
            # Successful completion
            self.status = WorkerStatus.IDLE
            self.metrics.end_time = datetime.now()
            await self.add_checkpoint("worker_completed", {"result": result})
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Worker failed: {e}")
            self.status = WorkerStatus.ERROR
            await self.record_error("worker_failure", str(e))
            raise
            
        finally:
            # Cleanup
            await self.on_shutdown()
            await self.save_progress()
    
    @abstractmethod
    async def on_startup(self):
        """Perform startup tasks - override in subclasses"""
        pass
    
    @abstractmethod
    async def run(self) -> Dict[str, Any]:
        """Main worker logic - must be implemented by subclasses"""
        pass
    
    @abstractmethod
    async def on_shutdown(self):
        """Perform cleanup tasks - override in subclasses"""
        pass
    
    def update_metrics(self, 
                      processed: int = 0, 
                      succeeded: int = 0, 
                      failed: int = 0,
                      custom_metrics: Dict[str, Any] = None):
        """Update worker metrics"""
        self.metrics.items_processed += processed
        self.metrics.items_succeeded += succeeded
        self.metrics.items_failed += failed
        
        if custom_metrics:
            if self.metrics.custom_metrics is None:
                self.metrics.custom_metrics = {}
            self.metrics.custom_metrics.update(custom_metrics)
    
    def log_summary(self):
        """Log execution summary"""
        duration = (self.metrics.end_time or datetime.now()) - self.metrics.start_time
        
        self.logger.info(f"📊 {self.worker_name} Execution Summary:")
        self.logger.info(f"   Status: {self.status.value}")
        self.logger.info(f"   Duration: {duration.total_seconds():.1f}s")
        self.logger.info(f"   Items Processed: {self.metrics.items_processed}")
        self.logger.info(f"   Success Rate: {self.metrics.items_succeeded}/{self.metrics.items_processed}")
        self.logger.info(f"   Errors: {self.metrics.errors_encountered}")
        
        if self.metrics.custom_metrics:
            self.logger.info(f"   Custom Metrics: {self.metrics.custom_metrics}")

# Example usage for testing
if __name__ == "__main__":
    class TestWorker(BaseWorker):
        async def on_startup(self):
            self.logger.info("Test worker starting up...")
            
        async def run(self) -> Dict[str, Any]:
            self.logger.info("Test worker running...")
            await asyncio.sleep(2)
            self.update_metrics(processed=10, succeeded=9, failed=1)
            return {"status": "success", "test": True}
            
        async def on_shutdown(self):
            self.logger.info("Test worker shutting down...")
    
    async def test():
        worker = TestWorker("test-worker", WorkerType.MAINTENANCE)
        result = await worker.start()
        worker.log_summary()
        print(f"Result: {result}")
    
    asyncio.run(test())