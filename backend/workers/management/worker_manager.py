#!/usr/bin/env python3
"""
YodaBuffett Worker Manager

Unified management system for all workers with:
- Worker discovery and registration
- Scheduling and orchestration
- Health monitoring
- Performance metrics
- Docker container management
- Web-based management interface
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import docker
from docker.errors import DockerException

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from workers.config.worker_registry import (
    WORKER_REGISTRY, WorkerMetadata, ScheduleType,
    get_worker, get_enabled_workers, get_workers_by_type,
    get_scheduled_workers, can_run_together
)
from workers.base.base_worker import WorkerType
from shared.database import AsyncSessionLocal

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WorkerManager:
    """
    Central worker management system
    
    Features:
    - Worker discovery and health monitoring
    - Scheduling and orchestration
    - Docker container management
    - Performance metrics collection
    - Web-based management interface
    """
    
    def __init__(self):
        self.docker_client = None
        self.running_workers = {}  # worker_name -> container_info
        self.worker_schedules = {}  # worker_name -> schedule_info
        self.metrics_cache = {}    # worker_name -> metrics
        self.last_health_check = {}  # worker_name -> timestamp
        
        # Initialize Docker client
        try:
            self.docker_client = docker.from_env()
            logger.info("✅ Docker client initialized")
        except DockerException as e:
            logger.error(f"❌ Failed to initialize Docker client: {e}")
    
    async def discover_workers(self) -> List[WorkerMetadata]:
        """Discover all registered workers"""
        return list(WORKER_REGISTRY.values())
    
    async def get_worker_status(self, worker_name: str) -> Dict[str, Any]:
        """Get current status of a worker"""
        worker = get_worker(worker_name)
        if not worker:
            return {"status": "not_found", "error": f"Worker {worker_name} not found"}
        
        status = {
            "name": worker_name,
            "display_name": worker.display_name,
            "type": worker.worker_type.value,
            "market": worker.market.value if worker.market else None,
            "enabled": worker.schedule.enabled if worker.schedule else False,
            "schedule_type": worker.schedule.schedule_type.value if worker.schedule else None,
            "container_status": "unknown",
            "health_status": "unknown",
            "last_run": None,
            "next_run": None
        }
        
        # Check Docker container status
        if self.docker_client:
            container_name = f"yodabuffett-{worker_name.replace('_', '-')}"
            try:
                container = self.docker_client.containers.get(container_name)
                status["container_status"] = container.status
                status["container_id"] = container.short_id
            except docker.errors.NotFound:
                status["container_status"] = "not_running"
            except Exception as e:
                status["container_status"] = f"error: {e}"
        
        # Check health status
        status["health_status"] = await self.check_worker_health(worker_name)
        
        # Get metrics
        status["metrics"] = self.metrics_cache.get(worker_name, {})
        
        return status
    
    async def check_worker_health(self, worker_name: str) -> str:
        """Check health of a worker"""
        try:
            # Try to connect to worker health endpoint
            import aiohttp
            
            # Map worker name to container name and port
            container_name = f"yodabuffett-{worker_name.replace('_', '-')}"
            health_url = f"http://{container_name}:8080/health"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(health_url, timeout=5) as response:
                    if response.status == 200:
                        health_data = await response.json()
                        self.last_health_check[worker_name] = datetime.now()
                        return health_data.get("status", "unknown")
                    else:
                        return f"unhealthy_http_{response.status}"
        except Exception as e:
            return f"unreachable: {str(e)}"
    
    async def start_worker(self, worker_name: str, **kwargs) -> Dict[str, Any]:
        """Start a worker container"""
        if not self.docker_client:
            return {"success": False, "error": "Docker client not available"}
        
        worker = get_worker(worker_name)
        if not worker:
            return {"success": False, "error": f"Worker {worker_name} not found"}
        
        container_name = f"yodabuffett-{worker_name.replace('_', '-')}"
        
        try:
            # Check if already running
            try:
                existing = self.docker_client.containers.get(container_name)
                if existing.status == 'running':
                    return {"success": False, "error": "Worker already running"}
                else:
                    # Remove stopped container
                    existing.remove()
            except docker.errors.NotFound:
                pass
            
            # Build environment variables
            env_vars = {
                "WORKER_TYPE": worker.worker_type.value,
                "WORKER_NAME": worker_name,
                "WORKER_MODE": "production",
                "LOG_LEVEL": "INFO",
                
                # Database configuration
                "DB_HOST": os.environ.get("DB_HOST", "postgres"),
                "DB_PORT": os.environ.get("DB_PORT", "5432"),
                "DB_NAME": os.environ.get("DB_NAME", "yodabuffett"),
                "DB_USER": os.environ.get("DB_USER", "postgres"),
                "DB_PASSWORD": os.environ.get("DB_PASSWORD", "dev_password"),
                
                # Worker-specific config
                "DATA_VOLUME_PATH": "/app/data",
                "LOG_FILE_PATH": f"/app/logs/{worker_name}.log",
                "HEALTH_CHECK_PORT": "8080"
            }
            
            # Add market-specific configuration
            if worker.market:
                env_vars["WORKER_MARKET"] = worker.market.value
            
            # Override with provided kwargs
            env_vars.update(kwargs)
            
            # Start container
            container = self.docker_client.containers.run(
                image="yodabuffett-worker",
                name=container_name,
                environment=env_vars,
                networks=["yodabuffett-network"],
                volumes={
                    "yodabuffett_worker_data": {"bind": "/app/data", "mode": "rw"},
                    "yodabuffett_worker_logs": {"bind": "/app/logs", "mode": "rw"}
                },
                restart_policy={"Name": "unless-stopped"},
                detach=True,
                remove=False
            )
            
            self.running_workers[worker_name] = {
                "container_id": container.id,
                "container_name": container_name,
                "started_at": datetime.now(),
                "environment": env_vars
            }
            
            logger.info(f"✅ Started worker {worker_name} (container: {container.short_id})")
            return {
                "success": True,
                "container_id": container.short_id,
                "message": f"Worker {worker_name} started successfully"
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to start worker {worker_name}: {e}")
            return {"success": False, "error": str(e)}
    
    async def stop_worker(self, worker_name: str) -> Dict[str, Any]:
        """Stop a worker container"""
        if not self.docker_client:
            return {"success": False, "error": "Docker client not available"}
        
        container_name = f"yodabuffett-{worker_name.replace('_', '-')}"
        
        try:
            container = self.docker_client.containers.get(container_name)
            container.stop(timeout=30)
            container.remove()
            
            if worker_name in self.running_workers:
                del self.running_workers[worker_name]
            
            logger.info(f"✅ Stopped worker {worker_name}")
            return {"success": True, "message": f"Worker {worker_name} stopped successfully"}
            
        except docker.errors.NotFound:
            return {"success": False, "error": "Worker not running"}
        except Exception as e:
            logger.error(f"❌ Failed to stop worker {worker_name}: {e}")
            return {"success": False, "error": str(e)}
    
    async def restart_worker(self, worker_name: str, **kwargs) -> Dict[str, Any]:
        """Restart a worker"""
        # Stop first
        stop_result = await self.stop_worker(worker_name)
        if not stop_result["success"] and "not running" not in stop_result["error"]:
            return stop_result
        
        # Wait a moment
        await asyncio.sleep(2)
        
        # Start again
        return await self.start_worker(worker_name, **kwargs)
    
    async def get_system_status(self) -> Dict[str, Any]:
        """Get overall system status"""
        workers = await self.discover_workers()
        
        status = {
            "timestamp": datetime.now().isoformat(),
            "total_workers": len(workers),
            "enabled_workers": len(get_enabled_workers()),
            "running_workers": len(self.running_workers),
            "worker_types": {},
            "docker_status": "unknown",
            "database_status": "unknown"
        }
        
        # Count by type
        for worker_type in WorkerType:
            workers_of_type = get_workers_by_type(worker_type)
            status["worker_types"][worker_type.value] = {
                "total": len(workers_of_type),
                "enabled": len([w for w in workers_of_type if w.schedule and w.schedule.enabled])
            }
        
        # Check Docker status
        if self.docker_client:
            try:
                self.docker_client.ping()
                status["docker_status"] = "connected"
            except Exception as e:
                status["docker_status"] = f"error: {e}"
        
        # Check database status
        try:
            async with AsyncSessionLocal() as db:
                await db.execute("SELECT 1")
            status["database_status"] = "connected"
        except Exception as e:
            status["database_status"] = f"error: {e}"
        
        return status
    
    async def schedule_worker(self, worker_name: str) -> Dict[str, Any]:
        """Schedule a worker to run based on its configuration"""
        worker = get_worker(worker_name)
        if not worker or not worker.schedule:
            return {"success": False, "error": "Worker not found or not schedulable"}
        
        if not worker.schedule.enabled:
            return {"success": False, "error": "Worker not enabled"}
        
        # For now, just start the worker
        # In production, this would integrate with a scheduler like Celery
        return await self.start_worker(worker_name)
    
    async def get_worker_logs(self, worker_name: str, tail: int = 100) -> Dict[str, Any]:
        """Get recent logs from a worker"""
        if not self.docker_client:
            return {"success": False, "error": "Docker client not available"}
        
        container_name = f"yodabuffett-{worker_name.replace('_', '-')}"
        
        try:
            container = self.docker_client.containers.get(container_name)
            logs = container.logs(tail=tail, timestamps=True).decode('utf-8')
            
            return {
                "success": True,
                "worker_name": worker_name,
                "container_id": container.short_id,
                "logs": logs
            }
            
        except docker.errors.NotFound:
            return {"success": False, "error": "Worker container not found"}
        except Exception as e:
            return {"success": False, "error": str(e)}

# FastAPI Application
app = FastAPI(
    title="YodaBuffett Worker Manager",
    description="Centralized management system for YodaBuffett workers",
    version="1.0.0"
)

# Global manager instance
manager = WorkerManager()

@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "YodaBuffett Worker Manager", "timestamp": datetime.now().isoformat()}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/workers")
async def list_workers():
    """List all workers"""
    workers = await manager.discover_workers()
    return {
        "workers": [
            {
                "name": w.name,
                "display_name": w.display_name,
                "type": w.worker_type.value,
                "market": w.market.value if w.market else None,
                "enabled": w.schedule.enabled if w.schedule else False
            }
            for w in workers
        ]
    }

@app.get("/workers/{worker_name}/status")
async def get_worker_status(worker_name: str):
    """Get status of a specific worker"""
    status = await manager.get_worker_status(worker_name)
    if status.get("status") == "not_found":
        raise HTTPException(status_code=404, detail="Worker not found")
    return status

@app.post("/workers/{worker_name}/start")
async def start_worker(worker_name: str, background_tasks: BackgroundTasks):
    """Start a worker"""
    result = await manager.start_worker(worker_name)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["error"])
    return result

@app.post("/workers/{worker_name}/stop")
async def stop_worker(worker_name: str):
    """Stop a worker"""
    result = await manager.stop_worker(worker_name)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["error"])
    return result

@app.post("/workers/{worker_name}/restart")
async def restart_worker(worker_name: str):
    """Restart a worker"""
    result = await manager.restart_worker(worker_name)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["error"])
    return result

@app.get("/workers/{worker_name}/logs")
async def get_worker_logs(worker_name: str, tail: int = 100):
    """Get worker logs"""
    result = await manager.get_worker_logs(worker_name, tail)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["error"])
    return result

@app.get("/system/status")
async def get_system_status():
    """Get overall system status"""
    return await manager.get_system_status()

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """Simple web dashboard"""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>YodaBuffett Worker Manager</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            .worker { border: 1px solid #ccc; margin: 10px 0; padding: 15px; border-radius: 5px; }
            .status { padding: 2px 8px; border-radius: 3px; color: white; font-size: 12px; }
            .running { background-color: green; }
            .stopped { background-color: red; }
            .unknown { background-color: gray; }
            button { margin: 5px; padding: 5px 10px; cursor: pointer; }
            .start { background-color: green; color: white; }
            .stop { background-color: red; color: white; }
            .restart { background-color: orange; color: white; }
        </style>
        <script>
            async function fetchWorkers() {
                try {
                    const response = await fetch('/workers');
                    const data = await response.json();
                    displayWorkers(data.workers);
                } catch (error) {
                    console.error('Error fetching workers:', error);
                }
            }
            
            async function fetchSystemStatus() {
                try {
                    const response = await fetch('/system/status');
                    const data = await response.json();
                    displaySystemStatus(data);
                } catch (error) {
                    console.error('Error fetching system status:', error);
                }
            }
            
            function displaySystemStatus(status) {
                document.getElementById('systemStatus').innerHTML = `
                    <h3>System Status</h3>
                    <p>Total Workers: ${status.total_workers}</p>
                    <p>Enabled Workers: ${status.enabled_workers}</p>
                    <p>Running Workers: ${status.running_workers}</p>
                    <p>Docker Status: ${status.docker_status}</p>
                    <p>Database Status: ${status.database_status}</p>
                `;
            }
            
            function displayWorkers(workers) {
                const container = document.getElementById('workersContainer');
                container.innerHTML = workers.map(worker => `
                    <div class="worker">
                        <h4>${worker.display_name} (${worker.name})</h4>
                        <p>Type: ${worker.type}</p>
                        <p>Market: ${worker.market || 'N/A'}</p>
                        <span class="status ${worker.enabled ? 'running' : 'stopped'}">
                            ${worker.enabled ? 'Enabled' : 'Disabled'}
                        </span>
                        <br><br>
                        <button class="start" onclick="controlWorker('${worker.name}', 'start')">Start</button>
                        <button class="stop" onclick="controlWorker('${worker.name}', 'stop')">Stop</button>
                        <button class="restart" onclick="controlWorker('${worker.name}', 'restart')">Restart</button>
                        <button onclick="showLogs('${worker.name}')">Logs</button>
                    </div>
                `).join('');
            }
            
            async function controlWorker(workerName, action) {
                try {
                    const response = await fetch(`/workers/${workerName}/${action}`, {
                        method: 'POST'
                    });
                    const result = await response.json();
                    alert(result.message || result.error);
                    fetchWorkers(); // Refresh
                } catch (error) {
                    alert('Error: ' + error.message);
                }
            }
            
            async function showLogs(workerName) {
                try {
                    const response = await fetch(`/workers/${workerName}/logs`);
                    const result = await response.json();
                    if (result.success) {
                        const popup = window.open('', '_blank', 'width=800,height=600');
                        popup.document.write(`
                            <html>
                            <head><title>Logs: ${workerName}</title></head>
                            <body style="font-family: monospace; padding: 20px;">
                                <h3>Logs: ${workerName}</h3>
                                <pre style="background: #f5f5f5; padding: 15px; overflow-x: auto;">${result.logs}</pre>
                            </body>
                            </html>
                        `);
                    } else {
                        alert('Error fetching logs: ' + result.error);
                    }
                } catch (error) {
                    alert('Error: ' + error.message);
                }
            }
            
            // Auto-refresh every 30 seconds
            setInterval(() => {
                fetchWorkers();
                fetchSystemStatus();
            }, 30000);
            
            // Initial load
            window.onload = () => {
                fetchWorkers();
                fetchSystemStatus();
            };
        </script>
    </head>
    <body>
        <h1>🎛️ YodaBuffett Worker Manager</h1>
        
        <div id="systemStatus"></div>
        
        <h2>Workers</h2>
        <div id="workersContainer">Loading...</div>
        
        <p><em>Dashboard auto-refreshes every 30 seconds</em></p>
    </body>
    </html>
    """

async def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='YodaBuffett Worker Manager')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=8090, help='Port to run on')
    parser.add_argument('--reload', action='store_true', help='Enable auto-reload')
    
    args = parser.parse_args()
    
    logger.info(f"🎛️  Starting YodaBuffett Worker Manager on {args.host}:{args.port}")
    
    # Initialize manager
    workers = await manager.discover_workers()
    logger.info(f"📋 Discovered {len(workers)} workers")
    
    # Run FastAPI server
    uvicorn.run(
        "workers.management.worker_manager:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level="info"
    )

if __name__ == "__main__":
    asyncio.run(main())