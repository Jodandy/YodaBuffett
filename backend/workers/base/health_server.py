#!/usr/bin/env python3
"""
Health Check Server for YodaBuffett Workers

Simple HTTP server that provides health check endpoints for Docker containers.
Runs in background while workers are operating.
"""

import asyncio
import json
import argparse
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Dict, Any
import threading
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from workers.config.worker_registry import get_worker

class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP handler for health check requests"""
    
    def do_GET(self):
        """Handle GET requests"""
        if self.path == '/health':
            self.send_health_response()
        elif self.path == '/metrics':
            self.send_metrics_response()
        elif self.path == '/info':
            self.send_info_response()
        else:
            self.send_error(404, "Not Found")
    
    def send_health_response(self):
        """Send health check response"""
        try:
            health_data = {
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "worker_type": os.environ.get("WORKER_TYPE", "unknown"),
                "worker_name": os.environ.get("WORKER_NAME", "unknown"),
                "worker_market": os.environ.get("WORKER_MARKET", ""),
                "uptime_seconds": self.get_uptime_seconds()
            }
            
            # Add worker-specific health checks
            health_data.update(self.get_worker_health())
            
            self.send_json_response(200, health_data)
        except Exception as e:
            error_data = {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
            self.send_json_response(500, error_data)
    
    def send_metrics_response(self):
        """Send metrics response"""
        try:
            metrics_data = {
                "worker_type": os.environ.get("WORKER_TYPE", "unknown"),
                "worker_name": os.environ.get("WORKER_NAME", "unknown"),
                "timestamp": datetime.now().isoformat(),
                "metrics": self.get_worker_metrics()
            }
            self.send_json_response(200, metrics_data)
        except Exception as e:
            error_data = {
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
            self.send_json_response(500, error_data)
    
    def send_info_response(self):
        """Send worker info response"""
        try:
            worker_name = os.environ.get("WORKER_NAME", "unknown")
            worker_info = get_worker(worker_name)
            
            info_data = {
                "worker_name": worker_name,
                "worker_type": os.environ.get("WORKER_TYPE", "unknown"),
                "worker_market": os.environ.get("WORKER_MARKET", ""),
                "environment": {
                    "worker_mode": os.environ.get("WORKER_MODE", "unknown"),
                    "log_level": os.environ.get("LOG_LEVEL", "INFO"),
                    "data_volume_path": os.environ.get("DATA_VOLUME_PATH", "/app/data"),
                    "log_file_path": os.environ.get("LOG_FILE_PATH", "/app/logs/worker.log")
                },
                "registry_info": {
                    "display_name": worker_info.display_name if worker_info else "Unknown",
                    "description": worker_info.description if worker_info else "No description",
                    "capabilities": worker_info.capabilities if worker_info else [],
                    "schedule_enabled": worker_info.schedule.enabled if worker_info and worker_info.schedule else False
                } if worker_info else None,
                "timestamp": datetime.now().isoformat()
            }
            
            self.send_json_response(200, info_data)
        except Exception as e:
            error_data = {
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
            self.send_json_response(500, error_data)
    
    def send_json_response(self, status_code: int, data: Dict[str, Any]):
        """Send JSON response"""
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        
        response_json = json.dumps(data, indent=2)
        self.wfile.write(response_json.encode('utf-8'))
    
    def get_uptime_seconds(self) -> float:
        """Get worker uptime in seconds"""
        try:
            # Try to read start time from a file
            start_time_file = os.environ.get("DATA_VOLUME_PATH", "/app/data") + "/.start_time"
            if os.path.exists(start_time_file):
                with open(start_time_file, 'r') as f:
                    start_time = float(f.read().strip())
                return datetime.now().timestamp() - start_time
        except:
            pass
        
        return 0.0
    
    def get_worker_health(self) -> Dict[str, Any]:
        """Get worker-specific health information"""
        health = {}
        
        try:
            # Check database connectivity
            health["database"] = self.check_database_health()
            
            # Check disk space
            health["disk_space"] = self.check_disk_space()
            
            # Check memory usage
            health["memory"] = self.check_memory_usage()
            
        except Exception as e:
            health["health_check_error"] = str(e)
        
        return health
    
    def check_database_health(self) -> Dict[str, Any]:
        """Check database connectivity"""
        try:
            # This is a basic check - could be enhanced
            db_host = os.environ.get("DB_HOST", "localhost")
            return {
                "status": "unknown",
                "host": db_host,
                "message": "Database health check not implemented"
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }
    
    def check_disk_space(self) -> Dict[str, Any]:
        """Check available disk space"""
        try:
            import shutil
            data_path = os.environ.get("DATA_VOLUME_PATH", "/app/data")
            total, used, free = shutil.disk_usage(data_path)
            
            return {
                "data_volume": data_path,
                "total_gb": round(total / (1024**3), 2),
                "used_gb": round(used / (1024**3), 2),
                "free_gb": round(free / (1024**3), 2),
                "usage_percent": round((used / total) * 100, 1)
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }
    
    def check_memory_usage(self) -> Dict[str, Any]:
        """Check memory usage"""
        try:
            import psutil
            memory = psutil.virtual_memory()
            process = psutil.Process()
            
            return {
                "system_memory_gb": round(memory.total / (1024**3), 2),
                "system_available_gb": round(memory.available / (1024**3), 2),
                "system_usage_percent": memory.percent,
                "process_memory_mb": round(process.memory_info().rss / (1024**2), 2)
            }
        except ImportError:
            return {
                "status": "psutil_not_available",
                "message": "Install psutil for detailed memory metrics"
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }
    
    def get_worker_metrics(self) -> Dict[str, Any]:
        """Get worker-specific metrics"""
        metrics = {}
        
        try:
            # Try to read metrics from worker progress file
            progress_file = os.environ.get("DATA_VOLUME_PATH", "/app/data") + "/worker_progress.json"
            if os.path.exists(progress_file):
                with open(progress_file, 'r') as f:
                    metrics = json.load(f)
        except Exception as e:
            metrics["metrics_error"] = str(e)
        
        return metrics
    
    def log_message(self, format, *args):
        """Override to suppress HTTP server logs"""
        pass


class HealthServer:
    """Health check server for workers"""
    
    def __init__(self, port: int = 8080):
        self.port = port
        self.server = None
        self.thread = None
    
    def start(self):
        """Start the health server"""
        try:
            self.server = HTTPServer(('0.0.0.0', self.port), HealthCheckHandler)
            self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
            self.thread.start()
            print(f"✅ Health server started on port {self.port}")
            
            # Write start time for uptime calculation
            start_time_file = os.environ.get("DATA_VOLUME_PATH", "/app/data") + "/.start_time"
            os.makedirs(os.path.dirname(start_time_file), exist_ok=True)
            with open(start_time_file, 'w') as f:
                f.write(str(datetime.now().timestamp()))
            
        except Exception as e:
            print(f"❌ Failed to start health server: {e}")
    
    def stop(self):
        """Stop the health server"""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            if self.thread:
                self.thread.join()
            print("✅ Health server stopped")


def main():
    """Main function for standalone execution"""
    parser = argparse.ArgumentParser(description='YodaBuffett Worker Health Server')
    parser.add_argument('--port', type=int, default=8080, help='Port to run health server on')
    args = parser.parse_args()
    
    print(f"🏥 Starting YodaBuffett Worker Health Server on port {args.port}")
    
    server = HealthServer(port=args.port)
    server.start()
    
    try:
        # Keep the server running
        while True:
            import time
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Shutting down health server...")
        server.stop()


if __name__ == "__main__":
    main()