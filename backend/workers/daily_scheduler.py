#!/usr/bin/env python3
"""
Daily Event Worker Scheduler

Runs the daily event worker automatically every day at a specified time.
Can be run as a daemon service or in Docker.
"""

import asyncio
import schedule
import time
import sys
import os
from datetime import datetime, time as dt_time
import logging
import subprocess
import signal
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workers.worker_config import get_config, setup_worker_logging

class DailyScheduler:
    """Scheduler that runs the daily event worker automatically"""
    
    def __init__(self, run_time: str = "06:00"):
        """
        Initialize scheduler
        
        Args:
            run_time: Time to run daily (24-hour format, e.g. "06:00")
        """
        self.run_time = run_time
        self.config = get_config()
        self.logger = setup_worker_logging()
        self.is_running = False
        self.should_stop = False
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        self.logger.info(f"🗓️  Daily scheduler initialized - will run at {run_time} daily")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"📡 Received signal {signum}, stopping scheduler...")
        self.should_stop = True
    
    def run_daily_worker(self):
        """Execute the daily event worker"""
        if self.is_running:
            self.logger.warning("⚠️  Daily worker already running, skipping this execution")
            return
        
        self.logger.info("🚀 Starting scheduled daily event worker execution...")
        self.is_running = True
        
        try:
            # Run the daily event worker
            python_path = sys.executable
            script_path = Path(__file__).parent / "daily_event_worker.py"
            
            # Set environment variables
            env = os.environ.copy()
            env['DATA_VOLUME_PATH'] = env.get('DATA_VOLUME_PATH', str(Path.cwd() / 'data'))
            
            # Execute the worker
            result = subprocess.run([
                python_path, "-m", "workers.daily_event_worker"
            ], 
            cwd=Path(__file__).parent.parent,
            env=env,
            capture_output=True, 
            text=True,
            timeout=3600  # 1 hour timeout
            )
            
            if result.returncode == 0:
                self.logger.info("✅ Daily worker execution completed successfully")
                if result.stdout:
                    self.logger.info(f"Worker output: {result.stdout[-500:]}")  # Last 500 chars
            else:
                self.logger.error(f"❌ Daily worker failed with return code {result.returncode}")
                if result.stderr:
                    self.logger.error(f"Worker error: {result.stderr}")
                if result.stdout:
                    self.logger.info(f"Worker output: {result.stdout}")
        
        except subprocess.TimeoutExpired:
            self.logger.error("⏰ Daily worker execution timed out (1 hour)")
        except Exception as e:
            self.logger.error(f"❌ Failed to execute daily worker: {e}")
        finally:
            self.is_running = False
    
    def start(self):
        """Start the scheduler daemon"""
        self.logger.info(f"📅 Starting daily scheduler - runs at {self.run_time}")
        
        # Schedule the daily job
        schedule.every().day.at(self.run_time).do(self.run_daily_worker)
        
        # Run immediately on startup (optional)
        if self.config.mode.value == "development":
            self.logger.info("🧪 Development mode - running worker immediately for testing")
            self.run_daily_worker()
        
        # Main scheduler loop
        while not self.should_stop:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            except KeyboardInterrupt:
                self.logger.info("⏹️  Scheduler stopped by user")
                break
            except Exception as e:
                self.logger.error(f"❌ Scheduler error: {e}")
                time.sleep(60)  # Wait before retrying
        
        self.logger.info("🏁 Daily scheduler stopped")

def main():
    """Main entry point with CLI arguments"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Daily Event Worker Scheduler')
    parser.add_argument('--time', default='06:00', help='Daily run time (HH:MM format, default: 06:00)')
    parser.add_argument('--run-now', action='store_true', help='Run worker immediately and exit')
    
    args = parser.parse_args()
    
    scheduler = DailyScheduler(run_time=args.time)
    
    if args.run_now:
        # Run once and exit
        scheduler.run_daily_worker()
        return
    
    try:
        # Start the scheduler daemon
        scheduler.start()
    except KeyboardInterrupt:
        print("\n⏹️  Scheduler stopped by user")
    except Exception as e:
        print(f"❌ Scheduler failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()