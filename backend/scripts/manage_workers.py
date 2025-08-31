#!/usr/bin/env python3
"""
YodaBuffett Worker Management CLI

Production-ready management interface for event-driven Swedish data workers.
Provides operational commands for setup, monitoring, and maintenance.

Features:
- Worker lifecycle management (start, stop, status)
- Health monitoring and diagnostics
- Configuration management
- Results analysis and reporting
- Docker operations
- Development utilities
"""

import asyncio
import sys
import os
import json
import subprocess
import time
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
import argparse
import logging
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workers.event_scheduler import EventScheduler
from workers.daily_event_worker import DailyEventWorker
from workers.weekly_scanner import WeeklyScanner
from workers.worker_config import get_config, setup_worker_logging

class WorkerManager:
    """
    Management interface for YodaBuffett workers
    """
    
    def __init__(self):
        self.config = get_config()
        self.logger = setup_worker_logging()
        self.docker_compose_path = Path(__file__).parent.parent / "docker" / "docker-compose.yml"
        
    # =============================================================================
    # WORKER OPERATIONS
    # =============================================================================
    
    async def run_daily_worker(self, target_date: Optional[str] = None, dry_run: bool = False) -> int:
        """Run the daily event worker"""
        
        parsed_date = None
        if target_date:
            try:
                parsed_date = datetime.strptime(target_date, '%Y-%m-%d').date()
            except ValueError:
                print(f"❌ Invalid date format: {target_date}. Use YYYY-MM-DD")
                return 1
        
        if dry_run:
            print("🧪 DRY RUN: Daily Event Worker")
            print("=" * 50)
            
            scheduler = EventScheduler()
            targets = await scheduler.get_daily_scrape_targets(parsed_date)
            
            if not targets:
                print("📭 No events scheduled for target date")
                return 0
            
            print(f"🎯 Would process {len(targets)} companies:")
            for target in targets[:10]:
                print(f"  🏢 {target.company_name} - {target.event_title}")
            
            if len(targets) > 10:
                print(f"  ... and {len(targets) - 10} more companies")
            
            return 0
        
        # Production run
        worker = DailyEventWorker()
        results = await worker.run_daily_collection(parsed_date)
        
        print(f"\n📊 Daily Worker Results:")
        print(f"   Status: {results.get('status')}")
        print(f"   Companies: {results.get('statistics', {}).get('successful_companies', 0)}")
        print(f"   Documents: {results.get('statistics', {}).get('total_documents', 0)}")
        print(f"   Duration: {results.get('total_execution_time', 0):.1f}s")
        
        return 0 if results.get('status') == 'completed' else 1
    
    async def run_weekly_scanner(self, sample_size: int = 50, dry_run: bool = False) -> int:
        """Run the weekly surprise scanner"""
        
        if dry_run:
            print("🧪 DRY RUN: Weekly Surprise Scanner")
            print("=" * 50)
            
            scheduler = EventScheduler()
            targets = await scheduler.get_weekly_surprise_targets(sample_size)
            
            print(f"🎲 Would scan {len(targets)} companies for surprises")
            
            by_tier = {}
            for target in targets:
                tier = target.company_tier.value
                by_tier[tier] = by_tier.get(tier, 0) + 1
            
            print(f"📊 By company tier: {dict(by_tier)}")
            return 0
        
        # Production run
        scanner = WeeklyScanner(sample_size=sample_size)
        results = await scanner.run_weekly_scan()
        
        print(f"\n📊 Weekly Scanner Results:")
        print(f"   Status: {results.get('status')}")
        print(f"   Companies Scanned: {results.get('statistics', {}).get('companies_scanned', 0)}")
        print(f"   Surprises Found: {results.get('statistics', {}).get('surprises_found', 0)}")
        print(f"   Duration: {results.get('total_execution_time', 0):.1f}s")
        
        return 0 if results.get('status') == 'completed' else 1
    
    async def show_schedule_preview(self, days: int = 7) -> int:
        """Show upcoming schedule for next N days"""
        
        print(f"📅 Event Schedule Preview - Next {days} Days")
        print("=" * 60)
        
        scheduler = EventScheduler()
        
        for i in range(days):
            check_date = date.today() + timedelta(days=i)
            targets = await scheduler.get_daily_scrape_targets(check_date)
            
            if targets:
                print(f"\n📆 {check_date.strftime('%A, %Y-%m-%d')} ({len(targets)} companies)")
                
                by_priority = {}
                by_event_type = {}
                
                for target in targets:
                    # Count by priority
                    priority = target.priority.value
                    by_priority[priority] = by_priority.get(priority, 0) + 1
                    
                    # Count by event type
                    event_type = target.event_type
                    by_event_type[event_type] = by_event_type.get(event_type, 0) + 1
                
                print(f"   🎯 Priority: {dict(by_priority)}")
                print(f"   📋 Event Types: {dict(by_event_type)}")
                
                # Show top companies
                for target in targets[:3]:
                    print(f"   🏢 {target.company_name} - {target.event_title}")
                
                if len(targets) > 3:
                    print(f"   ... and {len(targets) - 3} more")
            else:
                print(f"\n📆 {check_date.strftime('%A, %Y-%m-%d')} - No scheduled events")
        
        return 0
    
    # =============================================================================
    # DOCKER OPERATIONS
    # =============================================================================
    
    def docker_status(self) -> int:
        """Show Docker container status"""
        
        if not self.docker_compose_path.exists():
            print(f"❌ Docker compose file not found: {self.docker_compose_path}")
            return 1
        
        print("🐳 Docker Container Status")
        print("=" * 40)
        
        try:
            result = subprocess.run([
                "docker-compose", "-f", str(self.docker_compose_path), "ps"
            ], capture_output=True, text=True, check=True)
            
            print(result.stdout)
            return 0
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Docker command failed: {e}")
            print(e.stderr)
            return 1
        except FileNotFoundError:
            print("❌ Docker Compose not found. Please install Docker and Docker Compose.")
            return 1
    
    def docker_start(self, service: Optional[str] = None) -> int:
        """Start Docker services"""
        
        if not self.docker_compose_path.exists():
            print(f"❌ Docker compose file not found: {self.docker_compose_path}")
            return 1
        
        print(f"🚀 Starting Docker services...")
        if service:
            print(f"   Service: {service}")
        
        try:
            cmd = ["docker-compose", "-f", str(self.docker_compose_path), "up", "-d"]
            if service:
                cmd.append(service)
            
            result = subprocess.run(cmd, check=True)
            print("✅ Services started successfully")
            return 0
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to start services: {e}")
            return 1
    
    def docker_stop(self, service: Optional[str] = None) -> int:
        """Stop Docker services"""
        
        print(f"⏹️  Stopping Docker services...")
        if service:
            print(f"   Service: {service}")
        
        try:
            cmd = ["docker-compose", "-f", str(self.docker_compose_path), "stop"]
            if service:
                cmd.append(service)
            
            result = subprocess.run(cmd, check=True)
            print("✅ Services stopped successfully")
            return 0
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to stop services: {e}")
            return 1
    
    def docker_logs(self, service: str, follow: bool = False, tail: int = 100) -> int:
        """Show Docker service logs"""
        
        print(f"📋 Logs for service: {service}")
        print("=" * 40)
        
        try:
            cmd = ["docker-compose", "-f", str(self.docker_compose_path), "logs"]
            if follow:
                cmd.append("-f")
            cmd.extend(["--tail", str(tail)])
            cmd.append(service)
            
            result = subprocess.run(cmd)
            return result.returncode
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to get logs: {e}")
            return 1
    
    # =============================================================================
    # MONITORING AND ANALYSIS
    # =============================================================================
    
    async def analyze_results(self, results_file: Optional[str] = None, days: int = 7) -> int:
        """Analyze recent worker results"""
        
        print(f"📊 Worker Results Analysis - Last {days} Days")
        print("=" * 60)
        
        data_path = Path(self.config.data_volume_path)
        
        # Find recent result files
        result_files = []
        cutoff_date = datetime.now() - timedelta(days=days)
        
        if results_file:
            # Analyze specific file
            file_path = Path(results_file)
            if file_path.exists():
                result_files = [file_path]
            else:
                print(f"❌ Results file not found: {results_file}")
                return 1
        else:
            # Find recent files
            for pattern in ["daily_worker_*.json", "weekly_scanner_*.json"]:
                for file_path in data_path.glob(pattern):
                    if file_path.stat().st_mtime > cutoff_date.timestamp():
                        result_files.append(file_path)
        
        if not result_files:
            print("📭 No recent result files found")
            return 0
        
        result_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
        
        print(f"📁 Found {len(result_files)} result files:")
        
        total_stats = {
            'total_runs': 0,
            'successful_runs': 0,
            'total_companies': 0,
            'successful_companies': 0,
            'total_documents': 0,
            'total_events': 0,
            'total_time': 0
        }
        
        for file_path in result_files:
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                session_id = data.get('session_id', file_path.name)
                status = data.get('status', 'unknown')
                stats = data.get('statistics', {})
                
                print(f"\n📋 {session_id}")
                print(f"   Status: {status}")
                print(f"   Companies: {stats.get('successful_companies', 0)}/{stats.get('total_companies', 0)}")
                print(f"   Documents: {stats.get('total_documents', 0)}")
                print(f"   Events: {stats.get('total_events', 0)}")
                print(f"   Duration: {data.get('total_execution_time', 0):.1f}s")
                
                # Aggregate stats
                total_stats['total_runs'] += 1
                if status == 'completed':
                    total_stats['successful_runs'] += 1
                
                total_stats['total_companies'] += stats.get('total_companies', 0)
                total_stats['successful_companies'] += stats.get('successful_companies', 0)
                total_stats['total_documents'] += stats.get('total_documents', 0)
                total_stats['total_events'] += stats.get('total_events', 0)
                total_stats['total_time'] += data.get('total_execution_time', 0)
                
            except Exception as e:
                print(f"⚠️  Error reading {file_path}: {e}")
        
        # Show summary
        print(f"\n📈 SUMMARY - Last {days} Days:")
        print(f"   Total Runs: {total_stats['total_runs']} ({total_stats['successful_runs']} successful)")
        print(f"   Success Rate: {(total_stats['successful_runs']/max(total_stats['total_runs'], 1)*100):.1f}%")
        print(f"   Companies Processed: {total_stats['successful_companies']}")
        print(f"   Documents Collected: {total_stats['total_documents']}")
        print(f"   Events Created: {total_stats['total_events']}")
        print(f"   Total Processing Time: {total_stats['total_time']:.1f}s")
        
        return 0
    
    def health_check(self) -> int:
        """Perform comprehensive health check"""
        
        print("🏥 YodaBuffett Worker Health Check")
        print("=" * 50)
        
        checks_passed = 0
        total_checks = 0
        
        # Check 1: Configuration
        total_checks += 1
        try:
            config = get_config()
            print("✅ Configuration loaded successfully")
            checks_passed += 1
        except Exception as e:
            print(f"❌ Configuration failed: {e}")
        
        # Check 2: Database connectivity
        total_checks += 1
        try:
            # This is a simplified check - in production you'd test actual connection
            db_url = self.config.database.connection_url
            if db_url:
                print("✅ Database configuration valid")
                checks_passed += 1
            else:
                print("❌ Database configuration invalid")
        except Exception as e:
            print(f"❌ Database check failed: {e}")
        
        # Check 3: Data directory
        total_checks += 1
        data_path = Path(self.config.data_volume_path)
        if data_path.exists() and data_path.is_dir():
            print(f"✅ Data directory accessible: {data_path}")
            checks_passed += 1
        else:
            print(f"❌ Data directory not accessible: {data_path}")
        
        # Check 4: Docker services (if available)
        total_checks += 1
        try:
            result = subprocess.run([
                "docker-compose", "-f", str(self.docker_compose_path), "ps", "--services"
            ], capture_output=True, text=True, check=True)
            print("✅ Docker Compose available")
            checks_passed += 1
        except:
            print("⚠️  Docker Compose not available (optional)")
        
        # Summary
        print(f"\n📊 Health Check Results:")
        print(f"   Checks Passed: {checks_passed}/{total_checks}")
        print(f"   Status: {'HEALTHY' if checks_passed == total_checks else 'DEGRADED'}")
        
        return 0 if checks_passed == total_checks else 1

# =============================================================================
# CLI INTERFACE
# =============================================================================

async def main():
    """Main CLI interface"""
    
    parser = argparse.ArgumentParser(
        description='YodaBuffett Worker Management CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run daily worker for today
  python manage_workers.py daily-worker
  
  # Preview schedule for next week
  python manage_workers.py schedule --days 7
  
  # Run weekly surprise scanner
  python manage_workers.py weekly-scanner --sample-size 30
  
  # Docker operations
  python manage_workers.py docker --action start
  python manage_workers.py docker --action logs --service daily-worker
  
  # Analysis and monitoring
  python manage_workers.py analyze --days 7
  python manage_workers.py health-check
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Daily worker command
    daily_parser = subparsers.add_parser('daily-worker', help='Run daily event worker')
    daily_parser.add_argument('--date', help='Target date (YYYY-MM-DD)')
    daily_parser.add_argument('--dry-run', action='store_true', help='Show targets without running')
    
    # Weekly scanner command
    weekly_parser = subparsers.add_parser('weekly-scanner', help='Run weekly surprise scanner')
    weekly_parser.add_argument('--sample-size', type=int, default=50, help='Number of companies to scan')
    weekly_parser.add_argument('--dry-run', action='store_true', help='Show targets without running')
    
    # Schedule command
    schedule_parser = subparsers.add_parser('schedule', help='Show upcoming schedule')
    schedule_parser.add_argument('--days', type=int, default=7, help='Days to preview')
    
    # Docker command
    docker_parser = subparsers.add_parser('docker', help='Docker operations')
    docker_parser.add_argument('--action', choices=['status', 'start', 'stop', 'logs'], required=True)
    docker_parser.add_argument('--service', help='Specific service name')
    docker_parser.add_argument('--follow', action='store_true', help='Follow logs')
    docker_parser.add_argument('--tail', type=int, default=100, help='Number of log lines')
    
    # Analysis command
    analyze_parser = subparsers.add_parser('analyze', help='Analyze worker results')
    analyze_parser.add_argument('--file', help='Specific results file to analyze')
    analyze_parser.add_argument('--days', type=int, default=7, help='Days to analyze')
    
    # Health check command
    health_parser = subparsers.add_parser('health-check', help='Perform health check')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    manager = WorkerManager()
    
    try:
        if args.command == 'daily-worker':
            return await manager.run_daily_worker(args.date, args.dry_run)
        
        elif args.command == 'weekly-scanner':
            return await manager.run_weekly_scanner(args.sample_size, args.dry_run)
        
        elif args.command == 'schedule':
            return await manager.show_schedule_preview(args.days)
        
        elif args.command == 'docker':
            if args.action == 'status':
                return manager.docker_status()
            elif args.action == 'start':
                return manager.docker_start(args.service)
            elif args.action == 'stop':
                return manager.docker_stop(args.service)
            elif args.action == 'logs':
                return manager.docker_logs(args.service or 'daily-worker', args.follow, args.tail)
        
        elif args.command == 'analyze':
            return await manager.analyze_results(args.file, args.days)
        
        elif args.command == 'health-check':
            return manager.health_check()
        
        else:
            print(f"❌ Unknown command: {args.command}")
            return 1
            
    except KeyboardInterrupt:
        print("\n⏹️  Operation interrupted by user")
        return 130
    except Exception as e:
        print(f"❌ Command failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)