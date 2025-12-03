#!/usr/bin/env python3
"""
Setup and Management Script for Daily Workers

Manages both document and market data daily workers with easy commands.
"""

import asyncio
import subprocess
import sys
import os
import json
import requests
from datetime import datetime, date, timedelta
from typing import Dict, List

def run_command(command: str, description: str = None) -> bool:
    """Run a shell command and return success status"""
    if description:
        print(f"🔄 {description}")
    
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Success: {description or command}")
            if result.stdout.strip():
                print(f"   Output: {result.stdout.strip()}")
            return True
        else:
            print(f"❌ Failed: {description or command}")
            if result.stderr.strip():
                print(f"   Error: {result.stderr.strip()}")
            return False
    except Exception as e:
        print(f"❌ Exception running command: {e}")
        return False

def check_worker_health(port: int, worker_name: str) -> Dict:
    """Check health of a daily worker"""
    try:
        response = requests.get(f"http://localhost:{port}/health", timeout=5)
        if response.status_code == 200:
            health_data = response.json()
            print(f"✅ {worker_name} is healthy")
            print(f"   Status: {health_data.get('status', 'unknown')}")
            print(f"   Stats: {health_data.get('stats', {})}")
            return health_data
        else:
            print(f"❌ {worker_name} health check failed (HTTP {response.status_code})")
            return {}
    except Exception as e:
        print(f"❌ {worker_name} not responding: {e}")
        return {}

def check_docker_status():
    """Check if Docker is running and services are up"""
    print("🐳 Checking Docker Status")
    print("=" * 50)
    
    # Check if Docker is running
    docker_running = run_command("docker info > /dev/null 2>&1", "Checking if Docker is running")
    
    if not docker_running:
        print("❌ Docker is not running. Please start Docker first.")
        return False
    
    # Check running containers
    result = subprocess.run(
        "docker ps --filter 'name=yodabuffett' --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'",
        shell=True, capture_output=True, text=True
    )
    
    if result.returncode == 0:
        print("\n📊 YodaBuffett Containers:")
        print(result.stdout)
    
    return True

def start_daily_workers():
    """Start both daily workers"""
    print("\n🚀 Starting Daily Workers")
    print("=" * 40)
    
    # Start document scheduler
    success1 = run_command(
        "cd docker && docker-compose up daily-event-scheduler -d",
        "Starting daily document scheduler"
    )
    
    # Start market data scheduler  
    success2 = run_command(
        "cd docker && docker-compose up daily-market-data-scheduler -d",
        "Starting daily market data scheduler"
    )
    
    if success1 and success2:
        print("\n✅ Both daily workers started successfully!")
        print("⏰ Document updates: 06:00 AM daily")
        print("⏰ Market data updates: 06:30 AM daily")
        
        # Wait a moment for services to start
        print("\n⏳ Waiting for services to initialize...")
        import time
        time.sleep(10)
        
        # Check health
        print("\n🏥 Checking Worker Health:")
        check_worker_health(8085, "Document Daily Scheduler")
        check_worker_health(8086, "Market Data Daily Scheduler")
        
    else:
        print("❌ Failed to start some daily workers")

def stop_daily_workers():
    """Stop both daily workers"""
    print("\n🛑 Stopping Daily Workers")
    print("=" * 40)
    
    success1 = run_command(
        "cd docker && docker-compose stop daily-event-scheduler",
        "Stopping daily document scheduler"
    )
    
    success2 = run_command(
        "cd docker && docker-compose stop daily-market-data-scheduler", 
        "Stopping daily market data scheduler"
    )
    
    if success1 and success2:
        print("✅ Both daily workers stopped successfully!")
    else:
        print("❌ Failed to stop some daily workers")

def run_document_catchup():
    """Run document catch-up interactively"""
    print("\n📚 Document Historical Catch-Up")
    print("=" * 40)
    
    # Ask for the catch-up period
    while True:
        try:
            days_back = input("🗓️  How many days back to catch up? (e.g., 30): ").strip()
            days_back = int(days_back)
            if days_back > 0 and days_back <= 365:
                break
            else:
                print("❌ Please enter a number between 1 and 365")
        except ValueError:
            print("❌ Please enter a valid number")
    
    print(f"\n🚀 Running document catch-up for the last {days_back} days...")
    
    # Run the catch-up script
    success = run_command(
        f"cd .. && python3 historical_document_catchup.py --days-back {days_back}",
        f"Document catch-up for {days_back} days"
    )
    
    if success:
        print("✅ Document catch-up completed successfully!")
    else:
        print("❌ Document catch-up failed")

def run_market_data_now():
    """Run market data update immediately"""
    print("\n📈 Running Market Data Update Now")
    print("=" * 40)
    
    success = run_command(
        "python3 workers/daily_market_data_worker.py --run-now",
        "Immediate market data update"
    )
    
    if success:
        print("✅ Market data update completed!")
    else:
        print("❌ Market data update failed")

def show_status():
    """Show status of all daily workers and recent activity"""
    print("\n📊 Daily Workers Status")
    print("=" * 40)
    
    # Check Docker status
    if not check_docker_status():
        return
    
    print("\n🏥 Worker Health Checks:")
    doc_health = check_worker_health(8085, "Document Daily Scheduler")
    market_health = check_worker_health(8086, "Market Data Daily Scheduler")
    
    # Check for recent results files
    print("\n📁 Recent Activity:")
    try:
        # Check for recent document results
        import glob
        doc_files = glob.glob("data/daily_worker_*.json")
        doc_files.sort(reverse=True)
        
        if doc_files:
            print(f"📚 Latest document update: {os.path.basename(doc_files[0])}")
        
        market_files = glob.glob("data/daily_market_data_*.json")
        market_files.sort(reverse=True)
        
        if market_files:
            print(f"📈 Latest market data update: {os.path.basename(market_files[0])}")
            
    except Exception as e:
        print(f"⚠️  Could not check recent activity: {e}")

def main():
    """Main interactive menu"""
    
    print("🤖 YODABUFFETT DAILY WORKERS SETUP")
    print("Manage document and market data daily workers")
    print("=" * 60)
    
    while True:
        print("\n📋 Available Commands:")
        print("  1. 🏥 Show status of daily workers")
        print("  2. 🚀 Start both daily workers") 
        print("  3. 🛑 Stop both daily workers")
        print("  4. 📚 Run document historical catch-up")
        print("  5. 📈 Run market data update now")
        print("  6. ❌ Exit")
        
        try:
            choice = input("\n🎯 Select option (1-6): ").strip()
            
            if choice == "1":
                show_status()
            elif choice == "2":
                start_daily_workers()
            elif choice == "3":
                stop_daily_workers()
            elif choice == "4":
                run_document_catchup()
            elif choice == "5":
                run_market_data_now()
            elif choice == "6":
                print("\n👋 Goodbye!")
                break
            else:
                print("❌ Invalid choice. Please select 1-6.")
                
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except EOFError:
            print("\n\n👋 Goodbye!")
            break

if __name__ == "__main__":
    main()