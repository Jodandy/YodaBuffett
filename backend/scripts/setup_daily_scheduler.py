#!/usr/bin/env python3
"""
Setup Script for Daily Event Worker Scheduling

This script helps you set up automatic daily execution of the event worker.
Choose between different scheduling methods based on your preference.
"""

import os
import sys
import subprocess
from pathlib import Path
import shutil

class DailySchedulerSetup:
    def __init__(self):
        self.backend_dir = Path(__file__).parent.parent
        self.venv_python = self.backend_dir / "venv" / "bin" / "python"
        
    def show_options(self):
        """Show available scheduling options"""
        print("🗓️  YodaBuffett Daily Event Worker Scheduler Setup")
        print("=" * 60)
        print()
        print("Choose how you want to run the daily worker automatically:")
        print()
        print("1. 🐳 Docker Scheduler (Recommended)")
        print("   - Runs in Docker container")
        print("   - Automatically restarts if it crashes") 
        print("   - Handles all dependencies")
        print("   - Best for production use")
        print()
        print("2. 🖥️  macOS LaunchAgent")
        print("   - Native macOS scheduling")
        print("   - Runs when you're logged in")
        print("   - Uses your virtual environment")
        print("   - Good for development")
        print()
        print("3. ⏰ Simple Python Scheduler")
        print("   - Runs as a background Python process")
        print("   - Keep terminal open or run with nohup")
        print("   - Manual process management")
        print("   - Good for testing")
        print()
        print("4. 📋 Manual Setup Instructions")
        print("   - Show commands to run manually")
        print("   - For custom setups")
        print()
        
        while True:
            choice = input("Enter your choice (1-4): ").strip()
            if choice in ['1', '2', '3', '4']:
                return choice
            print("Please enter 1, 2, 3, or 4")
    
    def setup_docker_scheduler(self):
        """Set up Docker-based scheduling"""
        print("\n🐳 Setting up Docker Scheduler...")
        
        # Check if Docker is installed
        try:
            subprocess.run(['docker', '--version'], check=True, capture_output=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("❌ Docker is not installed or not in PATH")
            print("Please install Docker Desktop and try again")
            return False
        
        docker_dir = self.backend_dir / "docker"
        env_file = docker_dir / ".env"
        
        if not env_file.exists():
            print("❌ Docker environment file not found")
            print(f"Please make sure {env_file} exists")
            return False
        
        print(f"✅ Docker environment file found: {env_file}")
        
        # Ask for schedule time
        run_time = input("What time should it run daily? (HH:MM format, default 06:00): ").strip()
        if not run_time:
            run_time = "06:00"
        
        # Update environment file
        with open(env_file, 'r') as f:
            content = f.read()
        
        if "DAILY_RUN_TIME=" in content:
            # Update existing
            lines = content.split('\n')
            for i, line in enumerate(lines):
                if line.startswith('DAILY_RUN_TIME='):
                    lines[i] = f'DAILY_RUN_TIME={run_time}'
                    break
            content = '\n'.join(lines)
        else:
            # Add new
            content += f'\nDAILY_RUN_TIME={run_time}\n'
        
        with open(env_file, 'w') as f:
            f.write(content)
        
        print(f"✅ Set daily run time to {run_time}")
        
        print("\nTo start the scheduler:")
        print(f"cd {docker_dir}")
        print("docker-compose up daily-event-scheduler -d")
        print()
        print("To check logs:")
        print("docker-compose logs -f daily-event-scheduler")
        print()
        print("To stop:")
        print("docker-compose down daily-event-scheduler")
        
        return True
    
    def setup_macos_launchagent(self):
        """Set up macOS LaunchAgent"""
        print("\n🖥️  Setting up macOS LaunchAgent...")
        
        plist_source = self.backend_dir / "scripts" / "yodabuffett.daily.plist"
        plist_dest = Path.home() / "Library" / "LaunchAgents" / "com.yodabuffett.daily-worker.plist"
        
        if not self.venv_python.exists():
            print(f"❌ Virtual environment not found: {self.venv_python}")
            print("Please set up your virtual environment first")
            return False
        
        # Create logs directory
        logs_dir = self.backend_dir / "logs"
        logs_dir.mkdir(exist_ok=True)
        
        # Copy and customize plist file
        with open(plist_source, 'r') as f:
            plist_content = f.read()
        
        # Replace paths with actual paths
        plist_content = plist_content.replace(
            "/Users/jdandemar/Documents/YodaBuffett/backend/venv/bin/python",
            str(self.venv_python)
        )
        plist_content = plist_content.replace(
            "/Users/jdandemar/Documents/YodaBuffett/backend",
            str(self.backend_dir)
        )
        
        # Ask for schedule time
        run_time = input("What time should it run daily? (HH:MM format, default 06:00): ").strip()
        if not run_time:
            run_time = "06:00"
        
        hour, minute = run_time.split(':')
        plist_content = plist_content.replace('<integer>6</integer>', f'<integer>{int(hour)}</integer>')
        plist_content = plist_content.replace('<integer>0</integer>', f'<integer>{int(minute)}</integer>')
        
        # Write the plist file
        with open(plist_dest, 'w') as f:
            f.write(plist_content)
        
        print(f"✅ Created launch agent: {plist_dest}")
        
        # Load the launch agent
        try:
            subprocess.run(['launchctl', 'load', str(plist_dest)], check=True)
            print(f"✅ Loaded launch agent - will run daily at {run_time}")
        except subprocess.CalledProcessError:
            print("⚠️  Failed to load launch agent automatically")
            print(f"You can load it manually with: launchctl load {plist_dest}")
        
        print("\nTo check status:")
        print("launchctl list | grep yodabuffett")
        print()
        print("To unload:")
        print(f"launchctl unload {plist_dest}")
        
        return True
    
    def setup_python_scheduler(self):
        """Set up Python-based scheduling"""
        print("\n⏰ Setting up Python Scheduler...")
        
        if not self.venv_python.exists():
            print(f"❌ Virtual environment not found: {self.venv_python}")
            print("Please set up your virtual environment first")
            return False
        
        # Ask for schedule time
        run_time = input("What time should it run daily? (HH:MM format, default 06:00): ").strip()
        if not run_time:
            run_time = "06:00"
        
        print(f"✅ Scheduler configured to run daily at {run_time}")
        print()
        print("To start the scheduler:")
        print(f"cd {self.backend_dir}")
        print(f"source venv/bin/activate")
        print(f"export DATA_VOLUME_PATH={self.backend_dir}/data")
        print(f"python -m workers.daily_scheduler --time {run_time}")
        print()
        print("To run in background:")
        print(f"nohup python -m workers.daily_scheduler --time {run_time} > logs/scheduler.log 2>&1 &")
        print()
        print("To test immediately:")
        print("python -m workers.daily_scheduler --run-now")
        
        return True
    
    def show_manual_instructions(self):
        """Show manual setup instructions"""
        print("\n📋 Manual Setup Instructions")
        print("=" * 40)
        print()
        print("Option 1 - Run Once:")
        print(f"cd {self.backend_dir}")
        print("source venv/bin/activate")
        print("export DATA_VOLUME_PATH=$(pwd)/data")
        print("python -m workers.daily_event_worker")
        print()
        print("Option 2 - Use Cron (edit with 'crontab -e'):")
        print(f"0 6 * * * cd {self.backend_dir} && source venv/bin/activate && export DATA_VOLUME_PATH={self.backend_dir}/data && python -m workers.daily_event_worker")
        print()
        print("Option 3 - Docker:")
        print(f"cd {self.backend_dir}/docker")
        print("docker-compose up daily-event-scheduler -d")
        
        return True
    
    def run(self):
        """Main setup flow"""
        choice = self.show_options()
        
        print()
        
        if choice == '1':
            success = self.setup_docker_scheduler()
        elif choice == '2':
            success = self.setup_macos_launchagent()
        elif choice == '3':
            success = self.setup_python_scheduler()
        elif choice == '4':
            success = self.show_manual_instructions()
        else:
            success = False
        
        if success:
            print("\n🎉 Setup completed!")
            print()
            print("The daily event worker will now run automatically.")
            print("It will only process companies with upcoming financial events,")
            print("making it much more efficient than processing all companies.")
        else:
            print("\n❌ Setup failed. Please check the error messages above.")
            return 1
        
        return 0

if __name__ == "__main__":
    setup = DailySchedulerSetup()
    sys.exit(setup.run())