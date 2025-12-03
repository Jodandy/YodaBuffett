#!/usr/bin/env python3
"""
Setup Daily Document Pipeline Automation
Creates LaunchAgent for automated daily document processing pipeline
"""

import os
import subprocess
from pathlib import Path

def create_document_pipeline_launchagent():
    """Create LaunchAgent for daily document processing pipeline"""
    
    # Get absolute paths
    backend_dir = Path(__file__).parent
    python_path = backend_dir / "venv" / "bin" / "python"
    script_path = backend_dir / "workers" / "daily_document_pipeline.py"
    
    # LaunchAgent configuration
    plist_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.yodabuffett.daily-document-pipeline</string>
    
    <key>ProgramArguments</key>
    <array>
        <string>{python_path}</string>
        <string>{script_path}</string>
        <string>--run-time</string>
        <string>11:00</string>
    </array>
    
    <key>WorkingDirectory</key>
    <string>{backend_dir}</string>
    
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>11</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    
    <key>StandardOutPath</key>
    <string>{backend_dir}/logs/daily-document-pipeline.log</string>
    
    <key>StandardErrorPath</key>
    <string>{backend_dir}/logs/daily-document-pipeline-error.log</string>
    
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin</string>
        <key>PYTHONPATH</key>
        <string>{backend_dir}</string>
    </dict>
    
    <key>RunAtLoad</key>
    <false/>
    
    <key>KeepAlive</key>
    <false/>
</dict>
</plist>"""
    
    # Write plist file
    home_dir = Path.home()
    launchagents_dir = home_dir / "Library" / "LaunchAgents"
    launchagents_dir.mkdir(exist_ok=True)
    
    plist_path = launchagents_dir / "com.yodabuffett.daily-document-pipeline.plist"
    
    with open(plist_path, 'w') as f:
        f.write(plist_content)
    
    print(f"✅ Created LaunchAgent: {plist_path}")
    
    # Load the LaunchAgent
    try:
        subprocess.run(['launchctl', 'unload', str(plist_path)], 
                      capture_output=True, check=False)
        
        result = subprocess.run(['launchctl', 'load', str(plist_path)], 
                               capture_output=True, text=True, check=True)
        
        print(f"✅ Loaded LaunchAgent successfully")
        
        # List the job to verify
        list_result = subprocess.run(['launchctl', 'list', 'com.yodabuffett.daily-document-pipeline'], 
                                   capture_output=True, text=True, check=False)
        
        if list_result.returncode == 0:
            print(f"✅ LaunchAgent is registered and ready")
            print(f"📅 Will run daily at 11:00 AM")
            print(f"📝 Logs: {backend_dir}/logs/daily-document-pipeline.log")
        else:
            print(f"⚠️  LaunchAgent loaded but not visible in list")
            
    except subprocess.CalledProcessError as e:
        print(f"❌ Error loading LaunchAgent: {e}")
        print(f"   stdout: {e.stdout}")
        print(f"   stderr: {e.stderr}")
    
    return plist_path

def update_existing_schedule():
    """Update the existing automation schedule to include document pipeline"""
    
    print("\n📅 UPDATED DAILY AUTOMATION SCHEDULE:")
    print("="*60)
    print("🌅 03:00 AM - Daily Market Data Worker")
    print("📄 07:00 AM - Document Discovery Worker (Morning)")  
    print("📄 09:00 AM - Document Discovery Worker (Late)")
    print("📥 10:00 AM - PDF Download Worker")
    print("🔄 11:00 AM - Document Processing Pipeline (NEW!)")
    print("   └── Text Extraction")
    print("   └── Vector Embeddings")
    print("   └── Section Processing")
    print("="*60)
    
    print("\n💡 PIPELINE FLOW:")
    print("1. Documents discovered (7-9 AM)")
    print("2. PDFs downloaded (10 AM)")  
    print("3. Text extracted & embedded (11 AM)")
    print("4. Ready for search & analysis!")

def show_management_commands():
    """Show commands for managing the document pipeline"""
    
    print("\n🔧 MANAGEMENT COMMANDS:")
    print("="*50)
    
    print("\n📊 Check pipeline status:")
    print("launchctl list com.yodabuffett.daily-document-pipeline")
    
    print("\n📝 View pipeline logs:")
    print("tail -f logs/daily-document-pipeline.log")
    
    print("\n🏃 Run pipeline manually:")
    print("python3 workers/daily_document_pipeline.py")
    
    print("\n⏸️  Temporarily disable:")
    print("launchctl unload ~/Library/LaunchAgents/com.yodabuffett.daily-document-pipeline.plist")
    
    print("\n▶️  Re-enable:")
    print("launchctl load ~/Library/LaunchAgents/com.yodabuffett.daily-document-pipeline.plist")
    
    print("\n🗑️  Remove completely:")
    print("launchctl unload ~/Library/LaunchAgents/com.yodabuffett.daily-document-pipeline.plist")
    print("rm ~/Library/LaunchAgents/com.yodabuffett.daily-document-pipeline.plist")

def main():
    print("🚀 Setting up Daily Document Processing Pipeline Automation")
    print("="*70)
    
    # Create logs directory
    logs_dir = Path(__file__).parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    
    # Create data directory for pipeline results
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)
    
    # Create the LaunchAgent
    plist_path = create_document_pipeline_launchagent()
    
    # Show updated schedule
    update_existing_schedule()
    
    # Show management commands
    show_management_commands()
    
    print("\n✅ Document processing pipeline automation is now set up!")
    print("🎯 Complete automation: Discovery → Download → Extract → Embed")

if __name__ == "__main__":
    main()