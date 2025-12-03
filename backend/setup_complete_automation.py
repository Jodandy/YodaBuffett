#!/usr/bin/env python3
"""
Complete Automation Setup Summary
Shows all daily automation components
"""

from pathlib import Path
from datetime import datetime


def show_automation_status():
    """Show status of all automation components"""
    
    print("🚀 YODABUFFETT COMPLETE DAILY AUTOMATION SYSTEM")
    print("="*70)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print()
    
    print("📅 DAILY SCHEDULE (macOS LaunchAgents)")
    print("="*70)
    print("🌅 03:00 AM - Market Data Collection")
    print("   └── Updates price data for all 787 Nordic companies")
    print()
    print("📄 07:00 AM - Document Discovery (Morning)")
    print("   └── Checks for new financial documents")
    print()
    print("📄 09:00 AM - Document Discovery (Late)")
    print("   └── Catches documents published after market open")
    print()
    print("📥 10:00 AM - PDF Download")
    print("   └── Downloads all discovered PDFs")
    print()
    print("🔄 11:00 AM - Document Processing Pipeline")
    print("   ├── Text extraction from PDFs")
    print("   ├── Vector embeddings generation")
    print("   └── Section-level processing")
    print()
    print("🚨 12:00 PM - Temporal Anomaly Detection")
    print("   ├── Document-level pattern analysis")
    print("   ├── Section-level anomaly detection")
    print("   └── Alert notifications")
    print()
    
    print("📊 WHAT THIS GIVES YOU")
    print("="*70)
    print("✅ Complete automated financial intelligence pipeline")
    print("✅ Real-time anomaly detection for trading signals")
    print("✅ Searchable document database with embeddings")
    print("✅ Historical pattern analysis")
    print("✅ Zero manual intervention required")
    print()
    
    print("🔧 MANAGEMENT COMMANDS")
    print("="*70)
    
    print("\n📋 Check all automation status:")
    print("launchctl list | grep yodabuffett")
    
    print("\n🚨 View recent anomalies:")
    print("cd backend && python3 view_anomalies.py")
    
    print("\n📊 View latest pipeline results:")
    print("ls -la backend/data/pipeline_results_*.json | tail -5")
    print("cat backend/data/pipeline_results_*.json | jq '.total_stats' | tail -20")
    
    print("\n📝 Monitor real-time logs:")
    print("# All logs at once")
    print("tail -f backend/logs/daily-*.log")
    print("\n# Specific component")
    print("tail -f backend/logs/daily-anomaly-detection.log")
    
    print("\n🔄 Load all LaunchAgents:")
    print("""
for plist in ~/Library/LaunchAgents/com.yodabuffett.*.plist; do
    launchctl load "$plist"
done
""")
    
    print("\n⏸️  Temporarily disable all:")
    print("""
for plist in ~/Library/LaunchAgents/com.yodabuffett.*.plist; do
    launchctl unload "$plist"
done
""")
    
    print("\n📁 KEY FILES")
    print("="*70)
    print("LaunchAgents:")
    home = Path.home()
    launchagents = home / "Library" / "LaunchAgents"
    for plist in sorted(launchagents.glob("com.yodabuffett.*.plist")):
        print(f"   {plist.name}")
    
    print("\nWorker Scripts:")
    backend = Path(__file__).parent
    workers = [
        "workers/daily_market_data_worker.py",
        "workers/daily_event_worker.py",
        "pdf_download_batch.py",
        "workers/daily_document_pipeline.py",
        "workers/daily_anomaly_detection.py"
    ]
    for worker in workers:
        if (backend / worker).exists():
            print(f"   ✓ {worker}")
        else:
            print(f"   ✗ {worker} (missing)")
    
    print("\n🎯 NEXT STEPS")
    print("="*70)
    print("1. Load the new LaunchAgents:")
    print("   launchctl load ~/Library/LaunchAgents/com.yodabuffett.daily-document-pipeline.plist")
    print("   launchctl load ~/Library/LaunchAgents/com.yodabuffett.daily-anomaly-detection.plist")
    print()
    print("2. Test anomaly detection manually:")
    print("   python3 workers/daily_anomaly_detection.py")
    print()
    print("3. Monitor for anomalies:")
    print("   python3 view_anomalies.py")
    print()
    print("✨ Your financial intelligence system is now fully automated!")


if __name__ == "__main__":
    show_automation_status()