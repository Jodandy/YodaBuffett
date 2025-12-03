#!/usr/bin/env python3
"""
View Temporal Anomalies
Simple dashboard to view detected anomalies
"""

import asyncio
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path
import json

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from sqlalchemy import text


async def view_recent_anomalies(days_back=7):
    """View recent temporal anomalies"""
    
    print("🔍 TEMPORAL ANOMALY DASHBOARD")
    print("="*70)
    print(f"📅 Showing anomalies from last {days_back} days")
    print()
    
    try:
        async with AsyncSessionLocal() as db:
            # Check if anomaly table exists
            table_exists = await db.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'temporal_anomalies'
                )
            """))
            
            if not table_exists.scalar():
                print("❌ No anomalies table found. Run anomaly detection first.")
                print("💡 Try: python3 workers/daily_anomaly_detection.py")
                return
            
            # Get total count first
            total_result = await db.execute(text("""
                SELECT COUNT(*) FROM temporal_anomalies
            """))
            total_anomalies = total_result.scalar()
            
            if total_anomalies == 0:
                print("📊 No anomalies have been detected yet.")
                print("💡 The system will start detecting anomalies once documents are processed.")
                return
            
            # Get recent anomalies
            recent_cutoff = datetime.now() - timedelta(days=days_back)
            
            result = await db.execute(text("""
                SELECT 
                    detected_at,
                    anomaly_type,
                    severity,
                    score,
                    company_id,
                    description,
                    metadata
                FROM temporal_anomalies
                WHERE detected_at >= :cutoff
                ORDER BY score DESC, detected_at DESC
                LIMIT 50
            """), {"cutoff": recent_cutoff})
            
            recent_anomalies = result.fetchall()
            
            # Also get stats for different time periods
            periods = [
                ("Today", 1),
                ("Last 3 days", 3),
                ("Last week", 7),
                ("Last month", 30)
            ]
            
            print("📊 ANOMALY STATISTICS")
            print("-"*50)
            for period_name, days in periods:
                cutoff = datetime.now() - timedelta(days=days)
                period_result = await db.execute(text("""
                    SELECT 
                        severity,
                        COUNT(*) as count
                    FROM temporal_anomalies 
                    WHERE detected_at >= :cutoff
                    GROUP BY severity
                    ORDER BY 
                        CASE severity 
                            WHEN 'significant' THEN 1 
                            WHEN 'moderate' THEN 2 
                            WHEN 'minor' THEN 3 
                        END
                """), {"cutoff": cutoff})
                
                period_stats = period_result.fetchall()
                if period_stats:
                    stats_str = ", ".join([f"{severity}: {count}" for severity, count in period_stats])
                    total_period = sum(count for _, count in period_stats)
                    print(f"   {period_name:12} │ {total_period:3} total ({stats_str})")
                else:
                    print(f"   {period_name:12} │   0 total")
            
            print(f"\n📊 Total anomalies in database: {total_anomalies:,}")
            print()
            
            anomalies = result.fetchall()
            
            if not anomalies:
                print("✅ No anomalies detected in the last 7 days")
                return
            
            print(f"📊 Found {len(anomalies)} anomalies in the last 7 days\n")
            
            # Group by severity
            by_severity = {"significant": [], "moderate": [], "minor": []}
            
            for anomaly in anomalies:
                by_severity[anomaly.severity].append(anomaly)
            
            # Display significant anomalies
            if by_severity["significant"]:
                print("🚨 SIGNIFICANT ANOMALIES")
                print("-"*70)
                for i, anomaly in enumerate(by_severity["significant"], 1):
                    metadata = json.loads(anomaly.metadata) if anomaly.metadata else {}
                    print(f"\n{i}. Score: {anomaly.score:.2f}")
                    print(f"   Company: {anomaly.company_id}")
                    print(f"   Type: {anomaly.anomaly_type}")
                    print(f"   Date: {anomaly.detected_at.strftime('%Y-%m-%d %H:%M')}")
                    print(f"   Description: {anomaly.description or 'N/A'}")
                    
                    if metadata.get("document_title"):
                        print(f"   Document: {metadata['document_title']}")
                    
                    if metadata.get("previous_document"):
                        print(f"   Previous: {metadata['previous_document']}")
            
            # Summary of moderate anomalies
            if by_severity["moderate"]:
                print(f"\n\n⚠️  MODERATE ANOMALIES: {len(by_severity['moderate'])} detected")
                print("-"*70)
                for anomaly in by_severity["moderate"][:5]:
                    print(f"   • {anomaly.company_id}: {anomaly.description[:60]}...")
                
                if len(by_severity["moderate"]) > 5:
                    print(f"   ... and {len(by_severity['moderate']) - 5} more")
            
            # Count of minor anomalies
            if by_severity["minor"]:
                print(f"\n\nℹ️  MINOR ANOMALIES: {len(by_severity['minor'])} detected")
            
            # Recent results files
            print("\n\n📁 RECENT RESULT FILES:")
            print("-"*70)
            
            data_dir = Path("data")
            anomaly_files = sorted(data_dir.glob("anomaly_results_*.json"), reverse=True)[:5]
            
            for f in anomaly_files:
                size_kb = f.stat().st_size / 1024
                print(f"   {f.name} ({size_kb:.1f} KB)")
            
            # Notification files
            notification_files = sorted(data_dir.glob("anomaly_notifications_*.txt"), reverse=True)[:5]
            
            if notification_files:
                print("\n📧 RECENT NOTIFICATIONS:")
                print("-"*70)
                for f in notification_files:
                    print(f"   {f.name}")
                    
                # Show latest notification
                with open(notification_files[0], 'r') as f:
                    content = f.read()
                    print(f"\n📄 Latest Notification:\n{'-'*70}")
                    print(content[:500] + "..." if len(content) > 500 else content)
                    
    except Exception as e:
        print(f"❌ Error viewing anomalies: {e}")
        import traceback
        traceback.print_exc()


async def view_anomalies_by_company(company_filter=None, days_back=30):
    """View anomalies filtered by company"""
    
    print(f"🏢 COMPANY-SPECIFIC ANOMALIES")
    if company_filter:
        print(f"🔍 Filtering for: {company_filter}")
    print("="*70)
    
    try:
        async with AsyncSessionLocal() as db:
            where_clause = "WHERE detected_at >= :cutoff"
            params = {"cutoff": datetime.now() - timedelta(days=days_back)}
            
            if company_filter:
                where_clause += " AND company_id ILIKE :company_filter"
                params["company_filter"] = f"%{company_filter}%"
            
            result = await db.execute(text(f"""
                SELECT 
                    detected_at,
                    company_id,
                    severity,
                    score,
                    description,
                    metadata
                FROM temporal_anomalies
                {where_clause}
                ORDER BY detected_at DESC, score DESC
                LIMIT 100
            """), params)
            
            anomalies = result.fetchall()
            
            if not anomalies:
                filter_text = f" matching '{company_filter}'" if company_filter else ""
                print(f"❌ No anomalies found{filter_text} in the last {days_back} days")
                return
            
            # Group by company
            by_company = {}
            for anomaly in anomalies:
                company = anomaly.company_id
                if company not in by_company:
                    by_company[company] = []
                by_company[company].append(anomaly)
            
            print(f"📊 Found {len(anomalies)} anomalies for {len(by_company)} companies")
            print()
            
            for company, company_anomalies in by_company.items():
                print(f"🏢 {company}")
                print("-" * (len(company) + 2))
                
                for anomaly in company_anomalies[:5]:  # Show top 5 per company
                    date_str = anomaly.detected_at.strftime("%Y-%m-%d %H:%M")
                    severity_emoji = {"significant": "🚨", "moderate": "⚠️", "minor": "ℹ️"}
                    emoji = severity_emoji.get(anomaly.severity, "❓")
                    
                    print(f"   {emoji} {date_str} │ {anomaly.severity} │ {anomaly.score:.2f}")
                    if anomaly.description:
                        print(f"      {anomaly.description[:80]}...")
                    
                if len(company_anomalies) > 5:
                    print(f"   ... and {len(company_anomalies) - 5} more anomalies")
                print()
                
    except Exception as e:
        print(f"❌ Error viewing company anomalies: {e}")


async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="View Temporal Anomalies")
    parser.add_argument("--days", type=int, default=7, help="Days back to search (default: 7)")
    parser.add_argument("--company", type=str, help="Filter by company name/ticker")
    parser.add_argument("--stats-only", action="store_true", help="Show only statistics")
    
    args = parser.parse_args()
    
    if args.company:
        await view_anomalies_by_company(args.company, args.days)
    else:
        await view_recent_anomalies(args.days)


if __name__ == "__main__":
    asyncio.run(main())