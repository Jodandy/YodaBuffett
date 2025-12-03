#!/usr/bin/env python3
"""
Anomaly CLI - Quick commands for anomaly analysis
"""

import asyncio
import sys
import os
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from sqlalchemy import text


async def quick_stats():
    """Show quick anomaly statistics"""
    
    print("🚨 ANOMALY QUICK STATS")
    print("="*50)
    
    try:
        async with AsyncSessionLocal() as db:
            # Check if table exists
            table_exists = await db.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'temporal_anomalies'
                )
            """))
            
            if not table_exists.scalar():
                print("❌ No anomalies detected yet")
                print("💡 Run: python3 workers/daily_anomaly_detection.py")
                return
            
            # Get counts by time period
            periods = [
                ("Last 24 hours", 1),
                ("Last 3 days", 3), 
                ("Last week", 7),
                ("Last month", 30)
            ]
            
            for period_name, days in periods:
                cutoff = datetime.now() - timedelta(days=days)
                
                result = await db.execute(text("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(*) FILTER (WHERE severity = 'significant') as significant,
                        COUNT(*) FILTER (WHERE severity = 'moderate') as moderate,
                        COUNT(*) FILTER (WHERE severity = 'minor') as minor
                    FROM temporal_anomalies 
                    WHERE detected_at >= :cutoff
                """), {"cutoff": cutoff})
                
                stats = result.fetchone()
                if stats.total > 0:
                    print(f"{period_name:15} │ 🚨{stats.significant:2} ⚠️{stats.moderate:2} ℹ️{stats.minor:2} │ Total: {stats.total}")
                else:
                    print(f"{period_name:15} │ No anomalies")
            
    except Exception as e:
        print(f"❌ Error: {e}")


async def latest_significant():
    """Show latest significant anomalies"""
    
    print("🚨 LATEST SIGNIFICANT ANOMALIES")
    print("="*50)
    
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(text("""
                SELECT 
                    detected_at,
                    company_id,
                    score,
                    description,
                    metadata::text
                FROM temporal_anomalies
                WHERE severity = 'significant'
                ORDER BY detected_at DESC
                LIMIT 10
            """))
            
            anomalies = result.fetchall()
            
            if not anomalies:
                print("✅ No significant anomalies found")
                return
            
            for i, anomaly in enumerate(anomalies, 1):
                date_str = anomaly.detected_at.strftime("%Y-%m-%d %H:%M")
                print(f"\n{i:2}. {date_str} │ Score: {anomaly.score:.2f}")
                print(f"    Company: {anomaly.company_id}")
                if anomaly.description:
                    print(f"    {anomaly.description}")
                    
    except Exception as e:
        print(f"❌ Error: {e}")


async def top_companies():
    """Show companies with most anomalies"""
    
    print("🏢 COMPANIES WITH MOST ANOMALIES (Last 30 days)")
    print("="*50)
    
    try:
        async with AsyncSessionLocal() as db:
            cutoff = datetime.now() - timedelta(days=30)
            
            result = await db.execute(text("""
                SELECT 
                    company_id,
                    COUNT(*) as total_anomalies,
                    COUNT(*) FILTER (WHERE severity = 'significant') as significant,
                    MAX(score) as max_score,
                    MAX(detected_at) as latest_anomaly
                FROM temporal_anomalies
                WHERE detected_at >= :cutoff
                GROUP BY company_id
                ORDER BY total_anomalies DESC, max_score DESC
                LIMIT 15
            """), {"cutoff": cutoff})
            
            companies = result.fetchall()
            
            if not companies:
                print("❌ No anomalies found in last 30 days")
                return
            
            print(f"{'Company':<30} │ {'Total':<6} │ {'🚨Sig':<4} │ {'Max Score':<9} │ {'Latest'}")
            print("-" * 80)
            
            for company in companies:
                latest_str = company.latest_anomaly.strftime("%Y-%m-%d") if company.latest_anomaly else "Unknown"
                print(f"{company.company_id:<30} │ {company.total_anomalies:<6} │ {company.significant:<4} │ {company.max_score:<9.2f} │ {latest_str}")
                
    except Exception as e:
        print(f"❌ Error: {e}")


async def search_company(company_name):
    """Search anomalies for specific company"""
    
    print(f"🔍 ANOMALIES FOR: {company_name}")
    print("="*50)
    
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(text("""
                SELECT 
                    detected_at,
                    severity,
                    score,
                    description
                FROM temporal_anomalies
                WHERE company_id ILIKE :company_filter
                ORDER BY detected_at DESC
                LIMIT 20
            """), {"company_filter": f"%{company_name}%"})
            
            anomalies = result.fetchall()
            
            if not anomalies:
                print(f"❌ No anomalies found for '{company_name}'")
                return
            
            severity_emoji = {"significant": "🚨", "moderate": "⚠️", "minor": "ℹ️"}
            
            for anomaly in anomalies:
                date_str = anomaly.detected_at.strftime("%Y-%m-%d %H:%M")
                emoji = severity_emoji.get(anomaly.severity, "❓")
                
                print(f"{emoji} {date_str} │ {anomaly.severity:<11} │ {anomaly.score:.2f}")
                if anomaly.description:
                    print(f"   {anomaly.description}")
                print()
                
    except Exception as e:
        print(f"❌ Error: {e}")


def main():
    """Main CLI interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Anomaly Analysis CLI")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Stats command
    subparsers.add_parser('stats', help='Show quick statistics')
    
    # Latest command
    subparsers.add_parser('latest', help='Show latest significant anomalies')
    
    # Top companies command
    subparsers.add_parser('top', help='Show companies with most anomalies')
    
    # Search command
    search_parser = subparsers.add_parser('search', help='Search anomalies for specific company')
    search_parser.add_argument('company', help='Company name or ticker to search')
    
    args = parser.parse_args()
    
    if not args.command:
        # Default: show stats
        asyncio.run(quick_stats())
    elif args.command == 'stats':
        asyncio.run(quick_stats())
    elif args.command == 'latest':
        asyncio.run(latest_significant())
    elif args.command == 'top':
        asyncio.run(top_companies())
    elif args.command == 'search':
        asyncio.run(search_company(args.company))
    else:
        print("❌ Unknown command")
        parser.print_help()


if __name__ == "__main__":
    main()