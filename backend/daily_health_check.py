#!/usr/bin/env python3
"""
Daily Health Check - Verify YodaBuffett data pipeline is healthy

Run daily at 6 PM to ensure:
- Price data is current
- Database is accessible
- No stale data

Usage:
    python3 daily_health_check.py
    python3 daily_health_check.py --email your@email.com  # Send alert email
"""

import asyncio
import asyncpg
import sys
from datetime import datetime, timedelta, date
from typing import Dict, List

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'

class HealthStatus:
    """Health check status tracker"""

    def __init__(self):
        self.checks: List[Dict] = []
        self.errors: List[str] = []

    def add_check(self, name: str, status: str, details: str, is_critical: bool = False):
        """Add a health check result"""
        self.checks.append({
            'name': name,
            'status': status,  # 'healthy', 'warning', 'critical'
            'details': details,
            'is_critical': is_critical
        })

        if status == 'critical' and is_critical:
            self.errors.append(f"{name}: {details}")

    def is_healthy(self) -> bool:
        """Check if overall system is healthy"""
        return len(self.errors) == 0

    def print_report(self):
        """Print health check report"""
        today = date.today()

        print("=" * 70)
        print("📊 YODABUFFETT HEALTH CHECK")
        print(f"Date: {today}")
        print(f"Time: {datetime.now().strftime('%H:%M:%S')}")
        print("=" * 70)
        print()

        for check in self.checks:
            icon = {
                'healthy': '✅',
                'warning': '⚠️ ',
                'critical': '❌'
            }.get(check['status'], '❓')

            print(f"{icon} {check['name']}")
            print(f"   {check['details']}")
            print()

        print("=" * 70)

        if self.is_healthy():
            print("✅ SYSTEM HEALTHY - All checks passed")
        else:
            print("❌ SYSTEM UNHEALTHY - Action required:")
            for error in self.errors:
                print(f"   • {error}")

        print("=" * 70)


async def check_database_connection(status: HealthStatus):
    """Check if database is accessible"""
    try:
        conn = await asyncpg.connect(DATABASE_URL, timeout=5)
        await conn.close()
        status.add_check(
            "Database Connection",
            "healthy",
            "PostgreSQL is accessible"
        )
        return True
    except Exception as e:
        status.add_check(
            "Database Connection",
            "critical",
            f"Cannot connect to database: {e}",
            is_critical=True
        )
        return False


async def check_price_data_freshness(conn: asyncpg.Connection, status: HealthStatus):
    """Check if price data is current"""
    try:
        latest_date = await conn.fetchval('SELECT MAX(date) FROM daily_price_data')

        if not latest_date:
            status.add_check(
                "Price Data",
                "critical",
                "No price data found in database",
                is_critical=True
            )
            return

        today = date.today()
        days_behind = (today - latest_date).days

        # Account for weekends
        if today.weekday() >= 5:  # Saturday or Sunday
            acceptable_lag = 2
        else:
            acceptable_lag = 1

        if days_behind <= acceptable_lag:
            status.add_check(
                "Price Data Freshness",
                "healthy",
                f"Latest: {latest_date} ({days_behind} days behind - current)"
            )
        elif days_behind <= 3:
            status.add_check(
                "Price Data Freshness",
                "warning",
                f"Latest: {latest_date} ({days_behind} days behind - acceptable)"
            )
        else:
            status.add_check(
                "Price Data Freshness",
                "critical",
                f"Latest: {latest_date} ({days_behind} days behind - STALE)",
                is_critical=True
            )

    except Exception as e:
        status.add_check(
            "Price Data Freshness",
            "critical",
            f"Error checking price data: {e}",
            is_critical=True
        )


async def check_data_volumes(conn: asyncpg.Connection, status: HealthStatus):
    """Check data volumes"""
    try:
        # Price data
        price_count = await conn.fetchval('SELECT COUNT(*) FROM daily_price_data')
        price_companies = await conn.fetchval('SELECT COUNT(DISTINCT symbol) FROM daily_price_data')

        # Financials
        financial_count = await conn.fetchval('SELECT COUNT(*) FROM financial_statements')

        # Documents
        doc_count = await conn.fetchval('SELECT COUNT(*) FROM nordic_documents')

        status.add_check(
            "Data Volumes",
            "healthy",
            f"Prices: {price_count:,} records ({price_companies} companies) | "
            f"Financials: {financial_count:,} | Documents: {doc_count:,}"
        )

    except Exception as e:
        status.add_check(
            "Data Volumes",
            "warning",
            f"Error checking volumes: {e}"
        )


async def check_recent_activity(conn: asyncpg.Connection, status: HealthStatus):
    """Check if recent data collection happened"""
    try:
        # Check companies with recent price data
        recent_threshold = date.today() - timedelta(days=7)

        recent_companies = await conn.fetchval("""
            SELECT COUNT(DISTINCT symbol) FROM daily_price_data
            WHERE date >= $1
        """, recent_threshold)

        if recent_companies > 0:
            status.add_check(
                "Recent Activity",
                "healthy",
                f"{recent_companies} companies have price data in last 7 days"
            )
        else:
            status.add_check(
                "Recent Activity",
                "warning",
                "No recent price data updates detected"
            )

    except Exception as e:
        status.add_check(
            "Recent Activity",
            "warning",
            f"Error checking recent activity: {e}"
        )


async def run_health_checks():
    """Run all health checks"""
    status = HealthStatus()

    # Check database connection
    db_ok = await check_database_connection(status)

    if not db_ok:
        status.print_report()
        return not status.is_healthy()

    # Run remaining checks
    try:
        conn = await asyncpg.connect(DATABASE_URL)

        await check_price_data_freshness(conn, status)
        await check_data_volumes(conn, status)
        await check_recent_activity(conn, status)

        await conn.close()

    except Exception as e:
        status.add_check(
            "Health Checks",
            "critical",
            f"Error running health checks: {e}",
            is_critical=True
        )

    # Print report
    status.print_report()

    # Return exit code (0 = healthy, 1 = unhealthy)
    return 0 if status.is_healthy() else 1


def main():
    """Main entry point"""
    exit_code = asyncio.run(run_health_checks())
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
