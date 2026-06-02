#!/usr/bin/env python3
"""
Business Screener Deluxe - Command Line Interface

Usage:
    python -m domains.business_screener.cli run --screen 1
    python -m domains.business_screener.cli run --screen 1 --date 2024-01-15
    python -m domains.business_screener.cli run --screen 1 --with-tier-b
    python -m domains.business_screener.cli run --all-tier-a
    python -m domains.business_screener.cli tier-b --screen 3 --limit 5
    python -m domains.business_screener.cli results --screen 1
    python -m domains.business_screener.cli results --active
    python -m domains.business_screener.cli multi-hits
    python -m domains.business_screener.cli status
    python -m domains.business_screener.cli backtest --screen 1 --start 2023-01-01 --end 2024-01-01
    python -m domains.business_screener.cli llm-status
"""

import argparse
import asyncio
import json
import sys
from datetime import datetime, date, timedelta
from typing import List, Optional
from uuid import UUID

import asyncpg

from .models.screen_definition import SCREEN_DEFINITIONS
from .repositories.screen_repository import ScreenRepository
from .screens.base import get_screen_class, get_all_screen_classes

# Import all screen implementations to register them (including new Tier B screens)
from .screens import (  # noqa: F401
    screen_01_net_nets,
    screen_02_defensive_bargains,
    screen_03_asset_plays,
    screen_04_revenue_turnarounds,
    screen_05_distressed_stable_earners,
    screen_06_garp,
    screen_07_compressed_fundamentals,
    screen_08_special_situations,
    screen_09_holding_companies,
    screen_10_sum_of_parts,
    screen_11_cannibal_companies,
    screen_12_wonderful_business,
    screen_13_crisis_bargains,
    screen_14_cyclicals,
    screen_15_stalwarts,
)

# LLM imports
from .llm import OllamaLLM, LLMService, create_llm_service

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


def parse_date(date_str: str) -> date:
    """Parse a date string in YYYY-MM-DD format."""
    return datetime.strptime(date_str, '%Y-%m-%d').date()


async def get_llm_service(conn: asyncpg.Connection) -> Optional[LLMService]:
    """Create LLM service if Ollama is available."""
    try:
        llm = OllamaLLM()
        healthy = await llm.health_check()
        if healthy:
            return await create_llm_service(conn)
        else:
            print("⚠️  Ollama not available - Tier B analysis disabled")
            return None
    except Exception as e:
        print(f"⚠️  LLM service init failed: {e}")
        return None


async def run_screen(
    screen_type: int,
    score_date: date = None,
    save: bool = True,
    with_tier_b: bool = False,
    tier_b_limit: int = 10,
    display_limit: int = 20
):
    """Run a specific screen, optionally as-of a historical date."""
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        screen_class = get_screen_class(screen_type)
        if not screen_class:
            print(f"❌ Screen {screen_type} not implemented yet")
            return []

        definition = SCREEN_DEFINITIONS.get(screen_type)
        date_str = f" as-of {score_date}" if score_date and score_date < date.today() else ""

        print(f"\n{'='*60}")
        print(f"🔍 Screen {screen_type}: {definition.name}{date_str}")
        print(f"   Tiers: {definition.tiers} | Frequency: {definition.run_frequency}")
        print(f"{'='*60}\n")

        # Initialize LLM service if Tier B is requested
        llm_service = None
        if with_tier_b and definition.tier_b_enabled:
            llm_service = await get_llm_service(conn)

        # Initialize and run the screen with optional score_date and LLM service
        screen = screen_class(conn, score_date=score_date, llm_service=llm_service)
        results = await screen.run_tier_a()

        print(f"✅ Found {len(results)} candidates\n")

        # Run Tier B analysis on top candidates if requested
        if with_tier_b and llm_service and definition.tier_b_enabled and results:
            print(f"\n🤖 Running Tier B analysis on top {min(tier_b_limit, len(results))} candidates...")
            print("-" * 60)

            for i, result in enumerate(results[:tier_b_limit]):
                company_name = result.metrics.get('company_name', 'Unknown')
                ticker = result.metrics.get('primary_ticker', 'N/A')

                print(f"\n[{i+1}/{min(tier_b_limit, len(results))}] Analyzing {ticker} - {company_name}...")

                tier_b_result = await screen.run_tier_b(
                    company_id=result.company_id,
                    company_name=company_name,
                    metrics=result.metrics
                )

                if tier_b_result and tier_b_result.get('tier_b_analysis'):
                    analysis = tier_b_result['tier_b_analysis']
                    score_adj = tier_b_result.get('tier_b_score_adjustment', 0)
                    latency = tier_b_result.get('tier_b_latency_ms', 0)

                    # Update result with Tier B data
                    result.metrics.update(tier_b_result)
                    result.tier = 'B'

                    # Only apply positive score adjustments
                    # Bad LLM analysis should never hurt a legitimate candidate
                    original_score = result.score
                    if score_adj > 0:
                        result.score = screen.clamp_score(result.score + score_adj)

                    print(f"   ✅ Analysis complete ({latency:.0f}ms)")
                    if score_adj > 0:
                        print(f"   Score adjustment: {score_adj:+.1f} → Final: {result.score:.1f}")
                    else:
                        print(f"   Score adjustment: {score_adj:+.1f} (ignored, keeping Tier A: {original_score:.1f})")

                    # Print key insights from the analysis
                    if isinstance(analysis, dict):
                        summary = analysis.get('summary', '')
                        if summary:
                            print(f"   Summary: {summary[:100]}...")
                elif tier_b_result and tier_b_result.get('tier_b_error'):
                    print(f"   ❌ Error: {tier_b_result['tier_b_error']}")
                else:
                    print(f"   ⚠️  No analysis returned")

            print("\n" + "-" * 60)

        if results:
            # Display results - use generic column for non-net-net screens
            has_ncav = 'ncav_to_price_ratio' in (results[0].metrics if results else {})

            if has_ncav:
                print(f"{'Ticker':<12} {'Company':<30} {'Score':>6} {'NCAV/Price':>10} {'Flags'}")
            else:
                print(f"{'Ticker':<12} {'Company':<30} {'Score':>6} {'Flags'}")
            print("-" * 80)

            # Use display_limit=0 to show all results
            show_count = len(results) if display_limit == 0 else min(display_limit, len(results))

            for r in results[:show_count]:
                ticker = r.metrics.get('primary_ticker', r.primary_ticker or 'N/A')[:11]
                name = r.metrics.get('company_name', r.company_name or 'N/A')[:29]
                flags_str = ', '.join(r.flags[:2]) if r.flags else ''

                if has_ncav:
                    ncav_ratio = r.metrics.get('ncav_to_price_ratio', 0)
                    ncav_str = f"{ncav_ratio:.2f}x" if ncav_ratio else "N/A"
                    print(f"{ticker:<12} {name:<30} {r.score:>6.1f} {ncav_str:>10} {flags_str[:30]}")
                else:
                    print(f"{ticker:<12} {name:<30} {r.score:>6.1f} {flags_str[:40]}")

            if display_limit > 0 and len(results) > display_limit:
                print(f"\n... and {len(results) - display_limit} more candidates")

            # Save to database (only for current date, not backtests)
            if save and (score_date is None or score_date >= date.today()):
                repo = ScreenRepository(conn)
                # Deactivate old results first
                deactivated = await repo.deactivate_old_results(screen_type)
                if deactivated:
                    print(f"\n📦 Deactivated {deactivated} old results")

                saved = await repo.save_results(results)
                print(f"💾 Saved {saved} new results to database")

                # CRITICAL: Flag cyclicals at peak on value screens (3, 4, 5)
                if screen_type in (3, 4, 5):
                    flagged = await repo.flag_cyclicals_at_peak()
                    if flagged > 0:
                        print(f"⚠️  Flagged {flagged} cyclicals at peak earnings - POTENTIAL VALUE TRAPS")
            elif save and score_date and score_date < date.today():
                print(f"\n⏳ Backtest mode - results not saved to database")

        else:
            print("No candidates found matching screen criteria.")

        return results

    finally:
        await conn.close()


async def run_all_tier_a(score_date: date = None, save: bool = True):
    """Run all Tier A enabled screens."""
    date_str = f" as-of {score_date}" if score_date else ""
    print(f"\n🚀 Running all Tier A screens{date_str}...")

    for screen_type, definition in SCREEN_DEFINITIONS.items():
        if definition.tier_a_enabled:
            screen_class = get_screen_class(screen_type)
            if screen_class:
                await run_screen(screen_type, score_date=score_date, save=save)
            else:
                print(f"⏭️  Screen {screen_type} ({definition.short_name}): Not implemented yet")


async def run_backtest(
    screen_type: int,
    start_date: date,
    end_date: date,
    frequency: str = 'monthly'
):
    """
    Run a screen across multiple historical dates.

    Args:
        screen_type: Screen number to backtest
        start_date: Start of backtest period
        end_date: End of backtest period
        frequency: 'weekly', 'monthly', or 'quarterly'
    """
    print(f"\n📊 Backtesting Screen {screen_type}")
    print(f"   Period: {start_date} to {end_date}")
    print(f"   Frequency: {frequency}")
    print("=" * 60)

    # Generate dates based on frequency
    dates = []
    current = start_date
    if frequency == 'weekly':
        delta = timedelta(days=7)
    elif frequency == 'monthly':
        delta = timedelta(days=30)
    elif frequency == 'quarterly':
        delta = timedelta(days=91)
    else:
        delta = timedelta(days=30)

    while current <= end_date:
        dates.append(current)
        current += delta

    print(f"   Running {len(dates)} snapshots...\n")

    all_results = {}
    for score_date in dates:
        results = await run_screen(screen_type, score_date=score_date, save=False)
        all_results[score_date] = results
        print()

    # Summary
    print("\n" + "=" * 60)
    print("📈 Backtest Summary")
    print("=" * 60)
    print(f"\n{'Date':<12} {'Candidates':>10} {'Avg Score':>10} {'Top Ticker':<15}")
    print("-" * 50)

    for score_date, results in all_results.items():
        if results:
            avg_score = sum(r.score for r in results) / len(results)
            top_ticker = results[0].metrics.get('primary_ticker', 'N/A')
            print(f"{score_date!s:<12} {len(results):>10} {avg_score:>10.1f} {top_ticker:<15}")
        else:
            print(f"{score_date!s:<12} {'0':>10} {'-':>10} {'-':<15}")

    # Companies that appeared most frequently
    company_counts = {}
    for results in all_results.values():
        for r in results:
            ticker = r.metrics.get('primary_ticker', 'Unknown')
            company_counts[ticker] = company_counts.get(ticker, 0) + 1

    if company_counts:
        print("\n📋 Most Frequent Candidates:")
        sorted_companies = sorted(company_counts.items(), key=lambda x: -x[1])[:10]
        for ticker, count in sorted_companies:
            pct = count / len(dates) * 100
            print(f"   {ticker:<15} appeared {count:>3} times ({pct:.0f}% of snapshots)")


async def show_results(screen_type: int = None, active_only: bool = True, limit: int = 50):
    """Show screen results."""
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        repo = ScreenRepository(conn)

        if screen_type:
            results = await repo.get_results_by_screen(screen_type, active_only, limit)
            definition = SCREEN_DEFINITIONS.get(screen_type)
            print(f"\n📋 Results for Screen {screen_type}: {definition.name if definition else 'Unknown'}")
        else:
            results = await repo.get_all_active_results(limit)
            print(f"\n📋 All active screen results")

        if not results:
            print("No results found.")
            return

        print(f"\n{'Screen':<8} {'Ticker':<12} {'Company':<25} {'Score':>6} {'Tier':<4} {'Triggered'}")
        print("-" * 80)

        for r in results:
            ticker = r.primary_ticker or 'N/A'
            name = (r.company_name or 'N/A')[:24]
            triggered = r.triggered_at.strftime('%Y-%m-%d %H:%M') if r.triggered_at else 'N/A'

            print(f"{r.screen_type:<8} {ticker:<12} {name:<25} {r.score:>6.1f} {r.tier:<4} {triggered}")

        print(f"\nTotal: {len(results)} results")

    finally:
        await conn.close()


async def show_multi_hits(min_screens: int = 2):
    """Show companies triggering multiple screens."""
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        repo = ScreenRepository(conn)
        hits = await repo.get_multi_screen_hits(min_screens)

        print(f"\n🎯 Companies triggering {min_screens}+ screens\n")

        if not hits:
            print("No multi-screen hits found.")
            return

        print(f"{'Ticker':<12} {'Company':<25} {'Screens':>8} {'Avg Score':>10} {'Screen Types'}")
        print("-" * 80)

        for h in hits:
            ticker = h['primary_ticker'] or 'N/A'
            name = (h['company_name'] or 'N/A')[:24]
            screens = h['screens_triggered']
            avg_score = h['avg_score'] or 0
            screen_types = ', '.join(str(s) for s in h['screen_types'])

            print(f"{ticker:<12} {name:<25} {screens:>8} {avg_score:>10.1f} [{screen_types}]")

        print(f"\nTotal: {len(hits)} companies")

    finally:
        await conn.close()


async def show_status():
    """Show screen system status."""
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        repo = ScreenRepository(conn)
        stats = await repo.get_stats()

        print("\n📊 Business Screener Deluxe - Status\n")
        print(f"Total results:     {stats['total_results']}")
        print(f"Active results:    {stats['active_results']}")
        print(f"Unique companies:  {stats['unique_companies']}")
        print(f"Last run:          {stats['last_run'] or 'Never'}")

        print("\n📋 Screen implementations:")
        implemented = get_all_screen_classes()
        for screen_type, definition in SCREEN_DEFINITIONS.items():
            status = "✅" if screen_type in implemented else "⬜"
            by_screen = next((s for s in stats['by_screen'] if s['screen_type'] == screen_type), None)
            count = by_screen['result_count'] if by_screen else 0
            avg = by_screen['avg_score'] if by_screen and by_screen['avg_score'] else 0

            print(f"  {status} {screen_type:>2}. {definition.short_name:<15} "
                  f"Tiers: {definition.tiers:<5} "
                  f"Results: {count:>4} "
                  f"Avg: {avg:>5.1f}")

    finally:
        await conn.close()


async def show_definitions():
    """Show all screen definitions."""
    print("\n📖 Business Screener Deluxe - Screen Definitions\n")

    print(f"{'#':<3} {'Name':<30} {'Tiers':<8} {'Freq':<10} {'Description'}")
    print("-" * 90)

    for screen_type, d in SCREEN_DEFINITIONS.items():
        print(f"{screen_type:<3} {d.name:<30} {d.tiers:<8} {d.run_frequency:<10} {d.description[:35]}")


async def run_tier_b_analysis(
    screen_type: int,
    limit: int = 5,
    company_id: str = None,
    save: bool = True
):
    """
    Run Tier B LLM analysis on existing Tier A candidates.

    This is useful for:
    - Running LLM analysis on previously identified candidates
    - Testing LLM prompts on specific companies
    - Batch processing candidates with local LLM
    """
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        # Check if screen has Tier B enabled
        definition = SCREEN_DEFINITIONS.get(screen_type)
        if not definition:
            print(f"❌ Unknown screen type: {screen_type}")
            return

        if not definition.tier_b_enabled:
            print(f"❌ Screen {screen_type} ({definition.name}) does not have Tier B enabled")
            return

        print(f"\n{'='*60}")
        print(f"🤖 Tier B Analysis: Screen {screen_type} - {definition.name}")
        print(f"{'='*60}\n")

        # Initialize LLM service
        llm_service = await get_llm_service(conn)
        if not llm_service:
            print("❌ Cannot run Tier B analysis without LLM service")
            return

        # Get candidates to analyze
        repo = ScreenRepository(conn)

        if company_id:
            # Analyze specific company
            try:
                uuid_company = UUID(company_id)
                results = [await repo.get_result_by_company(screen_type, uuid_company)]
                results = [r for r in results if r]
            except ValueError:
                # Try to find by ticker
                result = await repo.get_result_by_ticker(screen_type, company_id)
                results = [result] if result else []

            if not results:
                print(f"❌ No Tier A result found for company: {company_id}")
                return
        else:
            # Get top candidates from Tier A results
            results = await repo.get_results_by_screen(screen_type, active_only=True, limit=limit)

        if not results:
            print("❌ No Tier A candidates found. Run Tier A first:")
            print(f"   python -m domains.business_screener.cli run --screen {screen_type}")
            return

        print(f"📋 Analyzing {len(results)} candidates...\n")

        # Initialize the screen for LLM analysis
        screen_class = get_screen_class(screen_type)
        screen = screen_class(conn, llm_service=llm_service)

        analyzed = 0
        for i, result in enumerate(results):
            company_name = result.metrics.get('company_name') or result.company_name or 'Unknown'
            ticker = result.metrics.get('primary_ticker') or result.primary_ticker or 'N/A'

            print(f"\n[{i+1}/{len(results)}] 🔍 {ticker} - {company_name}")
            print(f"   Tier A Score: {result.score:.1f}")

            # Run Tier B analysis
            tier_b_result = await screen.run_tier_b(
                company_id=result.company_id,
                company_name=company_name,
                metrics=result.metrics
            )

            if tier_b_result and tier_b_result.get('tier_b_analysis'):
                analysis = tier_b_result['tier_b_analysis']
                score_adj = tier_b_result.get('tier_b_score_adjustment', 0)
                latency = tier_b_result.get('tier_b_latency_ms', 0)
                model = tier_b_result.get('tier_b_model', 'unknown')

                # Only apply positive score adjustments
                # Bad LLM analysis should never hurt a legitimate candidate
                if score_adj > 0:
                    new_score = screen.clamp_score(result.score + score_adj)
                else:
                    new_score = result.score

                print(f"   ✅ Analysis complete")
                print(f"   Model: {model} | Latency: {latency:.0f}ms")
                if score_adj > 0:
                    print(f"   Score: {result.score:.1f} {score_adj:+.1f} → {new_score:.1f}")
                else:
                    print(f"   Score: {result.score:.1f} (adj {score_adj:+.1f} ignored, keeping Tier A)")

                # Print key insights
                if isinstance(analysis, dict):
                    # Print a few key fields based on screen type
                    for key in ['compression_type', 'special_situation_type', 'implied_discount',
                                'margin_recovery_potential', 'is_actionable', 'confidence']:
                        if key in analysis:
                            print(f"   {key}: {analysis[key]}")

                    summary = analysis.get('summary', '')
                    if summary:
                        # Wrap long summary
                        wrapped = summary[:150] + '...' if len(summary) > 150 else summary
                        print(f"   Summary: {wrapped}")

                analyzed += 1

                # Update result in database if save is enabled
                if save:
                    result.tier = 'B'
                    result.score = new_score
                    result.metrics.update(tier_b_result)
                    await repo.update_result(result)

            elif tier_b_result and tier_b_result.get('tier_b_error'):
                print(f"   ❌ Error: {tier_b_result['tier_b_error']}")
            else:
                print(f"   ⚠️  No analysis returned")

        print(f"\n{'='*60}")
        print(f"✅ Completed Tier B analysis on {analyzed}/{len(results)} candidates")
        if save:
            print(f"💾 Results saved to database")
        print(f"{'='*60}\n")

    finally:
        await conn.close()


async def show_llm_status():
    """Show LLM infrastructure status."""
    print("\n🤖 LLM Infrastructure Status\n")

    # Check Ollama
    print("Tier B (Local LLM - Ollama):")
    try:
        llm = OllamaLLM()
        healthy = await llm.health_check()
        if healthy:
            print(f"  ✅ Ollama is running")
            print(f"     Model: {llm.model}")
            print(f"     URL: {llm.base_url}")
        else:
            print(f"  ❌ Ollama health check failed")
            print(f"     Try: ollama serve")
    except Exception as e:
        print(f"  ❌ Ollama not available: {e}")
        print(f"     Install: brew install ollama")
        print(f"     Start: ollama serve")
        print(f"     Pull model: ollama pull llama3.1:8b")

    # Check Claude API
    print("\nTier C (API LLM - Claude):")
    import os
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if api_key:
        print(f"  ✅ ANTHROPIC_API_KEY is set")
        print(f"     Key prefix: {api_key[:8]}...")
    else:
        print(f"  ⚠️  ANTHROPIC_API_KEY not set")
        print(f"     Set with: export ANTHROPIC_API_KEY=your-key")
        print(f"     (Optional - only needed for Tier C deep analysis)")

    # Show screen Tier B support
    print("\n📋 Screens with Tier B support:")
    for screen_type, d in SCREEN_DEFINITIONS.items():
        if d.tier_b_enabled:
            tier_a_status = "A+" if d.tier_a_enabled else "   "
            tier_c_status = "+C" if d.tier_c_enabled else "  "
            print(f"  {screen_type:>2}. {d.short_name:<15} [{tier_a_status}B{tier_c_status}]")


def main():
    parser = argparse.ArgumentParser(
        description='Business Screener Deluxe CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run screen for current date
  python -m domains.business_screener.cli run --screen 1

  # Run screen with Tier B LLM analysis (requires Ollama)
  python -m domains.business_screener.cli run --screen 3 --with-tier-b

  # Run screen as-of a historical date (backtest single date)
  python -m domains.business_screener.cli run --screen 1 --date 2024-01-15

  # Run all Tier A screens
  python -m domains.business_screener.cli run --all-tier-a

  # Run Tier B analysis on existing candidates
  python -m domains.business_screener.cli tier-b --screen 3 --limit 5

  # Analyze specific company
  python -m domains.business_screener.cli tier-b --screen 3 --company VOLV-B

  # Backtest a screen over a date range
  python -m domains.business_screener.cli backtest --screen 1 --start 2023-01-01 --end 2024-01-01

  # View results
  python -m domains.business_screener.cli results --screen 1
  python -m domains.business_screener.cli results --active

  # View multi-screen hits and status
  python -m domains.business_screener.cli multi-hits
  python -m domains.business_screener.cli status
  python -m domains.business_screener.cli definitions
  python -m domains.business_screener.cli llm-status
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to run')

    # Run command
    run_parser = subparsers.add_parser('run', help='Run screens')
    run_parser.add_argument('--screen', type=int, help='Screen number (1-15)')
    run_parser.add_argument('--all-tier-a', action='store_true', help='Run all Tier A screens')
    run_parser.add_argument('--date', type=str, help='Run as-of date (YYYY-MM-DD) for backtesting')
    run_parser.add_argument('--no-save', action='store_true', help='Do not save results to DB')
    run_parser.add_argument('--with-tier-b', action='store_true',
                            help='Also run Tier B LLM analysis on top candidates')
    run_parser.add_argument('--tier-b-limit', type=int, default=10,
                            help='Max candidates for Tier B analysis (default: 10)')
    run_parser.add_argument('--limit', type=int, default=20,
                            help='Max results to display (0 = show all, default: 20)')

    # Tier B command (standalone)
    tier_b_parser = subparsers.add_parser('tier-b', help='Run Tier B LLM analysis on existing candidates')
    tier_b_parser.add_argument('--screen', type=int, required=True, help='Screen number (1-15)')
    tier_b_parser.add_argument('--limit', type=int, default=5, help='Max candidates to analyze (default: 5)')
    tier_b_parser.add_argument('--company', type=str, help='Specific company ID or ticker to analyze')
    tier_b_parser.add_argument('--no-save', action='store_true', help='Do not save results to DB')

    # Backtest command
    backtest_parser = subparsers.add_parser('backtest', help='Backtest screen over date range')
    backtest_parser.add_argument('--screen', type=int, required=True, help='Screen number (1-15)')
    backtest_parser.add_argument('--start', type=str, required=True, help='Start date (YYYY-MM-DD)')
    backtest_parser.add_argument('--end', type=str, required=True, help='End date (YYYY-MM-DD)')
    backtest_parser.add_argument('--frequency', type=str, default='monthly',
                                  choices=['weekly', 'monthly', 'quarterly'],
                                  help='Snapshot frequency')

    # Results command
    results_parser = subparsers.add_parser('results', help='View results')
    results_parser.add_argument('--screen', type=int, help='Filter by screen number')
    results_parser.add_argument('--active', action='store_true', help='Show all active results')
    results_parser.add_argument('--limit', type=int, default=50, help='Max results to show')

    # Multi-hits command
    multi_parser = subparsers.add_parser('multi-hits', help='Companies triggering multiple screens')
    multi_parser.add_argument('--min', type=int, default=2, help='Minimum screens triggered')

    # Status command
    subparsers.add_parser('status', help='Show system status')

    # Definitions command
    subparsers.add_parser('definitions', help='Show screen definitions')

    # LLM status command
    subparsers.add_parser('llm-status', help='Show LLM infrastructure status')

    args = parser.parse_args()

    if args.command == 'run':
        score_date = parse_date(args.date) if args.date else None
        if args.screen:
            asyncio.run(run_screen(
                args.screen,
                score_date=score_date,
                save=not args.no_save,
                with_tier_b=args.with_tier_b,
                tier_b_limit=args.tier_b_limit,
                display_limit=args.limit
            ))
        elif args.all_tier_a:
            asyncio.run(run_all_tier_a(score_date=score_date, save=not args.no_save))
        else:
            print("Please specify --screen NUMBER or --all-tier-a")
            sys.exit(1)

    elif args.command == 'tier-b':
        asyncio.run(run_tier_b_analysis(
            screen_type=args.screen,
            limit=args.limit,
            company_id=args.company,
            save=not args.no_save
        ))

    elif args.command == 'backtest':
        start = parse_date(args.start)
        end = parse_date(args.end)
        asyncio.run(run_backtest(args.screen, start, end, args.frequency))

    elif args.command == 'results':
        asyncio.run(show_results(args.screen, active_only=True, limit=args.limit))

    elif args.command == 'multi-hits':
        asyncio.run(show_multi_hits(args.min))

    elif args.command == 'status':
        asyncio.run(show_status())

    elif args.command == 'definitions':
        asyncio.run(show_definitions())

    elif args.command == 'llm-status':
        asyncio.run(show_llm_status())

    else:
        parser.print_help()


if __name__ == '__main__':
    main()
