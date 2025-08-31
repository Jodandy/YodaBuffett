#!/usr/bin/env python3
"""
Slow PDF Download - Parallel Background Downloader
Ultra-slow downloader for running in parallel with other operations

Usage:
  python3 slow_pdf_download.py --year 2025
  python3 slow_pdf_download.py --year 2024 --company "Hexagon"
  
Rate: 1 PDF per minute (60 seconds between downloads)
Perfect for running in background while doing other work
"""
import asyncio
import sys
import os
from pdf_download_batch import PDFDownloadBatch

async def main():
    """Main entry point for slow download"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Slow PDF Download - 1 PDF per minute")
    parser.add_argument("--year", type=int, help="Target year for downloads (default: current year)")
    parser.add_argument("--company", type=str, help="Target company name or ticker (default: all companies)")
    parser.add_argument("--delay", type=int, default=60, help="Delay between downloads in seconds (default: 60 = 1 PDF/minute)")
    
    args = parser.parse_args()
    
    print(f"🐌 Starting SLOW PDF Download")
    print(f"⏰ Rate: 1 PDF every {args.delay} seconds ({60/args.delay:.1f} PDFs per minute)")
    print(f"🎯 Perfect for parallel background downloading")
    print(f"🔥 Filters: Year {args.year or 'current'}, Company: {args.company or 'all'}")
    print(f"{'='*60}")
    
    if args.delay < 30:
        confirm = input(f"⚠️  Delay is {args.delay}s - this is not very slow. Continue? (y/n): ")
        if confirm.lower() not in ['y', 'yes']:
            print("Cancelled")
            return
    
    batch_processor = PDFDownloadBatch(
        target_year=args.year,
        target_company=args.company,
        download_delay=args.delay
    )
    
    await batch_processor.run_batch_download()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Slow download interrupted by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()