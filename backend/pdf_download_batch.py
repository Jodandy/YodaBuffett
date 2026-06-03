#!/usr/bin/env python3
"""
PDF Download Batch Processor
Systematically downloads catalogued PDF documents with smart filtering

Features:
- Filter by year and company for targeted downloads
- 10-second wait between PDF downloads (respectful)
- 5-minute timeout per PDF download
- Comprehensive success/failure tracking
- Resume capability for interrupted sessions
- File validation and deduplication
- Storage organization by company/year/type
"""
import asyncio
import aiohttp
import sys
import os
import json
import time
import signal
import hashlib
from datetime import datetime, date
from typing import Dict, List, Set, Optional
from pathlib import Path
import logging

# Disable SQLAlchemy logging noise
logging.getLogger('sqlalchemy').setLevel(logging.CRITICAL)
logging.getLogger('sqlalchemy.engine').setLevel(logging.CRITICAL)
logging.getLogger('sqlalchemy.pool').setLevel(logging.CRITICAL)

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicDocument, NordicCompany
from sqlalchemy import select, update, and_, or_

class PDFDownloadBatch:
    """
    Batch processor for downloading catalogued PDF documents
    """
    
    def __init__(self, target_year: Optional[int] = None, target_company: Optional[str] = None, download_delay: int = 10, reports_only: bool = True):
        self.start_time = datetime.now()
        self.session_id = self.start_time.strftime("%Y%m%d_%H%M%S")
        
        # Filters
        self.target_year = target_year  # None means all years
        self.target_company = target_company  # Company name or None for all
        self.reports_only = reports_only  # Only annual/quarterly reports
        
        # File paths
        self.results_file = f"pdf_download_{self.session_id}.json"
        self.log_file = f"pdf_download_{self.session_id}.log"
        
        # Download settings
        self.pdf_timeout = 300  # 5 minutes per PDF
        self.download_delay = download_delay  # Configurable delay between downloads
        
        # Storage paths
        self.base_storage_path = Path("data/companies")
        self.base_storage_path.mkdir(parents=True, exist_ok=True)
        
        # Results tracking
        self.results = {
            "session_id": self.session_id,
            "start_time": self.start_time.isoformat(),
            "filters": {
                "target_year": self.target_year,
                "target_company": self.target_company,
                "reports_only": self.reports_only
            },
            "downloaded": [],
            "failed": [],
            "skipped": [],
            "in_progress": None,
            "stats": {
                "total_documents": 0,
                "downloaded_count": 0,
                "failed_count": 0,
                "skipped_count": 0,
                "total_size_mb": 0.0,
                "processing_time_seconds": 0,
                "download_speed_mbps": 0.0
            }
        }
        
        # Graceful shutdown handling
        self.shutdown_requested = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def _signal_handler(self, signum, frame):
        """Handle graceful shutdown"""
        self.logger.info(f"🛑 Shutdown signal received ({signum})")
        self.shutdown_requested = True
        
    def load_previous_results(self) -> bool:
        """Load results from previous run to enable resume"""
        try:
            # Look for most recent results file
            import glob
            result_files = glob.glob("pdf_download_*.json")
            if not result_files:
                return False
                
            latest_file = max(result_files, key=os.path.getctime)
            
            with open(latest_file, 'r') as f:
                previous_results = json.load(f)
                
            # Ask user if they want to resume
            downloaded_count = len(previous_results.get('downloaded', []))
            failed_count = len(previous_results.get('failed', []))
            
            print(f"📄 Found previous run: {latest_file}")
            print(f"   ✅ Downloaded: {downloaded_count}")
            print(f"   ❌ Failed: {failed_count}")
            
            resume = input("🔄 Resume from previous run? (y/n): ").lower().strip()
            if resume in ['y', 'yes']:
                # Load previous results but start fresh session
                downloaded_doc_ids = set(previous_results.get('downloaded', []))
                failed_doc_ids = set(previous_results.get('failed', []))
                
                # Skip documents that were already downloaded
                self.results['skipped'] = list(downloaded_doc_ids)
                self.results['stats']['skipped_count'] = len(downloaded_doc_ids)
                
                # Could retry failed downloads or skip them
                retry_failed = input("🔄 Retry previously failed downloads? (y/n): ").lower().strip()
                if retry_failed not in ['y', 'yes']:
                    self.results['skipped'].extend(failed_doc_ids)
                    self.results['stats']['skipped_count'] += len(failed_doc_ids)
                
                self.logger.info(f"📄 Resuming: {len(self.results['skipped'])} documents skipped")
                return True
                
        except Exception as e:
            self.logger.error(f"❌ Error loading previous results: {e}")
            
        return False
        
    def save_results(self):
        """Save current results to file"""
        try:
            self.results['stats']['processing_time_seconds'] = (datetime.now() - self.start_time).total_seconds()
            
            with open(self.results_file, 'w') as f:
                json.dump(self.results, f, indent=2, default=str)
                
            self.logger.info(f"💾 Results saved to {self.results_file}")
            
        except Exception as e:
            self.logger.error(f"❌ Error saving results: {e}")
    
    async def load_documents_to_download(self) -> List[Dict]:
        """Load documents that need to be downloaded based on filters"""
        try:
            async with AsyncSessionLocal() as db:
                from sqlalchemy import text

                # Use raw SQL to join with company_master (source of truth)
                # Documents may have company_id from either nordic_companies or company_master
                base_query = """
                    SELECT
                        nd.id,
                        nd.title,
                        nd.document_type,
                        nd.publish_date,
                        nd.storage_path,
                        nd.metadata as metadata_,
                        nd.processing_status,
                        COALESCE(cm.company_name, nc.name) as company_name,
                        COALESCE(cm.primary_ticker, nc.ticker) as ticker
                    FROM nordic_documents nd
                    LEFT JOIN company_master cm ON nd.company_id = cm.id
                    LEFT JOIN nordic_companies nc ON nd.company_id = nc.id
                    WHERE nd.processing_status = 'catalogued'
                """

                conditions = []
                params = {}

                # Apply document type filter
                if self.reports_only:
                    conditions.append("nd.document_type IN ('annual_report', 'quarterly_report')")

                # Apply year filter
                if self.target_year:
                    conditions.append("""
                        (nd.publish_date BETWEEN :year_start AND :year_end
                         OR nd.title LIKE :year_pattern)
                    """)
                    params['year_start'] = date(self.target_year, 1, 1)
                    params['year_end'] = date(self.target_year, 12, 31)
                    params['year_pattern'] = f"%{self.target_year}%"

                # Apply company filter
                if self.target_company:
                    conditions.append("""
                        (COALESCE(cm.company_name, nc.name) ILIKE :company_pattern
                         OR COALESCE(cm.primary_ticker, nc.ticker) ILIKE :company_pattern)
                    """)
                    params['company_pattern'] = f"%{self.target_company}%"

                # Build final query
                if conditions:
                    base_query += " AND " + " AND ".join(conditions)

                base_query += " ORDER BY COALESCE(cm.company_name, nc.name), nd.publish_date DESC"

                result = await db.execute(text(base_query), params)
                documents = result.fetchall()

                self.logger.info(f"📊 Found {len(documents)} documents to download")

                # Convert to dictionaries
                docs_to_download = []
                for doc in documents:
                    # Extract PDF URL from metadata (handle both dict and JSON string)
                    metadata = doc.metadata_
                    if isinstance(metadata, str):
                        import json
                        metadata = json.loads(metadata)
                    pdf_url = metadata.get('pdf_url') if metadata else None
                    if not pdf_url:
                        continue
                    
                    docs_to_download.append({
                        'id': str(doc.id),
                        'title': doc.title,
                        'company_name': doc.company_name,
                        'ticker': doc.ticker,
                        'document_type': doc.document_type,
                        'publish_date': doc.publish_date.isoformat() if doc.publish_date else None,
                        'pdf_url': pdf_url,
                        'year': doc.publish_date.year if doc.publish_date else self.target_year
                    })
                
                self.logger.info(f"✅ {len(docs_to_download)} documents have PDF URLs")
                
                # Show sample
                if docs_to_download:
                    self.logger.info(f"📄 Sample documents:")
                    for i, doc in enumerate(docs_to_download[:5]):
                        self.logger.info(f"   {i+1}. {doc['company_name']}: {doc['title'][:50]}...")
                    if len(docs_to_download) > 5:
                        self.logger.info(f"   ... and {len(docs_to_download) - 5} more documents")
                
                return docs_to_download
                
        except Exception as e:
            self.logger.error(f"❌ Error loading documents: {e}")
            return []
    
    def _generate_storage_path(self, document: Dict) -> Path:
        """Generate organized storage path for document"""
        company_name = document['company_name']
        year = document['year']
        doc_type = document['document_type']
        
        # Clean company name for filesystem
        company_clean = "".join(c for c in company_name if c.isalnum() or c in (' ', '-', '_')).strip()
        company_clean = company_clean.replace(' ', '_')
        
        # Organize by: companies/SE/{first_letter}/{company}/{year}/{doc_type}/
        first_letter = company_clean[0].upper() if company_clean else 'Z'
        
        storage_path = (
            self.base_storage_path / 
            "SE" / 
            first_letter / 
            company_clean / 
            str(year) / 
            doc_type
        )
        
        storage_path.mkdir(parents=True, exist_ok=True)
        return storage_path
    
    def _generate_filename(self, document: Dict, original_url: str) -> str:
        """Generate clean, descriptive filename"""
        title = document['title']
        publish_date = document.get('publish_date')
        
        # Clean title for filename
        title_clean = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).strip()
        title_clean = title_clean.replace(' ', '-').lower()[:50]  # Limit length
        
        # Add date prefix if available
        filename = title_clean
        if publish_date:
            try:
                date_obj = datetime.fromisoformat(publish_date.replace('Z', '+00:00'))
                date_prefix = date_obj.strftime("%Y-%m-%d")
                filename = f"{date_prefix}-{filename}"
            except:
                pass
        
        # Ensure .pdf extension
        if not filename.endswith('.pdf'):
            filename += '.pdf'
            
        return filename
    
    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of file for deduplication"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def _validate_pdf(self, file_path: Path) -> bool:
        """Validate that downloaded file is a valid PDF"""
        try:
            # Check file size (should be > 1KB)
            if file_path.stat().st_size < 1024:
                return False
            
            # Check PDF magic bytes
            with open(file_path, 'rb') as f:
                header = f.read(8)
                if not header.startswith(b'%PDF-'):
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ PDF validation error: {e}")
            return False
    
    async def download_document(
        self, 
        session: aiohttp.ClientSession, 
        document: Dict
    ) -> Dict[str, any]:
        """
        Download a single PDF document with validation
        
        Returns:
            Dict with download results
        """
        start_time = time.time()
        doc_id = document['id']
        pdf_url = document['pdf_url']
        
        try:
            company_name = f"{document['company_name']} ({document['ticker']})"
            
            self.logger.info(f"📄 Downloading: {company_name} - {document['title'][:50]}...")
            self.results['in_progress'] = doc_id
            self.save_results()
            
            # Generate storage path and filename
            storage_path = self._generate_storage_path(document)
            filename = self._generate_filename(document, pdf_url)
            file_path = storage_path / filename
            
            # Check if file already exists (deduplication)
            if file_path.exists():
                if self._validate_pdf(file_path):
                    # Update database to mark as downloaded (was missing!)
                    file_size_mb = file_path.stat().st_size / (1024 * 1024)
                    file_hash = self._calculate_file_hash(file_path)
                    
                    async with AsyncSessionLocal() as db:
                        await db.execute(
                            update(NordicDocument)
                            .where(NordicDocument.id == doc_id)
                            .values(
                                processing_status="downloaded",
                                storage_path=str(file_path),
                                file_hash=file_hash,
                                file_size_mb=file_size_mb
                            )
                        )
                        await db.commit()
                    
                    processing_time = time.time() - start_time
                    self.logger.info(f"✅ {company_name}: Already exists, updated DB ({file_size_mb:.1f}MB)")
                    
                    return {
                        "document_id": doc_id,
                        "company_name": company_name,
                        "title": document['title'],
                        "success": True,
                        "error": None,
                        "failure_reason": None,
                        "file_path": str(file_path),
                        "file_size_mb": file_size_mb,
                        "processing_time": processing_time,
                        "status": "already_exists",
                        "skipped": True  # Mark as skipped for better tracking
                    }
                else:
                    # Invalid existing file, remove and re-download
                    self.logger.warning(f"⚠️  Invalid PDF exists, removing: {file_path}")
                    file_path.unlink()
            
            # Download PDF with timeout
            try:
                async with asyncio.timeout(self.pdf_timeout):
                    async with session.get(pdf_url) as response:
                        if response.status != 200:
                            return {
                                "document_id": doc_id,
                                "company_name": company_name,
                                "title": document['title'],
                                "success": False,
                                "error": f"HTTP {response.status}: {response.reason}",
                                "failure_reason": "http_error",
                                "file_path": None,
                                "file_size_mb": 0,
                                "processing_time": time.time() - start_time
                            }
                        
                        # Stream download to file
                        total_size = 0
                        with open(file_path, 'wb') as f:
                            async for chunk in response.content.iter_chunked(8192):
                                f.write(chunk)
                                total_size += len(chunk)
                        
                        file_size_mb = total_size / (1024 * 1024)
                        
            except asyncio.TimeoutError:
                return {
                    "document_id": doc_id,
                    "company_name": company_name,
                    "title": document['title'],
                    "success": False,
                    "error": f"Download timeout ({self.pdf_timeout}s)",
                    "failure_reason": "timeout",
                    "file_path": None,
                    "file_size_mb": 0,
                    "processing_time": time.time() - start_time
                }
            
            # Validate downloaded PDF
            if not self._validate_pdf(file_path):
                file_path.unlink()  # Remove invalid file
                return {
                    "document_id": doc_id,
                    "company_name": company_name,
                    "title": document['title'],
                    "success": False,
                    "error": "Invalid PDF file (failed validation)",
                    "failure_reason": "invalid_pdf",
                    "file_path": None,
                    "file_size_mb": 0,
                    "processing_time": time.time() - start_time
                }
            
            # Calculate file hash for deduplication
            file_hash = self._calculate_file_hash(file_path)
            
            # Update database with download success
            async with AsyncSessionLocal() as db:
                await db.execute(
                    update(NordicDocument)
                    .where(NordicDocument.id == doc_id)
                    .values(
                        processing_status="downloaded",
                        storage_path=str(file_path),
                        file_hash=file_hash,
                        file_size_mb=file_size_mb,
                        page_count=None  # Will be set later if needed
                    )
                )
                await db.commit()
            
            processing_time = time.time() - start_time
            
            result = {
                "document_id": doc_id,
                "company_name": company_name,
                "title": document['title'],
                "success": True,
                "error": None,
                "failure_reason": None,
                "file_path": str(file_path),
                "file_size_mb": file_size_mb,
                "file_hash": file_hash,
                "processing_time": processing_time,
                "download_speed_mbps": file_size_mb / processing_time if processing_time > 0 else 0,
                "status": "downloaded"
            }
            
            self.logger.info(f"✅ {company_name}: {file_size_mb:.1f}MB downloaded ({processing_time:.1f}s)")
            return result
            
        except Exception as e:
            processing_time = time.time() - start_time
            self.logger.error(f"❌ {company_name}: Download error: {e}")
            
            return {
                "document_id": doc_id,
                "company_name": company_name,
                "title": document['title'],
                "success": False,
                "error": str(e),
                "failure_reason": "unexpected_error",
                "file_path": None,
                "file_size_mb": 0,
                "processing_time": processing_time
            }
    
    async def run_batch_download(self):
        """
        Run the complete batch download process
        """
        filter_desc = f"Year: {self.target_year if self.target_year else 'All years'}" + (f", Company: {self.target_company}" if self.target_company else ", All companies") + (f", Document Types: {'Annual & Quarterly Reports Only' if self.reports_only else 'All Document Types'}")
        
        self.logger.info(f"🚀 Starting PDF Download Batch - {'REPORTS FOCUS' if self.reports_only else 'ALL DOCUMENTS'}")
        self.logger.info(f"📊 Filters: {filter_desc}")
        self.logger.info(f"📋 Priority: {'HIGH-PRIORITY financial reports only' if self.reports_only else 'All document types'}")
        self.logger.info(f"⏱️  5-minute timeout per PDF (safety measure)")
        self.logger.info(f"⏳ {self.download_delay}-second delay between downloads ({'super slow' if self.download_delay >= 60 else 'respectful'})")
        self.logger.info(f"💾 Results will be saved to {self.results_file}")
        
        # Load documents to download
        documents_to_download = await self.load_documents_to_download()
        
        if not documents_to_download:
            self.logger.info("📄 No documents found matching filters")
            return
        
        # Load previous results if available
        self.load_previous_results()
        
        self.results['stats']['total_documents'] = len(documents_to_download)
        
        # Get documents to process (excluding skipped ones)
        skipped_set = set(self.results['skipped'])
        documents_to_process = [d for d in documents_to_download if d['id'] not in skipped_set]
        
        self.logger.info(f"📋 Processing {len(documents_to_process)} documents (skipping {len(skipped_set)})")
        
        async with aiohttp.ClientSession(
            headers={
                'User-Agent': 'YodaBuffett-Research/1.0 (Financial Research; +https://yodabuffett.com)',
                'Accept': 'application/pdf,*/*;q=0.9',
            },
            timeout=aiohttp.ClientTimeout(total=self.pdf_timeout + 60)
        ) as session:
            
            for i, document in enumerate(documents_to_process, 1):
                if self.shutdown_requested:
                    self.logger.info("🛑 Shutdown requested, stopping batch download")
                    break
                
                company_display = f"{document['company_name']} ({document['ticker']})"
                self.logger.info(f"\n{'='*80}")
                self.logger.info(f"📄 [{i}/{len(documents_to_process)}] {company_display}")
                self.logger.info(f"🔗 Title: {document['title'][:60]}...")
                self.logger.info(f"📅 Date: {document.get('publish_date', 'Unknown')}")
                self.logger.info(f"🔗 URL: {document['pdf_url']}")
                self.logger.info(f"{'='*80}")
                
                try:
                    # Download with timeout
                    result = await asyncio.wait_for(
                        self.download_document(session, document),
                        timeout=self.pdf_timeout + 30  # Extra buffer
                    )
                    
                    if result['success']:
                        if result.get('skipped'):
                            # File already existed - track separately
                            self.results['skipped'].append(result['document_id'])
                            self.results['stats']['skipped_count'] += 1
                            self.logger.info(f"⏭️  Skipped (already downloaded): {result['title'][:50]}...")
                        else:
                            # New download
                            self.results['downloaded'].append(result)
                            self.results['stats']['downloaded_count'] += 1
                            self.results['stats']['total_size_mb'] += result['file_size_mb']
                    else:
                        self.results['failed'].append(result)
                        self.results['stats']['failed_count'] += 1
                        
                except asyncio.TimeoutError:
                    self.logger.error(f"❌ {company_display}: Overall timeout ({self.pdf_timeout + 30}s)")
                    self.results['failed'].append({
                        "document_id": document['id'],
                        "company_name": company_display,
                        "title": document['title'],
                        "success": False,
                        "error": f"Overall timeout ({self.pdf_timeout + 30}s)",
                        "failure_reason": "timeout",
                        "file_path": None,
                        "file_size_mb": 0,
                        "processing_time": self.pdf_timeout + 30
                    })
                    self.results['stats']['failed_count'] += 1
                    
                except Exception as e:
                    self.logger.error(f"❌ {company_display}: Critical error: {e}")
                    self.results['failed'].append({
                        "document_id": document['id'],
                        "company_name": company_display,
                        "title": document['title'],
                        "success": False,
                        "error": f"Critical error: {e}",
                        "failure_reason": "critical_error",
                        "file_path": None,
                        "file_size_mb": 0,
                        "processing_time": 0
                    })
                    self.results['stats']['failed_count'] += 1
                
                # Clear in_progress
                self.results['in_progress'] = None

                # Save progress after each download
                self.save_results()

                # Wait between downloads (be respectful) - ONLY if we actually downloaded
                if i < len(documents_to_process):
                    if result.get('success') and not result.get('skipped'):
                        # Only delay if we actually downloaded a new file
                        self.logger.info(f"⏱️  Waiting {self.download_delay} seconds before next download...")
                        await asyncio.sleep(self.download_delay)
                    else:
                        # No delay for skipped files - continue immediately
                        pass
            
        # Final results
        self.print_final_summary()
        self.save_results()
    
    def print_final_summary(self):
        """Print comprehensive final summary"""
        total_time = datetime.now() - self.start_time
        stats = self.results['stats']
        
        print(f"\n{'='*70}")
        print(f"🎉 PDF DOWNLOAD BATCH COMPLETE")
        print(f"{'='*70}")
        print(f"⏱️  Total Time: {total_time}")
        print(f"📊 Total Documents: {stats['total_documents']}")
        print(f"✅ Downloaded: {stats['downloaded_count']}")
        print(f"❌ Failed: {stats['failed_count']}")
        print(f"⏭️  Skipped: {stats['skipped_count']}")
        print(f"💾 Total Size: {stats['total_size_mb']:.1f} MB")
        
        if stats['downloaded_count'] > 0 and stats['processing_time_seconds'] > 0:
            avg_speed = stats['total_size_mb'] / (stats['processing_time_seconds'] / 60)  # MB/min
            print(f"📈 Average Speed: {avg_speed:.1f} MB/min")
        
        print(f"\n📁 FILES GENERATED:")
        print(f"   📊 Results: {self.results_file}")
        print(f"   📝 Logs: {self.log_file}")
        print(f"   📂 Storage: {self.base_storage_path}/")
        
        if self.results['downloaded']:
            print(f"\n✅ SUCCESSFUL DOWNLOADS:")
            for result in self.results['downloaded'][:10]:  # Show first 10
                company_name = result.get('company_name', 'Unknown')
                size_mb = result.get('file_size_mb', 0)
                processing_time = result.get('processing_time', 0)
                print(f"   {company_name}: {size_mb:.1f}MB ({processing_time:.1f}s)")
            if len(self.results['downloaded']) > 10:
                print(f"   ... and {len(self.results['downloaded']) - 10} more downloads")
        
        if self.results['failed']:
            print(f"\n❌ FAILED DOWNLOADS:")
            # Group by failure reason
            failure_groups = {}
            for result in self.results['failed']:
                reason = result.get('failure_reason', 'unknown')
                if reason not in failure_groups:
                    failure_groups[reason] = []
                failure_groups[reason].append(result)
            
            for reason, failures in failure_groups.items():
                print(f"\n   🔴 {reason.upper().replace('_', ' ')} ({len(failures)} documents):")
                for result in failures[:5]:  # Show first 5
                    company_name = result.get('company_name', 'Unknown')
                    print(f"      • {company_name}: {result.get('error', 'No details')}")
                if len(failures) > 5:
                    print(f"      ... and {len(failures) - 5} more")
                
        print(f"\n💡 To retry failed downloads, run this script again and choose 'resume'")

async def main():
    """Main entry point with argument parsing"""
    import argparse
    
    parser = argparse.ArgumentParser(description="PDF Download Batch Processor")
    parser.add_argument("--year", type=int, help="Target year for downloads (default: all years)")
    parser.add_argument("--company", type=str, help="Target company name or ticker (default: all companies)")
    parser.add_argument("--delay", type=int, default=10, help="Delay between downloads in seconds (default: 10, use 60 for 1 PDF/minute)")
    parser.add_argument("--all-types", action="store_true", help="Download all document types (default: only annual & quarterly reports)")
    
    args = parser.parse_args()
    
    batch_processor = PDFDownloadBatch(
        target_year=args.year,
        target_company=args.company,
        download_delay=args.delay,
        reports_only=not args.all_types  # Default to reports only, unless --all-types is specified
    )
    
    await batch_processor.run_batch_download()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()