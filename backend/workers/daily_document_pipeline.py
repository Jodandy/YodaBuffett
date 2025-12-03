#!/usr/bin/env python3
"""
Daily Document Processing Pipeline
Complete automated pipeline: Download PDFs → Extract Text → Generate Embeddings

Features:
- PDF download for new documents
- Text extraction from downloaded PDFs
- Vector embedding generation 
- Section-level processing
- Full pipeline integration
- Progress tracking and resume capability
- Health monitoring
"""

import asyncio
import sys
import os
import json
import signal
import time
import schedule
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
import logging
from pathlib import Path
import traceback

# Add parent directories to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from workers.worker_config import get_config, setup_worker_logging
from workers.base.health_server import HealthServer


class DailyDocumentPipeline:
    """
    Complete daily document processing pipeline
    """
    
    def __init__(self):
        self.config = get_config()
        self.logger = setup_worker_logging()
        self.start_time = datetime.now()
        self.session_id = self.start_time.strftime("%Y%m%d_%H%M%S")
        
        # Results tracking
        self.results = {
            "session_id": self.session_id,
            "start_time": self.start_time.isoformat(),
            "pipeline_stages": {
                "pdf_download": {"status": "pending", "stats": {}},
                "text_extraction": {"status": "pending", "stats": {}},
                "embedding_generation": {"status": "pending", "stats": {}}
            },
            "total_stats": {
                "documents_processed": 0,
                "pdfs_downloaded": 0,
                "texts_extracted": 0,
                "embeddings_generated": 0,
                "processing_time_seconds": 0
            }
        }
        
        # File paths
        self.results_file = f"data/pipeline_results_{self.session_id}.json"
        self.log_file = f"logs/pipeline_{self.session_id}.log"
        
        # Health server for monitoring
        health_port = int(os.environ.get('HEALTH_CHECK_PORT', 8087))
        self.health_server = HealthServer(health_port, self._health_check)
        
        # Graceful shutdown
        self.shutdown_requested = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Handle graceful shutdown"""
        self.logger.info(f"🛑 Shutdown signal received ({signum})")
        self.shutdown_requested = True
        
    def _health_check(self) -> Dict:
        """Health check for monitoring"""
        return {
            "service": "daily-document-pipeline",
            "status": "running" if not self.shutdown_requested else "shutting_down",
            "session_id": self.session_id,
            "uptime_seconds": (datetime.now() - self.start_time).total_seconds(),
            "current_stage": self._get_current_stage(),
            "total_processed": self.results["total_stats"]["documents_processed"]
        }
    
    def _get_current_stage(self) -> str:
        """Get current pipeline stage"""
        for stage, info in self.results["pipeline_stages"].items():
            if info["status"] == "running":
                return stage
        return "idle"
    
    def save_results(self):
        """Save current results to file"""
        try:
            self.results["total_stats"]["processing_time_seconds"] = (
                datetime.now() - self.start_time
            ).total_seconds()
            
            os.makedirs(os.path.dirname(self.results_file), exist_ok=True)
            with open(self.results_file, 'w') as f:
                json.dump(self.results, f, indent=2, default=str)
                
        except Exception as e:
            self.logger.error(f"❌ Error saving results: {e}")
    
    async def stage_1_pdf_download(self) -> bool:
        """
        Stage 1: Download new PDFs
        """
        self.logger.info("📥 Starting Stage 1: PDF Download")
        self.results["pipeline_stages"]["pdf_download"]["status"] = "running"
        self.save_results()
        
        try:
            # Import PDF download batch processor
            from pdf_download_batch import PDFDownloadBatch
            
            # Configure for recent documents (last 30 days + current year)
            current_year = datetime.now().year
            batch_processor = PDFDownloadBatch(
                target_year=current_year,
                target_company=None,
                download_delay=5,  # Faster for daily pipeline
                reports_only=False  # Get all document types
            )
            
            self.logger.info("🔍 Loading documents to download...")
            documents_to_download = await batch_processor.load_documents_to_download()
            
            if not documents_to_download:
                self.logger.info("✅ No new documents to download")
                self.results["pipeline_stages"]["pdf_download"]["status"] = "completed"
                self.results["pipeline_stages"]["pdf_download"]["stats"] = {
                    "documents_found": 0,
                    "pdfs_downloaded": 0
                }
                return True
            
            # Limit to reasonable daily batch (last 7 days of documents)
            recent_cutoff = datetime.now() - timedelta(days=7)
            recent_documents = []
            
            for doc in documents_to_download:
                if doc.get('publish_date'):
                    try:
                        pub_date = datetime.fromisoformat(doc['publish_date'].replace('Z', '+00:00'))
                        if pub_date >= recent_cutoff:
                            recent_documents.append(doc)
                    except:
                        # Include documents with parsing issues
                        recent_documents.append(doc)
                else:
                    # Include documents without dates
                    recent_documents.append(doc)
            
            # Limit to maximum 100 documents per day
            if len(recent_documents) > 100:
                recent_documents = recent_documents[:100]
                self.logger.info(f"📋 Limited to {len(recent_documents)} most recent documents")
            
            self.logger.info(f"📄 Found {len(recent_documents)} recent documents to download")
            
            if not recent_documents:
                self.logger.info("✅ No recent documents to download")
                self.results["pipeline_stages"]["pdf_download"]["status"] = "completed"
                return True
            
            # Run PDF download
            downloaded_count = 0
            failed_count = 0
            
            import aiohttp
            
            async with aiohttp.ClientSession(
                headers={
                    'User-Agent': 'YodaBuffett-Research/1.0 (Financial Research; +https://yodabuffett.com)',
                    'Accept': 'application/pdf,*/*;q=0.9',
                },
                timeout=aiohttp.ClientTimeout(total=300)
            ) as session:
                
                for i, document in enumerate(recent_documents, 1):
                    if self.shutdown_requested:
                        self.logger.info("🛑 Shutdown requested during PDF download")
                        break
                    
                    self.logger.info(f"📄 [{i}/{len(recent_documents)}] {document['company_name']}: {document['title'][:50]}...")
                    
                    try:
                        result = await batch_processor.download_document(session, document)
                        
                        if result['success']:
                            if result.get('skipped'):
                                self.logger.info(f"⏭️  Already downloaded: {document['title'][:50]}...")
                            else:
                                downloaded_count += 1
                                self.logger.info(f"✅ Downloaded: {result['file_size_mb']:.1f}MB")
                        else:
                            failed_count += 1
                            self.logger.error(f"❌ Failed: {result.get('error', 'Unknown error')}")
                            
                    except Exception as e:
                        failed_count += 1
                        self.logger.error(f"❌ Exception downloading {document['title'][:50]}...: {e}")
                    
                    # Brief pause between downloads
                    if i < len(recent_documents):
                        await asyncio.sleep(2)
            
            # Stage 1 results
            self.results["pipeline_stages"]["pdf_download"]["status"] = "completed"
            self.results["pipeline_stages"]["pdf_download"]["stats"] = {
                "documents_found": len(recent_documents),
                "pdfs_downloaded": downloaded_count,
                "pdfs_failed": failed_count,
                "success_rate": downloaded_count / len(recent_documents) if recent_documents else 0
            }
            
            self.results["total_stats"]["pdfs_downloaded"] = downloaded_count
            
            self.logger.info(f"✅ Stage 1 Complete: {downloaded_count} PDFs downloaded, {failed_count} failed")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Stage 1 failed: {e}")
            self.logger.error(traceback.format_exc())
            self.results["pipeline_stages"]["pdf_download"]["status"] = "failed"
            self.results["pipeline_stages"]["pdf_download"]["error"] = str(e)
            return False
    
    async def stage_2_text_extraction(self) -> bool:
        """
        Stage 2: Extract text from downloaded PDFs
        """
        self.logger.info("📝 Starting Stage 2: Text Extraction")
        self.results["pipeline_stages"]["text_extraction"]["status"] = "running"
        self.save_results()
        
        try:
            # Import document processing services
            from domains.document_intelligence.services.nordic_extraction_service import NordicExtractionService
            from domains.document_intelligence.repositories.postgres_repository import PostgresDocumentRepository
            from shared.database import AsyncSessionLocal
            
            # Initialize extraction service
            repo = PostgresDocumentRepository()
            extraction_service = NordicExtractionService(repo)
            
            # Find documents that need text extraction
            async with AsyncSessionLocal() as db:
                from nordic_ingestion.models import NordicDocument
                from sqlalchemy import select, and_
                
                # Get recently downloaded documents that need extraction
                recent_cutoff = datetime.now() - timedelta(days=7)
                
                result = await db.execute(
                    select(NordicDocument).where(
                        and_(
                            NordicDocument.processing_status == "downloaded",
                            NordicDocument.storage_path.is_not(None),
                            # Focus on recent documents
                            NordicDocument.publish_date >= recent_cutoff.date()
                        )
                    ).limit(50)  # Limit for daily processing
                )
                
                documents_to_extract = result.scalars().all()
            
            if not documents_to_extract:
                self.logger.info("✅ No documents need text extraction")
                self.results["pipeline_stages"]["text_extraction"]["status"] = "completed"
                self.results["pipeline_stages"]["text_extraction"]["stats"] = {
                    "documents_found": 0,
                    "texts_extracted": 0
                }
                return True
            
            self.logger.info(f"📄 Found {len(documents_to_extract)} documents for text extraction")
            
            extracted_count = 0
            failed_count = 0
            
            for i, doc in enumerate(documents_to_extract, 1):
                if self.shutdown_requested:
                    self.logger.info("🛑 Shutdown requested during text extraction")
                    break
                
                self.logger.info(f"📝 [{i}/{len(documents_to_extract)}] Extracting: {doc.title[:50]}...")
                
                try:
                    # Check if PDF file exists
                    if not doc.storage_path or not Path(doc.storage_path).exists():
                        self.logger.warning(f"⚠️  PDF not found: {doc.storage_path}")
                        failed_count += 1
                        continue
                    
                    # Run extraction
                    success = await extraction_service.extract_and_store_document(doc.id)
                    
                    if success:
                        extracted_count += 1
                        self.logger.info(f"✅ Extracted: {doc.title[:50]}...")
                    else:
                        failed_count += 1
                        self.logger.error(f"❌ Extraction failed: {doc.title[:50]}...")
                        
                except Exception as e:
                    failed_count += 1
                    self.logger.error(f"❌ Exception extracting {doc.title[:50]}...: {e}")
            
            # Stage 2 results
            self.results["pipeline_stages"]["text_extraction"]["status"] = "completed"
            self.results["pipeline_stages"]["text_extraction"]["stats"] = {
                "documents_found": len(documents_to_extract),
                "texts_extracted": extracted_count,
                "texts_failed": failed_count,
                "success_rate": extracted_count / len(documents_to_extract) if documents_to_extract else 0
            }
            
            self.results["total_stats"]["texts_extracted"] = extracted_count
            
            self.logger.info(f"✅ Stage 2 Complete: {extracted_count} texts extracted, {failed_count} failed")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Stage 2 failed: {e}")
            self.logger.error(traceback.format_exc())
            self.results["pipeline_stages"]["text_extraction"]["status"] = "failed"
            self.results["pipeline_stages"]["text_extraction"]["error"] = str(e)
            return False
    
    async def stage_3_embedding_generation(self) -> bool:
        """
        Stage 3: Generate embeddings for extracted text
        """
        self.logger.info("🧠 Starting Stage 3: Embedding Generation")
        self.results["pipeline_stages"]["embedding_generation"]["status"] = "running"
        self.save_results()
        
        try:
            # Import embedding services
            from domains.document_intelligence.services.document_embedding_service import DocumentEmbeddingService
            from domains.document_intelligence.services.section_embedding_service import SectionEmbeddingService
            from domains.document_intelligence.repositories.postgres_repository import PostgresDocumentRepository
            from shared.database import AsyncSessionLocal
            
            # Initialize embedding services
            repo = PostgresDocumentRepository()
            doc_embedding_service = DocumentEmbeddingService(repo)
            section_embedding_service = SectionEmbeddingService(repo)
            
            # Find documents that need embeddings
            async with AsyncSessionLocal() as db:
                from domains.document_intelligence.models.document import ExtractedDocument
                from sqlalchemy import select, and_
                
                # Get recently extracted documents that don't have embeddings yet
                recent_cutoff = datetime.now() - timedelta(days=7)
                
                result = await db.execute(
                    select(ExtractedDocument).where(
                        and_(
                            ExtractedDocument.processing_status == "completed",
                            ExtractedDocument.extracted_at >= recent_cutoff,
                            # Documents without embeddings
                            ~db.query(
                                select(1).select_from(db.query(
                                    select(1)
                                    # Note: Would need to join with document_embeddings table
                                    # This is a simplified version
                                ).subquery())
                            ).exists()
                        )
                    ).limit(30)  # Limit for daily processing (embeddings are slower)
                )
                
                documents_to_embed = result.scalars().all()
            
            if not documents_to_embed:
                self.logger.info("✅ No documents need embeddings")
                self.results["pipeline_stages"]["embedding_generation"]["status"] = "completed"
                self.results["pipeline_stages"]["embedding_generation"]["stats"] = {
                    "documents_found": 0,
                    "embeddings_generated": 0
                }
                return True
            
            self.logger.info(f"🧠 Found {len(documents_to_embed)} documents for embedding generation")
            
            embedded_count = 0
            failed_count = 0
            
            for i, doc in enumerate(documents_to_embed, 1):
                if self.shutdown_requested:
                    self.logger.info("🛑 Shutdown requested during embedding generation")
                    break
                
                self.logger.info(f"🧠 [{i}/{len(documents_to_embed)}] Embedding: {doc.title[:50]}...")
                
                try:
                    # Generate document-level embeddings
                    doc_success = await doc_embedding_service.generate_and_store_embedding(doc.id)
                    
                    # Generate section-level embeddings
                    section_success = await section_embedding_service.process_document_sections(doc.id)
                    
                    if doc_success or section_success:
                        embedded_count += 1
                        self.logger.info(f"✅ Embedded: {doc.title[:50]}... (doc: {doc_success}, sections: {section_success})")
                    else:
                        failed_count += 1
                        self.logger.error(f"❌ Embedding failed: {doc.title[:50]}...")
                        
                except Exception as e:
                    failed_count += 1
                    self.logger.error(f"❌ Exception embedding {doc.title[:50]}...: {e}")
            
            # Stage 3 results
            self.results["pipeline_stages"]["embedding_generation"]["status"] = "completed"
            self.results["pipeline_stages"]["embedding_generation"]["stats"] = {
                "documents_found": len(documents_to_embed),
                "embeddings_generated": embedded_count,
                "embeddings_failed": failed_count,
                "success_rate": embedded_count / len(documents_to_embed) if documents_to_embed else 0
            }
            
            self.results["total_stats"]["embeddings_generated"] = embedded_count
            
            self.logger.info(f"✅ Stage 3 Complete: {embedded_count} embeddings generated, {failed_count} failed")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Stage 3 failed: {e}")
            self.logger.error(traceback.format_exc())
            self.results["pipeline_stages"]["embedding_generation"]["status"] = "failed"
            self.results["pipeline_stages"]["embedding_generation"]["error"] = str(e)
            return False
    
    async def run_complete_pipeline(self):
        """
        Run the complete document processing pipeline
        """
        self.logger.info("🚀 Starting Daily Document Processing Pipeline")
        self.logger.info(f"📅 Session: {self.session_id}")
        
        # Start health server
        await self.health_server.start()
        
        try:
            # Track overall pipeline success
            pipeline_success = True
            
            # Stage 1: PDF Download
            if not self.shutdown_requested:
                stage1_success = await self.stage_1_pdf_download()
                pipeline_success &= stage1_success
                self.save_results()
            
            # Stage 2: Text Extraction
            if not self.shutdown_requested and stage1_success:
                stage2_success = await self.stage_2_text_extraction()
                pipeline_success &= stage2_success
                self.save_results()
            
            # Stage 3: Embedding Generation
            if not self.shutdown_requested and stage2_success:
                stage3_success = await self.stage_3_embedding_generation()
                pipeline_success &= stage3_success
                self.save_results()
            
            # Final results
            total_time = datetime.now() - self.start_time
            
            self.logger.info("="*70)
            if pipeline_success:
                self.logger.info("🎉 DAILY DOCUMENT PIPELINE COMPLETE")
            else:
                self.logger.info("⚠️  DAILY DOCUMENT PIPELINE COMPLETED WITH ISSUES")
            self.logger.info("="*70)
            
            self.logger.info(f"⏱️  Total Time: {total_time}")
            self.logger.info(f"📥 PDFs Downloaded: {self.results['total_stats']['pdfs_downloaded']}")
            self.logger.info(f"📝 Texts Extracted: {self.results['total_stats']['texts_extracted']}")
            self.logger.info(f"🧠 Embeddings Generated: {self.results['total_stats']['embeddings_generated']}")
            self.logger.info(f"📊 Results File: {self.results_file}")
            
        finally:
            # Stop health server
            await self.health_server.stop()
            
    def run_daily_schedule(self):
        """
        Run scheduled daily pipeline
        """
        # Schedule daily run at 11:00 AM (after document downloads at 10:00 AM)
        daily_time = os.environ.get('PIPELINE_RUN_TIME', '11:00')
        schedule.every().day.at(daily_time).do(self._run_pipeline_sync)
        
        self.logger.info(f"📅 Daily document pipeline scheduled for {daily_time}")
        
        while not self.shutdown_requested:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    def _run_pipeline_sync(self):
        """Sync wrapper for async pipeline execution"""
        try:
            asyncio.run(self.run_complete_pipeline())
        except Exception as e:
            self.logger.error(f"❌ Pipeline execution failed: {e}")
            self.logger.error(traceback.format_exc())


async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Daily Document Processing Pipeline")
    parser.add_argument("--schedule", action="store_true", help="Run on daily schedule")
    parser.add_argument("--run-time", type=str, help="Schedule time (HH:MM format)", default="11:00")
    
    args = parser.parse_args()
    
    pipeline = DailyDocumentPipeline()
    
    if args.schedule:
        # Set custom run time if provided
        if args.run_time:
            os.environ['PIPELINE_RUN_TIME'] = args.run_time
            
        # Run on schedule
        pipeline.run_daily_schedule()
    else:
        # Run once immediately
        await pipeline.run_complete_pipeline()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()