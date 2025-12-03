#!/usr/bin/env python3
"""
Daily Temporal Anomaly Detection Worker
Runs after document processing to detect unusual patterns in financial communications

Features:
- Document-level anomaly detection (macro patterns)
- Section-level anomaly detection (micro patterns)
- Anomaly scoring and classification
- Database storage of findings
- Email/Slack notifications for significant anomalies
- Historical pattern analysis
"""

import asyncio
import sys
import os
import json
import signal
import time
import schedule
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
import logging
from pathlib import Path
import traceback
import numpy as np

# Add parent directories to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from workers.worker_config import get_config, setup_worker_logging
from workers.base.health_server import HealthServer
from shared.database import AsyncSessionLocal


class DailyAnomalyDetector:
    """
    Daily temporal anomaly detection for financial documents
    """
    
    def __init__(self):
        self.config = get_config()
        self.logger = setup_worker_logging()
        self.start_time = datetime.now()
        self.session_id = self.start_time.strftime("%Y%m%d_%H%M%S")
        
        # Anomaly thresholds
        self.SIGNIFICANT_ANOMALY_THRESHOLD = 0.8  # High priority
        self.MODERATE_ANOMALY_THRESHOLD = 0.6    # Medium priority
        self.MINOR_ANOMALY_THRESHOLD = 0.4       # Low priority
        
        # Results tracking
        self.results = {
            "session_id": self.session_id,
            "start_time": self.start_time.isoformat(),
            "anomalies_detected": {
                "significant": [],
                "moderate": [],
                "minor": []
            },
            "stats": {
                "documents_analyzed": 0,
                "sections_analyzed": 0,
                "anomalies_found": 0,
                "processing_time_seconds": 0
            }
        }
        
        # File paths
        self.results_file = f"data/anomaly_results_{self.session_id}.json"
        self.log_file = f"logs/anomaly_detection_{self.session_id}.log"
        
        # Health server
        health_port = int(os.environ.get('HEALTH_CHECK_PORT', 8088))
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
            "service": "daily-anomaly-detector",
            "status": "running" if not self.shutdown_requested else "shutting_down",
            "session_id": self.session_id,
            "uptime_seconds": (datetime.now() - self.start_time).total_seconds(),
            "anomalies_found": self.results["stats"]["anomalies_found"]
        }
    
    def save_results(self):
        """Save current results to file"""
        try:
            self.results["stats"]["processing_time_seconds"] = (
                datetime.now() - self.start_time
            ).total_seconds()
            
            os.makedirs(os.path.dirname(self.results_file), exist_ok=True)
            with open(self.results_file, 'w') as f:
                json.dump(self.results, f, indent=2, default=str)
                
        except Exception as e:
            self.logger.error(f"❌ Error saving results: {e}")
    
    async def detect_document_anomalies(self):
        """
        Detect document-level temporal anomalies
        """
        self.logger.info("🔍 Starting document-level anomaly detection")
        
        try:
            from domains.document_intelligence.cli_document_temporal_detection import DocumentTemporalDetector
            
            detector = DocumentTemporalDetector()
            
            # Get recent documents (last 7 days)
            recent_cutoff = datetime.now() - timedelta(days=7)
            
            async with AsyncSessionLocal() as db:
                from domains.document_intelligence.models.document import ExtractedDocument, DocumentEmbedding
                from sqlalchemy import select, and_, desc
                from sqlalchemy.orm import selectinload
                
                # Get documents with embeddings from the last 7 days
                result = await db.execute(
                    select(ExtractedDocument)
                    .join(DocumentEmbedding)
                    .where(
                        ExtractedDocument.extracted_at >= recent_cutoff
                    )
                    .options(selectinload(ExtractedDocument.embeddings))
                    .order_by(desc(ExtractedDocument.extracted_at))
                    .limit(100)  # Process up to 100 recent documents
                )
                
                recent_documents = result.scalars().all()
            
            if not recent_documents:
                self.logger.info("✅ No recent documents to analyze")
                return
            
            self.logger.info(f"📄 Found {len(recent_documents)} recent documents to analyze")
            
            # Group by company for temporal analysis
            from collections import defaultdict
            companies = defaultdict(list)
            
            for doc in recent_documents:
                company_id = doc.source_metadata.get('company_id') if doc.source_metadata else None
                if company_id:
                    companies[company_id].append(doc)
            
            self.logger.info(f"🏢 Analyzing {len(companies)} companies")
            
            # Detect anomalies for each company
            for company_id, company_docs in companies.items():
                if len(company_docs) < 2:
                    continue  # Need at least 2 documents for comparison
                
                try:
                    # Sort by date
                    company_docs.sort(key=lambda d: d.publish_date or d.extracted_at)
                    
                    # Compare consecutive documents
                    for i in range(1, len(company_docs)):
                        prev_doc = company_docs[i-1]
                        curr_doc = company_docs[i]
                        
                        # Calculate similarity
                        if prev_doc.embeddings and curr_doc.embeddings:
                            prev_embedding = prev_doc.embeddings[0].embedding_vector
                            curr_embedding = curr_doc.embeddings[0].embedding_vector
                            
                            # Cosine similarity
                            similarity = np.dot(prev_embedding, curr_embedding) / (
                                np.linalg.norm(prev_embedding) * np.linalg.norm(curr_embedding)
                            )
                            
                            # Anomaly score (1 - similarity for dissimilarity)
                            anomaly_score = 1 - similarity
                            
                            # Classify anomaly
                            if anomaly_score >= self.SIGNIFICANT_ANOMALY_THRESHOLD:
                                anomaly = {
                                    "type": "document",
                                    "severity": "significant",
                                    "score": float(anomaly_score),
                                    "company_id": company_id,
                                    "document_id": str(curr_doc.id),
                                    "document_title": curr_doc.title[:100],
                                    "document_date": str(curr_doc.publish_date or curr_doc.extracted_at),
                                    "previous_document": prev_doc.title[:100],
                                    "description": f"Major shift detected in {curr_doc.company_name}'s communication pattern"
                                }
                                self.results["anomalies_detected"]["significant"].append(anomaly)
                                self.logger.warning(f"🚨 SIGNIFICANT anomaly: {curr_doc.company_name} - Score: {anomaly_score:.2f}")
                                
                            elif anomaly_score >= self.MODERATE_ANOMALY_THRESHOLD:
                                anomaly = {
                                    "type": "document",
                                    "severity": "moderate",
                                    "score": float(anomaly_score),
                                    "company_id": company_id,
                                    "document_id": str(curr_doc.id),
                                    "document_title": curr_doc.title[:100],
                                    "document_date": str(curr_doc.publish_date or curr_doc.extracted_at)
                                }
                                self.results["anomalies_detected"]["moderate"].append(anomaly)
                                self.logger.info(f"⚠️  Moderate anomaly: {curr_doc.company_name} - Score: {anomaly_score:.2f}")
                                
                            self.results["stats"]["documents_analyzed"] += 1
                            
                except Exception as e:
                    self.logger.error(f"❌ Error analyzing company {company_id}: {e}")
                    
        except Exception as e:
            self.logger.error(f"❌ Document anomaly detection failed: {e}")
            self.logger.error(traceback.format_exc())
    
    async def detect_section_anomalies(self):
        """
        Detect section-level temporal anomalies
        """
        self.logger.info("🔍 Starting section-level anomaly detection")
        
        try:
            # Get significant document anomalies to drill down
            significant_anomalies = self.results["anomalies_detected"]["significant"]
            
            if not significant_anomalies:
                self.logger.info("✅ No significant document anomalies to analyze at section level")
                return
                
            from domains.document_intelligence.repositories.postgres_repository import PostgresDocumentRepository
            
            repo = PostgresDocumentRepository()
            
            for anomaly in significant_anomalies[:10]:  # Limit to top 10
                try:
                    doc_id = anomaly["document_id"]
                    
                    # Get document sections
                    sections = await repo.get_document_sections(doc_id)
                    
                    if not sections:
                        continue
                        
                    # Analyze financial sections
                    financial_sections = [
                        s for s in sections 
                        if s.section_type in ['financial_statements', 'balance_sheet', 
                                            'income_statement', 'cash_flow', 'key_metrics']
                    ]
                    
                    for section in financial_sections:
                        # Here we would compare with historical sections
                        # For now, flag sections in anomalous documents
                        section_anomaly = {
                            "type": "section",
                            "parent_document": doc_id,
                            "section_type": section.section_type,
                            "section_title": section.title[:100] if section.title else "Untitled",
                            "significance": "High relevance - financial section in anomalous document"
                        }
                        
                        # Add to moderate anomalies (section-level details)
                        self.results["anomalies_detected"]["moderate"].append(section_anomaly)
                        self.results["stats"]["sections_analyzed"] += 1
                        
                except Exception as e:
                    self.logger.error(f"❌ Error analyzing sections for document {doc_id}: {e}")
                    
        except Exception as e:
            self.logger.error(f"❌ Section anomaly detection failed: {e}")
            self.logger.error(traceback.format_exc())
    
    async def store_anomalies_in_database(self):
        """
        Store detected anomalies in database for historical tracking
        """
        self.logger.info("💾 Storing anomalies in database")
        
        try:
            async with AsyncSessionLocal() as db:
                # Create anomaly records table if not exists
                await db.execute("""
                    CREATE TABLE IF NOT EXISTS temporal_anomalies (
                        id SERIAL PRIMARY KEY,
                        detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        anomaly_type VARCHAR(50),
                        severity VARCHAR(20),
                        score FLOAT,
                        company_id VARCHAR(100),
                        document_id UUID,
                        section_id UUID,
                        description TEXT,
                        metadata JSONB,
                        session_id VARCHAR(50)
                    )
                """)
                
                # Insert anomalies
                all_anomalies = []
                for severity, anomalies in self.results["anomalies_detected"].items():
                    for anomaly in anomalies:
                        all_anomalies.append({
                            "anomaly_type": anomaly.get("type", "unknown"),
                            "severity": severity,
                            "score": anomaly.get("score", 0.0),
                            "company_id": anomaly.get("company_id"),
                            "document_id": anomaly.get("document_id"),
                            "section_id": anomaly.get("section_id"),
                            "description": anomaly.get("description", ""),
                            "metadata": json.dumps(anomaly),
                            "session_id": self.session_id
                        })
                
                if all_anomalies:
                    await db.execute("""
                        INSERT INTO temporal_anomalies 
                        (anomaly_type, severity, score, company_id, document_id, 
                         section_id, description, metadata, session_id)
                        VALUES 
                        (:anomaly_type, :severity, :score, :company_id, :document_id,
                         :section_id, :description, :metadata::jsonb, :session_id)
                    """, all_anomalies)
                    
                    await db.commit()
                    self.logger.info(f"✅ Stored {len(all_anomalies)} anomalies in database")
                    
        except Exception as e:
            self.logger.error(f"❌ Failed to store anomalies: {e}")
            self.logger.error(traceback.format_exc())
    
    async def send_notifications(self):
        """
        Send notifications for significant anomalies
        """
        significant = self.results["anomalies_detected"]["significant"]
        
        if not significant:
            self.logger.info("✅ No significant anomalies to notify")
            return
            
        self.logger.info(f"📧 Sending notifications for {len(significant)} significant anomalies")
        
        # Create summary
        summary = f"""
🚨 TEMPORAL ANOMALY DETECTION ALERT
====================================

Session: {self.session_id}
Time: {datetime.now().strftime('%Y-%m-%d %H:%M')}

SIGNIFICANT ANOMALIES DETECTED: {len(significant)}

Top Anomalies:
"""
        
        for i, anomaly in enumerate(significant[:5], 1):
            summary += f"""
{i}. {anomaly.get('document_title', 'Unknown')}
   Company: {anomaly.get('company_id', 'Unknown')}
   Score: {anomaly.get('score', 0):.2f}
   Date: {anomaly.get('document_date', 'Unknown')}
   Description: {anomaly.get('description', 'Unusual pattern detected')}
"""
        
        if len(significant) > 5:
            summary += f"\n... and {len(significant) - 5} more anomalies"
        
        summary += f"""

Full results: {self.results_file}
Dashboard: http://localhost:8090/anomalies

This is an automated alert from YodaBuffett Temporal Anomaly Detection.
"""
        
        # Save notification to file (can be extended to email/Slack)
        notification_file = f"data/anomaly_notifications_{self.session_id}.txt"
        with open(notification_file, 'w') as f:
            f.write(summary)
            
        self.logger.info(f"📄 Notification saved to {notification_file}")
        
        # TODO: Add email/Slack integration here
        # Example:
        # await send_email(to="alerts@company.com", subject="Anomaly Alert", body=summary)
        # await send_slack_message(channel="#alerts", text=summary)
    
    async def run_anomaly_detection(self):
        """
        Run complete anomaly detection pipeline
        """
        self.logger.info("🚀 Starting Daily Temporal Anomaly Detection")
        self.logger.info(f"📅 Session: {self.session_id}")
        
        # Start health server
        await self.health_server.start()
        
        try:
            # Document-level anomalies
            if not self.shutdown_requested:
                await self.detect_document_anomalies()
                self.save_results()
            
            # Section-level anomalies (for significant findings)
            if not self.shutdown_requested:
                await self.detect_section_anomalies()
                self.save_results()
            
            # Store in database
            if not self.shutdown_requested:
                await self.store_anomalies_in_database()
            
            # Send notifications
            if not self.shutdown_requested:
                await self.send_notifications()
            
            # Update stats
            self.results["stats"]["anomalies_found"] = (
                len(self.results["anomalies_detected"]["significant"]) +
                len(self.results["anomalies_detected"]["moderate"]) +
                len(self.results["anomalies_detected"]["minor"])
            )
            
            # Final summary
            total_time = datetime.now() - self.start_time
            
            self.logger.info("="*70)
            self.logger.info("🎉 ANOMALY DETECTION COMPLETE")
            self.logger.info("="*70)
            self.logger.info(f"⏱️  Total Time: {total_time}")
            self.logger.info(f"📄 Documents Analyzed: {self.results['stats']['documents_analyzed']}")
            self.logger.info(f"📑 Sections Analyzed: {self.results['stats']['sections_analyzed']}")
            self.logger.info(f"🚨 Significant Anomalies: {len(self.results['anomalies_detected']['significant'])}")
            self.logger.info(f"⚠️  Moderate Anomalies: {len(self.results['anomalies_detected']['moderate'])}")
            self.logger.info(f"📊 Total Anomalies: {self.results['stats']['anomalies_found']}")
            self.logger.info(f"📁 Results File: {self.results_file}")
            
        finally:
            # Stop health server
            await self.health_server.stop()
            self.save_results()
    
    def run_daily_schedule(self):
        """
        Run scheduled daily anomaly detection
        """
        # Schedule daily run at 12:00 PM (after document processing at 11:00 AM)
        daily_time = os.environ.get('ANOMALY_RUN_TIME', '12:00')
        schedule.every().day.at(daily_time).do(self._run_detection_sync)
        
        self.logger.info(f"📅 Daily anomaly detection scheduled for {daily_time}")
        
        while not self.shutdown_requested:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    def _run_detection_sync(self):
        """Sync wrapper for async detection"""
        try:
            asyncio.run(self.run_anomaly_detection())
        except Exception as e:
            self.logger.error(f"❌ Anomaly detection failed: {e}")
            self.logger.error(traceback.format_exc())


async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Daily Temporal Anomaly Detection")
    parser.add_argument("--schedule", action="store_true", help="Run on daily schedule")
    parser.add_argument("--run-time", type=str, help="Schedule time (HH:MM format)", default="12:00")
    
    args = parser.parse_args()
    
    detector = DailyAnomalyDetector()
    
    if args.schedule:
        # Set custom run time if provided
        if args.run_time:
            os.environ['ANOMALY_RUN_TIME'] = args.run_time
            
        # Run on schedule
        detector.run_daily_schedule()
    else:
        # Run once immediately
        await detector.run_anomaly_detection()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()