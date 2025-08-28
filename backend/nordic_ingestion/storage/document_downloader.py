"""
Document Downloader
Downloads and stores PDF documents found by collectors
"""
import asyncio
import logging
import hashlib
import os
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime
import aiohttp
import aiofiles
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import update, select

from shared.config import settings
from shared.monitoring import record_document_processed
from ..models import NordicDocument


class DocumentDownloader:
    """
    Production-ready document downloader for Nordic financial documents
    """
    
    def __init__(self, storage_path: Optional[str] = None):
        self.logger = logging.getLogger(__name__)
        self.storage_path = Path(storage_path or settings.local_storage_path)
        self.session: Optional[aiohttp.ClientSession] = None
        
        # Ensure storage directory exists
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=settings.download_timeout_seconds),
            headers={
                'User-Agent': 'YodaBuffett-Nordic/1.0 (+https://yodabuffett.com/about)',
                'Accept': 'application/pdf,application/octet-stream,*/*'
            }
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    async def download_document(
        self, 
        document_id: str, 
        pdf_url: str, 
        db: AsyncSession
    ) -> Dict[str, Any]:
        """
        Download a single document and update database
        Returns: {"success": bool, "file_path": str, "error": str}
        """
        
        try:
            self.logger.info(f"📥 Downloading document {document_id}: {pdf_url}")
            
            # Get document metadata
            result = await db.execute(
                select(NordicDocument).where(NordicDocument.id == document_id)
            )
            document = result.scalar_one_or_none()
            
            if not document:
                return {"success": False, "error": "Document not found in database"}
            
            # Create scalable storage directory structure
            storage_dir = self._create_storage_path(document)
            storage_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate filename
            filename = self._generate_filename(document, pdf_url)
            file_path = storage_dir / filename
            
            # Download with retries
            download_result = await self._download_with_retries(pdf_url, file_path)
            
            if not download_result["success"]:
                await self._update_document_status(
                    db, document_id, "failed", error=download_result["error"]
                )
                return download_result
            
            # Validate downloaded file
            validation_result = await self._validate_downloaded_file(file_path, document)
            
            if not validation_result["valid"]:
                # Delete invalid file
                file_path.unlink(missing_ok=True)
                await self._update_document_status(
                    db, document_id, "failed", error=validation_result["error"]
                )
                return {"success": False, "error": validation_result["error"]}
            
            # Calculate file hash and size
            file_hash = await self._calculate_file_hash(file_path)
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            
            # Update document in database
            await db.execute(
                update(NordicDocument)
                .where(NordicDocument.id == document_id)
                .values(
                    storage_path=str(file_path),
                    file_hash=file_hash,
                    file_size_mb=round(file_size_mb, 2),
                    processing_status="downloaded",
                    metadata_={
                        **document.metadata_,
                        "download_date": datetime.utcnow().isoformat(),
                        "original_url": pdf_url,
                        "file_validation": validation_result
                    }
                )
            )
            
            await db.commit()
            
            # Record metrics - extract company name from metadata
            company_name = "Unknown"
            if document.metadata_ and 'mfn_source' in document.metadata_:
                import re
                mfn_url = document.metadata_['mfn_source']
                company_match = re.search(r'/a/([^?]+)', mfn_url)
                if company_match:
                    company_name = company_match.group(1).title()  # sandvik -> Sandvik
            
            record_document_processed(
                company_name,
                document.document_type, 
                "downloaded"
            )
            
            self.logger.info(f"✅ Downloaded {filename} ({file_size_mb:.2f}MB)")
            
            return {
                "success": True,
                "file_path": str(file_path),
                "file_size_mb": file_size_mb,
                "file_hash": file_hash
            }
            
        except Exception as e:
            self.logger.error(f"Download failed for {document_id}: {e}")
            await self._update_document_status(db, document_id, "failed", error=str(e))
            return {"success": False, "error": str(e)}
    
    async def _download_with_retries(
        self, 
        url: str, 
        file_path: Path, 
        max_retries: int = 3
    ) -> Dict[str, Any]:
        """Download file with retry logic"""
        
        last_error = None
        
        for attempt in range(max_retries):
            try:
                self.logger.debug(f"Download attempt {attempt + 1}/{max_retries}: {url}")
                
                async with self.session.get(url) as response:
                    # Check HTTP status
                    if response.status != 200:
                        last_error = f"HTTP {response.status}: {response.reason}"
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2 ** attempt)  # Exponential backoff
                            continue
                        else:
                            return {"success": False, "error": last_error}
                    
                    # Check content type
                    content_type = response.headers.get('content-type', '').lower()
                    if 'pdf' not in content_type and 'octet-stream' not in content_type:
                        last_error = f"Invalid content type: {content_type}"
                        if attempt < max_retries - 1:
                            continue
                        else:
                            return {"success": False, "error": last_error}
                    
                    # Check content length
                    content_length = response.headers.get('content-length')
                    if content_length and int(content_length) < 1000:  # Less than 1KB
                        last_error = f"File too small: {content_length} bytes"
                        if attempt < max_retries - 1:
                            continue
                        else:
                            return {"success": False, "error": last_error}
                    
                    # Download file
                    async with aiofiles.open(file_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(8192):
                            await f.write(chunk)
                    
                    return {"success": True, "file_path": str(file_path)}
                    
            except asyncio.TimeoutError:
                last_error = "Download timeout"
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                    
            except Exception as e:
                last_error = str(e)
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
        
        return {"success": False, "error": f"Download failed after {max_retries} attempts: {last_error}"}
    
    async def _validate_downloaded_file(self, file_path: Path, document) -> Dict[str, Any]:
        """Validate that downloaded file is a valid PDF"""
        
        try:
            # Check file exists and has content
            if not file_path.exists():
                return {"valid": False, "error": "File does not exist"}
            
            file_size = file_path.stat().st_size
            if file_size < 1000:  # Less than 1KB
                return {"valid": False, "error": f"File too small: {file_size} bytes"}
            
            # Check PDF magic bytes
            async with aiofiles.open(file_path, 'rb') as f:
                header = await f.read(8)
                if not header.startswith(b'%PDF-'):
                    return {"valid": False, "error": "Not a valid PDF file"}
            
            # Basic content validation (could be enhanced)
            try:
                # Try to read PDF content (would need PyPDF2 or similar)
                # For now, just validate it's a PDF by header
                pass
            except Exception as e:
                return {"valid": False, "error": f"PDF validation failed: {e}"}
            
            return {
                "valid": True, 
                "file_size_bytes": file_size,
                "validation_date": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            return {"valid": False, "error": f"Validation error: {e}"}
    
    async def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of file for deduplication"""
        
        sha256_hash = hashlib.sha256()
        
        async with aiofiles.open(file_path, 'rb') as f:
            while chunk := await f.read(8192):
                sha256_hash.update(chunk)
        
        return sha256_hash.hexdigest()
    
    def _create_storage_path(self, document) -> Path:
        """
        Create highly scalable directory structure for company data storage
        
        Structure: data/companies/{country}/{first_letter}/{company}/{year}/{document_type}/
        Example: data/companies/SE/S/sandvik/2025/quarterly_report/
        """
        from datetime import datetime
        
        # Extract year from document title or use current year
        year = "2025"  # Default for now
        if document.report_period and document.report_period != "Unknown":
            # Try to extract year from report period (e.g., "Q2_2025")
            if "_" in document.report_period and document.report_period.split("_")[-1].isdigit():
                year = document.report_period.split("_")[-1]
        elif document.title:
            # Extract year from title if available
            import re
            year_match = re.search(r'20(2[0-9])', document.title)
            if year_match:
                year = year_match.group(0)
        
        # Get country (default to Sweden)
        country = "SE"
        
        # Determine company name from the MFN source URL in metadata
        company_folder = "unknown"
        if document.metadata_ and 'mfn_source' in document.metadata_:
            mfn_url = document.metadata_['mfn_source']
            # Extract company from MFN URL: "https://mfn.se/all/a/sandvik?limit=5" -> "sandvik"
            import re
            company_match = re.search(r'/a/([^?]+)', mfn_url)
            if company_match:
                company_folder = company_match.group(1)
        
        # Clean company name for filesystem
        import re
        company_folder = re.sub(r'[^\w\-]', '', company_folder).lower()
        
        # First letter for alphabetical bucketing (handles thousands of companies)
        first_letter = company_folder[0].upper() if company_folder else "U"
        
        # Document type folder
        doc_type = document.document_type or "unknown"
        
        # Build path: data/companies/SE/S/sandvik/2025/quarterly_report/
        return self.storage_path / "companies" / country / first_letter / company_folder / year / doc_type
    
    def _generate_filename(self, document, pdf_url: str) -> str:
        """
        Generate clean, standardized filename
        
        Format: {report_period}-{document_type}.pdf
        Example: q2-2025-quarterly-report.pdf
        """
        # Document type (clean)
        doc_type = document.document_type or "unknown"
        if doc_type == "quarterly_report":
            doc_type = "quarterly-report"
        elif doc_type == "press_release":
            doc_type = "press-release"
        elif doc_type == "annual_report":
            doc_type = "annual-report"
        
        # Extract report period from title if not available
        period = document.report_period or "unknown"
        if period == "Unknown" and document.title:
            # Try to extract quarter/year from Swedish and English titles
            title_lower = document.title.lower()
            
            # Swedish: "delårsrapport andra kvartalet 2025" -> "q2-2025"
            if "andra kvartalet 2025" in title_lower:
                period = "q2-2025"
            elif "första kvartalet 2025" in title_lower:
                period = "q1-2025"
            elif "tredje kvartalet 2025" in title_lower:
                period = "q3-2025"
            elif "fjärde kvartalet 2025" in title_lower:
                period = "q4-2025"
            # English: "second quarter 2025" -> "q2-2025"
            elif "second quarter 2025" in title_lower:
                period = "q2-2025"
            elif "first quarter 2025" in title_lower:
                period = "q1-2025"
            elif "third quarter 2025" in title_lower:
                period = "q3-2025"
            elif "fourth quarter 2025" in title_lower:
                period = "q4-2025"
            # Year only
            elif "2025" in title_lower:
                period = "2025"
            elif "2024" in title_lower:
                period = "2024"
        
        # Clean up period format
        if "_" in period:
            period = period.replace("_", "-")  # Q2_2025 -> Q2-2025
            
        # Generate clean filename
        if period != "unknown":
            filename = f"{period}-{doc_type}.pdf"
        else:
            # Fallback: use first few words of title
            title_words = document.title[:30].replace(" ", "-") if document.title else "document"
            filename = f"{title_words}-{doc_type}.pdf"
        
        # Clean up filename
        import re
        filename = re.sub(r'[^\w\-\.]', '', filename)  # Remove special chars
        filename = re.sub(r'-+', '-', filename)        # Collapse multiple dashes
        
        return filename.lower()
    
    async def _update_document_status(
        self, 
        db: AsyncSession, 
        document_id: str, 
        status: str, 
        error: Optional[str] = None
    ):
        """Update document processing status"""
        
        metadata_update = {"last_updated": datetime.utcnow().isoformat()}
        if error:
            metadata_update["error"] = error
        
        await db.execute(
            update(NordicDocument)
            .where(NordicDocument.id == document_id)
            .values(
                processing_status=status,
                metadata_=metadata_update
            )
        )
        await db.commit()


# Convenience function for external use
async def download_pending_documents(limit: int = 10) -> Dict[str, int]:
    """
    Download pending documents from database
    Returns: {"downloaded": count, "failed": count}
    """
    from shared.database import AsyncSessionLocal
    from sqlalchemy import select
    
    downloaded = 0
    failed = 0
    
    async with AsyncSessionLocal() as db:
        # Get pending documents with PDF URLs
        query = select(NordicDocument).where(
            NordicDocument.processing_status == "pending",
            NordicDocument.metadata_.op('->>')('pdf_urls').isnot(None)
        ).limit(limit)
        
        result = await db.execute(query)
        pending_docs = result.scalars().all()
        
        if not pending_docs:
            return {"downloaded": 0, "failed": 0}
        
        async with DocumentDownloader() as downloader:
            for doc in pending_docs:
                pdf_urls = doc.metadata_.get('pdf_urls', [])
                if not pdf_urls:
                    continue
                
                # Try first PDF URL
                download_result = await downloader.download_document(
                    str(doc.id), 
                    pdf_urls[0], 
                    db
                )
                
                if download_result["success"]:
                    downloaded += 1
                else:
                    failed += 1
    
    return {"downloaded": downloaded, "failed": failed}