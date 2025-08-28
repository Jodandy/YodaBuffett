"""
Document Catalog Storage
Stores discovered PDF documents before download (index building)
"""
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, and_

from shared.database import AsyncSessionLocal
from ..models import NordicDocument, NordicCompany
from ..collectors.aggregator.mfn_collector import MFNNewsItem

class DocumentCatalogStorage:
    """
    Stores catalogued documents in database before download
    
    Status Flow:
    1. "catalogued" = PDF URL found, metadata stored
    2. "pending" = queued for download  
    3. "downloading" = currently downloading
    4. "downloaded" = file stored locally
    5. "failed" = download failed
    """
    
    async def store_catalogued_documents(
        self, 
        mfn_items: List[MFNNewsItem]
    ) -> Dict[str, int]:
        """
        Store discovered documents in catalog without downloading
        
        Args:
            mfn_items: List of MFN news items with PDF URLs
            
        Returns:
            {"stored": count, "duplicates": count, "errors": count}
        """
        stats = {"stored": 0, "duplicates": 0, "errors": 0}
        
        print(f"🔍 Document storage: processing {len(mfn_items)} items")
        
        async with AsyncSessionLocal() as db:
            
            for item in mfn_items:
                try:
                    print(f"📄 Processing item: {item.company_name} - {len(item.pdf_urls)} PDFs")
                    
                    # Find company by name
                    company = await self._find_company_by_name(db, item.company_name)
                    if not company:
                        print(f"⚠️  Company not found: {item.company_name}")
                        stats["errors"] += 1
                        continue
                    
                    print(f"✅ Found company: {company.name}")
                    
                    # Store each PDF URL as separate document
                    for pdf_url in item.pdf_urls:
                        print(f"  📄 Processing PDF: {pdf_url}")
                        
                        # Check if already catalogued
                        existing = await self._check_existing_document(db, pdf_url)
                        if existing:
                            print(f"  🔄 Skipping duplicate: {pdf_url}")
                            stats["duplicates"] += 1
                            continue
                        
                        # Create catalogued document entry
                        document = NordicDocument(
                            id=uuid.uuid4(),
                            company_id=company.id,
                            document_type=item.document_type,
                            report_period="Unknown",  # Default value for not-null constraint
                            title=item.title,
                            source_url=item.source_url,
                            storage_path=None,  # Not downloaded yet
                            file_hash=None,     # Not downloaded yet
                            language=company.reporting_language or "en",
                            ingestion_date=datetime.utcnow(),
                            processing_status="catalogued",  # Key status!
                            page_count=None,
                            file_size_mb=None,
                            metadata_={
                                # Core document info
                                "pdf_url": pdf_url,
                                "mfn_source": item.source_url,
                                "discovery_date": datetime.utcnow().isoformat(),
                                "catalog_source": "mfn.se",
                                
                                # Rich metadata for LLM filtering
                                "raw_title": item.title,  # Full original title
                                "content_preview": item.content[:300] if item.content else None,  # First 300 chars
                                "document_classification": item.document_type,  # Our initial classification
                                "publication_date": item.date_published.isoformat() if item.date_published else None,
                                "calendar_info": item.calendar_info,
                                
                                # Language and content indicators
                                "title_language": "swedish" if any(sw in item.title.lower() 
                                    for sw in ["kvartal", "delårs", "år", "styrelse", "bolag"]) else "english",
                                
                                # Financial relevance indicators
                                "financial_keywords": self._extract_financial_keywords(item.title, item.content),
                                "contains_financial_data": self._has_financial_indicators(item.title, item.content),
                                
                                # Document characteristics
                                "title_length": len(item.title) if item.title else 0,
                                "content_length": len(item.content) if item.content else 0,
                                "has_calendar_events": bool(item.calendar_info),
                                
                                # LLM filtering hints
                                "llm_filter_context": {
                                    "company": company.name,
                                    "suggested_relevance": self._assess_relevance(item.title, item.content, item.document_type),
                                    "key_phrases": self._extract_key_phrases(item.title, item.content),
                                    "document_purpose": self._infer_document_purpose(item.title, item.content)
                                }
                            }
                        )
                        
                        print(f"  ✅ Creating document: {item.title[:50]}...")
                        db.add(document)
                        stats["stored"] += 1
                        print(f"  💾 Added to session, total stored: {stats['stored']}")
                        
                except Exception as e:
                    print(f"❌ Error storing {item.company_name} document: {e}")
                    import traceback
                    traceback.print_exc()
                    stats["errors"] += 1
                    
            print(f"💾 Committing {stats['stored']} documents to database...")
            try:
                await db.commit()
                print(f"✅ Database commit successful")
            except Exception as e:
                print(f"❌ Database commit failed: {e}")
                import traceback
                traceback.print_exc()
                await db.rollback()
                stats["errors"] += stats["stored"]
                stats["stored"] = 0
            
        return stats
    
    def _extract_financial_keywords(self, title: str, content: str) -> list:
        """Extract financial keywords for LLM filtering"""
        text = f"{title} {content}".lower()
        
        financial_terms = [
            # Financial metrics
            "revenue", "intäkter", "omsättning", "försäljning", 
            "profit", "vinst", "resultat", "earnings", "ebit", "ebitda",
            "dividend", "utdelning", "cash flow", "kassaflöde",
            "margin", "marginal", "growth", "tillväxt",
            
            # Financial statements  
            "balance sheet", "balansräkning", "income statement", "resultaträkning",
            "quarterly", "kvartal", "annual", "år", "rapport", "report",
            
            # Business terms
            "market", "marknad", "segment", "acquisition", "förvärv", 
            "investment", "investering", "strategy", "strategi",
            "outlook", "prognos", "guidance", "forecast"
        ]
        
        found_terms = [term for term in financial_terms if term in text]
        return found_terms[:10]  # Limit to top 10 to keep metadata manageable
    
    def _has_financial_indicators(self, title: str, content: str) -> bool:
        """Check if document likely contains financial data"""
        text = f"{title} {content}".lower()
        
        financial_indicators = [
            "miljoner", "miljarder", "million", "billion", "msek", "bsek",
            "procent", "percent", "%", "sek", "usd", "eur",
            "q1", "q2", "q3", "q4", "kvartal", "quarterly"
        ]
        
        return any(indicator in text for indicator in financial_indicators)
    
    def _assess_relevance(self, title: str, content: str, doc_type: str) -> str:
        """Assess document relevance for investment research"""
        if doc_type in ["quarterly_report", "annual_report"]:
            return "high"
        elif doc_type in ["corporate_action", "governance"]:
            return "medium"  
        elif any(term in f"{title} {content}".lower() 
                for term in ["financial", "earnings", "results", "investment"]):
            return "medium"
        else:
            return "low"
    
    def _extract_key_phrases(self, title: str, content: str) -> list:
        """Extract key phrases for LLM context"""
        import re
        
        text = f"{title} {content}"
        
        # Extract phrases with financial significance
        key_patterns = [
            r'(Q[1-4]\s+20\d{2})',  # Quarter mentions
            r'(\d+\s+percent|\d+\s+procent|\d+%)',  # Percentages
            r'(\d+\s+(?:miljoner|miljarder|million|billion))',  # Large numbers
            r'(kvartal(?:et|s)?(?:rapport)?)',  # Quarterly terms
            r'(styrelse|board|vd|ceo)',  # Leadership terms
        ]
        
        key_phrases = []
        for pattern in key_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            key_phrases.extend(matches)
        
        return list(set(key_phrases))[:8]  # Unique phrases, limited
    
    def _infer_document_purpose(self, title: str, content: str) -> str:
        """Infer the main purpose of the document"""
        text = f"{title} {content}".lower()
        
        if any(term in text for term in ["rapport", "report", "resultat", "earnings"]):
            return "financial_reporting"
        elif any(term in text for term in ["förvärv", "acquisition", "köper", "invests"]):
            return "corporate_action"
        elif any(term in text for term in ["styrelse", "board", "vd", "ceo", "agm"]):
            return "governance"
        elif any(term in text for term in ["utdelning", "dividend"]):
            return "shareholder_communication"
        elif any(term in text for term in ["prize", "award", "forskning", "research"]):
            return "corporate_pr"
        else:
            return "general_communication"
    
    async def _find_company_by_name(
        self, 
        db: AsyncSession, 
        company_name: str
    ) -> Optional[NordicCompany]:
        """Find company by name (fuzzy matching)"""
        
        # Direct name match first
        result = await db.execute(
            select(NordicCompany).where(NordicCompany.name == company_name)
        )
        company = result.scalar_one_or_none()
        if company:
            return company
            
        # Fuzzy matching for common variations (MFN slug to database name)
        name_variations = {
            "volvo": "Volvo Group",
            "astrazeneca": "AstraZeneca", 
            "atlas-copco": "Atlas Copco AB",
            "ericsson": "Telefonaktiebolaget LM Ericsson",
            "handm": "H&M Hennes & Mauritz AB",
            "sandvik": "Sandvik AB",
            "nordea": "Nordea Bank Abp",
            "investor": "Investor AB",
            "abb": "ABB Ltd",
            "hexagon": "Hexagon AB"
        }
        
        # Check if company_name is a slug we know about
        company_name_lower = company_name.lower()
        if company_name_lower in name_variations:
            target_name = name_variations[company_name_lower]
            result = await db.execute(
                select(NordicCompany).where(NordicCompany.name == target_name)
            )
            return result.scalar_one_or_none()
        
        # Try partial match as fallback
        for slug, full_name in name_variations.items():
            if slug in company_name_lower or company_name_lower in slug:
                result = await db.execute(
                    select(NordicCompany).where(NordicCompany.name == full_name)
                )
                company = result.scalar_one_or_none()
                if company:
                    return company
                
        return None
    
    async def _check_existing_document(
        self, 
        db: AsyncSession, 
        pdf_url: str
    ) -> bool:
        """Check if PDF URL already catalogued"""
        from sqlalchemy import text
        
        result = await db.execute(
            select(NordicDocument).where(
                text("metadata->>'pdf_url' = :pdf_url")
            ).params(pdf_url=pdf_url)
        )
        return result.scalar_one_or_none() is not None
    
    async def get_catalogued_documents(
        self, 
        company_name: Optional[str] = None,
        document_type: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Query catalogued documents (not yet downloaded)
        
        Args:
            company_name: Filter by company
            document_type: Filter by document type
            limit: Maximum results
            
        Returns:
            List of catalogued documents with metadata
        """
        async with AsyncSessionLocal() as db:
            query = select(NordicDocument).where(
                NordicDocument.processing_status == "catalogued"
            )
            
            if company_name:
                # Join with companies table for name filtering
                query = query.join(NordicCompany).where(
                    NordicCompany.name.ilike(f"%{company_name}%")
                )
                
            if document_type:
                query = query.where(NordicDocument.document_type == document_type)
                
            query = query.limit(limit)
            result = await db.execute(query)
            documents = result.scalars().all()
            
            # Convert to dict for easy use
            catalog_items = []
            for doc in documents:
                catalog_items.append({
                    "id": str(doc.id),
                    "company_id": str(doc.company_id),
                    "title": doc.title,
                    "document_type": doc.document_type,
                    "pdf_url": doc.metadata_.get("pdf_url"),
                    "mfn_source": doc.metadata_.get("mfn_source"),
                    "discovery_date": doc.metadata_.get("discovery_date"),
                    "status": doc.processing_status
                })
                
            return catalog_items
    
    async def mark_for_download(self, document_ids: List[str]) -> int:
        """Mark catalogued documents as pending download"""
        
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                update(NordicDocument)
                .where(
                    and_(
                        NordicDocument.id.in_(document_ids),
                        NordicDocument.processing_status == "catalogued"
                    )
                )
                .values(processing_status="pending")
            )
            
            await db.commit()
            return result.rowcount

# Convenience functions
async def catalog_mfn_documents(mfn_items: List[MFNNewsItem]) -> Dict[str, int]:
    """Store MFN discovered documents in catalog"""
    storage = DocumentCatalogStorage()
    return await storage.store_catalogued_documents(mfn_items)

async def get_catalog_summary() -> Dict[str, Any]:
    """Get summary of catalogued documents"""
    async with AsyncSessionLocal() as db:
        # Count by status - simple approach
        catalogued_result = await db.execute(
            select(NordicDocument).where(NordicDocument.processing_status == "catalogued")
        )
        catalogued_count = len(catalogued_result.scalars().all())
        
        pending_result = await db.execute(
            select(NordicDocument).where(NordicDocument.processing_status == "pending")
        )
        pending_count = len(pending_result.scalars().all())
        
        downloaded_result = await db.execute(
            select(NordicDocument).where(NordicDocument.processing_status == "downloaded")
        )
        downloaded_count = len(downloaded_result.scalars().all())
        
        failed_result = await db.execute(
            select(NordicDocument).where(NordicDocument.processing_status == "failed")
        )
        failed_count = len(failed_result.scalars().all())
        
        total = catalogued_count + pending_count + downloaded_count + failed_count
        
        return {
            "catalogued": catalogued_count,
            "pending_download": pending_count,
            "downloaded": downloaded_count,
            "failed": failed_count,
            "total_discovered": total
        }