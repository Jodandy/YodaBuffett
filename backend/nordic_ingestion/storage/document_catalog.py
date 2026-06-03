"""
Document Catalog Storage
Stores discovered PDF documents before download (index building)
"""
import uuid
import json
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, and_, text, func, or_
from nordic_ingestion.common.company_mappings import COMPANY_SLUG_TO_NAME

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
    
    def _make_json_serializable(self, obj: Any) -> Any:
        """
        Convert Python objects to JSON-serializable format
        Handles date objects and nested dictionaries
        """
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {key: self._make_json_serializable(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._make_json_serializable(item) for item in obj]
        else:
            return obj
    
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
            
            # OPTIMIZATION: Pre-load all existing PDF URLs to avoid repeated queries
            all_pdf_urls = []
            for item in mfn_items:
                all_pdf_urls.extend(item.pdf_urls)
            
            print(f"🚀 Batch checking {len(all_pdf_urls)} PDF URLs for duplicates...")
            existing_urls = await self._batch_check_existing_documents(db, all_pdf_urls)
            print(f"📊 Found {len(existing_urls)} existing URLs in database")
            
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

                    # Handle items with or without PDFs
                    # If no PDFs, create a single announcement record
                    pdf_urls_to_process = item.pdf_urls if item.pdf_urls else [None]

                    # Store each PDF URL as separate document (or single announcement if no PDFs)
                    for pdf_url in pdf_urls_to_process:
                        if pdf_url:
                            print(f"  📄 Processing PDF: {pdf_url}")

                            # OPTIMIZED: Use pre-loaded set instead of individual queries
                            if pdf_url in existing_urls:
                                print(f"  🔄 Skipping duplicate: {pdf_url}")
                                stats["duplicates"] += 1
                                continue
                        else:
                            print(f"  📢 Processing announcement (no PDF): {item.title[:60]}")
                        
                        # Prepare clean metadata (calendar info is stored separately)
                        raw_metadata = {
                            # Core document info
                            "pdf_url": pdf_url,
                            "mfn_source": item.source_url,
                            "discovery_date": datetime.utcnow().isoformat(),
                            "catalog_source": "mfn.se",
                            
                            # Document content metadata
                            "raw_title": item.title,
                            "content_preview": item.content[:300] if item.content else None,
                            "document_classification": item.document_type,
                            "publication_date": item.date_published.isoformat() if item.date_published else None,
                            
                            # Language and content indicators
                            "title_language": "swedish" if any(sw in item.title.lower() 
                                for sw in ["kvartal", "delårs", "år", "styrelse", "bolag"]) else "english",
                            
                            # Financial relevance indicators
                            "financial_keywords": self._extract_financial_keywords(item.title, item.content),
                            "contains_financial_data": self._has_financial_indicators(item.title, item.content),
                            
                            # Document characteristics
                            "title_length": len(item.title) if item.title else 0,
                            "content_length": len(item.content) if item.content else 0,
                            "has_calendar_events": bool(item.calendar_info),  # Just a boolean flag
                            
                            # LLM filtering hints
                            "llm_filter_context": {
                                "company": company.name,
                                "suggested_relevance": self._assess_relevance(item.title, item.content, item.document_type),
                                "key_phrases": self._extract_key_phrases(item.title, item.content),
                                "document_purpose": self._infer_document_purpose(item.title, item.content)
                            }
                        }
                        
                        # Convert all date objects to JSON-serializable format
                        json_safe_metadata = self._make_json_serializable(raw_metadata)
                        
                        # Create catalogued document entry
                        document = NordicDocument(
                            id=uuid.uuid4(),
                            company_id=company.id,
                            document_type=item.document_type,
                            report_period="Unknown",  # Default value for not-null constraint
                            title=item.title,
                            publish_date=item.date_published.date() if item.date_published else None,  # Extract date from MFN table
                            source_url=item.source_url,
                            storage_path=None,  # Not downloaded yet
                            file_hash=None,     # Not downloaded yet
                            language=company.reporting_language or "en",
                            ingestion_date=datetime.utcnow(),
                            processing_status="catalogued",  # Key status!
                            page_count=None,
                            file_size_mb=None,
                            metadata_=json_safe_metadata
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
        """
        Find company by name with comprehensive fuzzy matching.

        IMPORTANT: This searches company_master (source of truth) and returns
        a NordicCompany-like object with the correct company_id for storage.
        Handles stock class suffixes (A, B, Pref) by searching for base company.
        """
        print(f"🔍 Looking for company: '{company_name}'")

        # Strip stock class suffixes to find base company
        base_name = company_name
        stock_class_suffixes = [" Pref B", " Pref A", " Pref", " SDB", " A", " B", " C", " D"]
        for suffix in stock_class_suffixes:
            if base_name.endswith(suffix):
                base_name = base_name[:-len(suffix)].strip()
                print(f"📊 Stripped stock class: '{company_name}' → '{base_name}'")
                break

        # Search in company_master (source of truth) using raw SQL for flexibility
        search_names = [company_name, base_name] if base_name != company_name else [company_name]

        for search_name in search_names:
            # 1. Try exact match first
            result = await db.execute(
                text("""
                    SELECT id, company_name as name, primary_ticker as ticker, country
                    FROM company_master
                    WHERE LOWER(company_name) = LOWER(:name)
                    LIMIT 1
                """),
                {"name": search_name}
            )
            row = result.fetchone()
            if row:
                print(f"✅ Direct match in company_master: {row.name}")
                return self._row_to_company(row)

        # 2. Try pattern matching (company name contains search term)
        for search_name in search_names:
            result = await db.execute(
                text("""
                    SELECT id, company_name as name, primary_ticker as ticker, country
                    FROM company_master
                    WHERE LOWER(company_name) LIKE LOWER(:pattern)
                    ORDER BY LENGTH(company_name)
                    LIMIT 5
                """),
                {"pattern": f"%{search_name}%"}
            )
            rows = result.fetchall()
            if rows:
                # Pick the best match - prefer exact base name match, then shortest
                for row in rows:
                    row_base = row.name
                    for suffix in stock_class_suffixes:
                        if row_base.endswith(suffix):
                            row_base = row_base[:-len(suffix)].strip()
                            break
                    if row_base.lower() == base_name.lower():
                        print(f"✅ Base name match in company_master: {row.name}")
                        return self._row_to_company(row)

                # Otherwise pick shortest (most specific)
                best = rows[0]
                print(f"✅ Pattern match in company_master: {best.name}")
                return self._row_to_company(best)

        # 3. Try centralized slug mappings
        name_variations = COMPANY_SLUG_TO_NAME
        company_name_lower = company_name.lower().replace(' ', '-')

        if company_name_lower in name_variations:
            target_name = name_variations[company_name_lower]
            result = await db.execute(
                text("""
                    SELECT id, company_name as name, primary_ticker as ticker, country
                    FROM company_master
                    WHERE LOWER(company_name) = LOWER(:name)
                    LIMIT 1
                """),
                {"name": target_name}
            )
            row = result.fetchone()
            if row:
                print(f"✅ Slug mapping match: {row.name}")
                return self._row_to_company(row)

        # 4. Fallback to nordic_companies (legacy)
        result = await db.execute(
            select(NordicCompany).where(
                or_(
                    func.lower(NordicCompany.name) == func.lower(company_name),
                    func.lower(NordicCompany.name) == func.lower(base_name),
                    func.lower(NordicCompany.name).contains(base_name.lower())
                )
            ).order_by(func.length(NordicCompany.name)).limit(1)
        )
        company = result.scalar_one_or_none()
        if company:
            print(f"⚠️ Fallback to nordic_companies: {company.name}")
            return company

        print(f"❌ No match found for '{company_name}'")
        return None

    def _row_to_company(self, row) -> NordicCompany:
        """Convert a company_master row to a NordicCompany-like object for storage"""
        # Create a minimal NordicCompany object with the company_master ID
        company = NordicCompany(
            id=row.id,
            name=row.name,
            ticker=row.ticker if hasattr(row, 'ticker') else None,
            country=row.country if hasattr(row, 'country') else 'SE'
        )
        return company
        
    def _normalize_company_name(self, name: str) -> str:
        """Normalize MFN company name to database format"""
        import re
        
        # Start with the original name
        normalized = name
        
        # Convert from hyphen-separated to space-separated
        normalized = normalized.replace('-', ' ')
        
        # Title case each word
        normalized = ' '.join(word.capitalize() for word in normalized.split())
        
        # Handle special cases
        special_cases = {
            'Aac': 'AAC',
            'Aak': 'AAK', 
            'Abb': 'ABB',
            '2curex': '2cureX',
            'H&m': 'H&M'
        }
        
        for old, new in special_cases.items():
            normalized = re.sub(r'\b' + re.escape(old) + r'\b', new, normalized, flags=re.IGNORECASE)
        
        return normalized.strip()
    
    def _extract_search_terms(self, company_name: str) -> list:
        """Extract meaningful search terms from company name"""
        import re
        
        # Clean the name
        clean_name = company_name.replace('-', ' ').replace('_', ' ')
        
        # Split into terms and filter
        terms = []
        for term in clean_name.split():
            # Skip very short terms and common words
            if len(term) >= 3 and term.lower() not in ['the', 'and', 'for', 'ltd', 'inc']:
                terms.append(term)
        
        # Limit to most meaningful terms
        return terms[:3]
    
    async def _batch_check_existing_documents(
        self, 
        db: AsyncSession, 
        pdf_urls: List[str]
    ) -> set:
        """Batch check which PDF URLs already exist - MUCH faster than individual queries"""
        from sqlalchemy import text
        
        if not pdf_urls:
            return set()
        
        try:
            print(f"🚀 Executing batch query for {len(pdf_urls)} URLs...")
            
            # SIMPLIFIED: Just get all documents and filter in Python (for now)
            # This ensures the batch approach works, even if not perfectly optimized
            result = await db.execute(
                select(NordicDocument.metadata_.op('->>')('pdf_url')).where(
                    NordicDocument.metadata_.op('->>')('pdf_url').isnot(None)
                )
            )
            
            # Get all existing PDF URLs and create set intersection
            all_existing_urls = {row[0] for row in result.fetchall() if row[0]}
            existing_urls = all_existing_urls.intersection(set(pdf_urls))
            
            print(f"✅ Batch query found {len(existing_urls)} existing URLs out of {len(pdf_urls)} requested")
            return existing_urls
            
        except Exception as e:
            print(f"❌ Batch query failed: {e}")
            import traceback
            traceback.print_exc()
            print(f"🔄 Falling back to individual queries...")
            
            # Fallback to individual queries if batch fails
            existing_urls = set()
            for pdf_url in pdf_urls:
                try:
                    exists = await self._check_existing_document(db, pdf_url)
                    if exists:
                        existing_urls.add(pdf_url)
                except:
                    pass
            return existing_urls
    
    async def _check_existing_document(
        self, 
        db: AsyncSession, 
        pdf_url: str
    ) -> bool:
        """Check if PDF URL already catalogued - DEPRECATED: Use batch method instead"""
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