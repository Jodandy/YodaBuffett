#!/usr/bin/env python3
"""
Financial Report Section Classifier

Uses OpenAI to intelligently classify chunks of Nordic financial reports
into standard financial sections (balance sheet, income statement, etc.)
"""

import asyncio
import logging
import json
import os
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from openai import AsyncOpenAI

import asyncpg
from ..factory import get_database_url

logger = logging.getLogger(__name__)


class FinancialSectionClassifier:
    """Classifies financial report chunks using OpenAI"""
    
    # Standard financial report sections
    SECTION_TYPES = {
        'balance_sheet': 'Balance Sheet - Assets, liabilities, equity, financial position',
        'income_statement': 'Income Statement - Revenue, expenses, profit/loss, EBIT, net income',
        'cash_flow': 'Cash Flow Statement - Operating, investing, financing cash flows',
        'equity_statement': 'Statement of Changes in Equity - Shareholder equity movements',
        'management_discussion': 'Management Discussion & Analysis - CEO letter, business review',
        'strategy': 'Strategic Direction - Future plans, outlook, strategic initiatives',
        'operations': 'Business Operations - Operational performance, segments, business units',
        'market_analysis': 'Market & Industry Analysis - Competition, market conditions',
        'risk_factors': 'Risk Factors - Risk identification, risk management, uncertainties',
        'corporate_governance': 'Corporate Governance - Board composition, governance practices',
        'sustainability': 'Sustainability & ESG - Environmental, social, governance metrics',
        'accounting_policies': 'Accounting Policies - Accounting principles, methods, standards',
        'notes': 'Notes to Financial Statements - Additional disclosures, explanations',
        'auditor_report': 'Auditor Report - Independent auditor findings and opinion',
        'other': 'Other content - General information, disclaimers, contact info'
    }
    
    def __init__(self, api_key: str = None, model: str = "gpt-4o-mini"):
        self.client = AsyncOpenAI(api_key=api_key) if api_key else None
        self.model = model
        
        if not self.client:
            logger.warning("No OpenAI API key provided - classification will not work")
    
    def _build_classification_prompt(self, chunk_text: str, context: Dict = None) -> str:
        """Build the classification prompt for OpenAI"""
        
        # Truncate text if too long (keep first 1000 chars for context)
        if len(chunk_text) > 1000:
            display_text = chunk_text[:1000] + "..."
        else:
            display_text = chunk_text
            
        section_list = '\n'.join([f"- {key}: {desc}" for key, desc in self.SECTION_TYPES.items()])
        
        prompt = f"""You are analyzing a chunk from a Nordic financial report. Classify this text into one of these financial report sections:

{section_list}

Context information:
{json.dumps(context, indent=2) if context else "No additional context provided"}

Text to classify:
\"\"\"{display_text}\"\"\"

Instructions:
1. Choose the MOST APPROPRIATE section type from the list above
2. Provide a confidence score between 0.0 and 1.0
3. Consider Nordic financial reporting standards (IFRS, local practices)
4. Look for key indicators like financial statement items, headers, content themes

Respond in this exact JSON format:
{{
    "section_type": "balance_sheet",
    "confidence": 0.85,
    "reasoning": "Contains typical balance sheet items like assets, liabilities, and equity"
}}"""

        return prompt
    
    async def classify_chunk(
        self, 
        chunk_text: str, 
        context: Dict = None
    ) -> Tuple[str, float, str]:
        """
        Classify a single chunk
        
        Returns:
            (section_type, confidence_score, reasoning)
        """
        if not self.client:
            logger.warning("No OpenAI client available")
            return "other", 0.0, "No API key provided"
            
        if not chunk_text.strip():
            return "other", 1.0, "Empty chunk"
            
        try:
            prompt = self._build_classification_prompt(chunk_text, context)
            
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,  # Low temperature for consistent classification
                max_tokens=200
            )
            
            response_text = response.choices[0].message.content.strip()
            
            # Parse JSON response
            try:
                result = json.loads(response_text)
                section_type = result.get('section_type', 'other')
                confidence = float(result.get('confidence', 0.0))
                reasoning = result.get('reasoning', 'No reasoning provided')
                
                # Validate section type
                if section_type not in self.SECTION_TYPES:
                    logger.warning(f"Unknown section type: {section_type}, defaulting to 'other'")
                    section_type = 'other'
                    confidence = 0.1
                    
                logger.info(f"Classified as {section_type} (confidence: {confidence:.2f})")
                return section_type, confidence, reasoning
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse OpenAI response: {response_text}")
                return "other", 0.0, f"JSON parse error: {str(e)}"
                
        except Exception as e:
            logger.error(f"OpenAI classification error: {str(e)}")
            return "other", 0.0, f"API error: {str(e)}"
    
    async def get_unclassified_chunks(
        self, 
        limit: int = 10,
        document_filter: str = None
    ) -> List[Dict]:
        """Get chunks that haven't been classified yet"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            where_clause = ""
            params = [limit]
            
            if document_filter:
                where_clause = "AND ed.company_name ILIKE $2"
                params.append(f"%{document_filter}%")
            
            query = f"""
                SELECT edc.id, edc.extracted_document_id, edc.chunk_index, 
                       edc.chunk_text, edc.page_numbers, edc.char_start, edc.char_end,
                       ed.company_name, ed.form_type, ed.year
                FROM extracted_document_chunks edc
                JOIN extracted_documents ed ON edc.extracted_document_id = ed.id
                WHERE edc.section_type IS NULL {where_clause}
                ORDER BY ed.company_name, edc.chunk_index
                LIMIT $1
            """
            
            result = await conn.fetch(query, *params)
            return [dict(row) for row in result]
            
        finally:
            await conn.close()
    
    async def update_chunk_classification(
        self,
        chunk_id: str,
        section_type: str, 
        confidence: float,
        reasoning: str = None
    ):
        """Update a chunk with its classification"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            await conn.execute("""
                UPDATE extracted_document_chunks 
                SET section_type = $2, section_confidence = $3, 
                    classification_reasoning = $4, classified_at = $5
                WHERE id = $1
            """, chunk_id, section_type, confidence, reasoning, datetime.now())
            
            logger.info(f"Updated chunk {chunk_id} as {section_type} (confidence: {confidence:.2f})")
            
        finally:
            await conn.close()
    
    async def classify_document_chunks(
        self, 
        document_id: str,
        dry_run: bool = False
    ) -> Dict:
        """Classify all chunks for a specific document"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            # Get all chunks for this document
            chunks = await conn.fetch("""
                SELECT edc.id, edc.chunk_index, edc.chunk_text, 
                       edc.page_numbers, edc.char_start, edc.char_end,
                       ed.company_name, ed.form_type, ed.year
                FROM extracted_document_chunks edc
                JOIN extracted_documents ed ON edc.extracted_document_id = ed.id
                WHERE edc.extracted_document_id = $1
                ORDER BY edc.chunk_index
            """, document_id)
            
            if not chunks:
                return {"success": False, "error": "No chunks found"}
            
            results = []
            total_cost = 0.0
            
            logger.info(f"Classifying {len(chunks)} chunks for document {document_id}")
            
            for chunk in chunks:
                chunk_dict = dict(chunk)
                
                # Build context for better classification
                context = {
                    "company": chunk_dict['company_name'],
                    "document_type": chunk_dict['form_type'],
                    "year": chunk_dict['year'],
                    "chunk_index": chunk_dict['chunk_index'],
                    "total_chunks": len(chunks),
                    "pages": chunk_dict['page_numbers']
                }
                
                # Classify the chunk
                section_type, confidence, reasoning = await self.classify_chunk(
                    chunk_dict['chunk_text'], 
                    context
                )
                
                result = {
                    "chunk_id": chunk_dict['id'],
                    "chunk_index": chunk_dict['chunk_index'],
                    "section_type": section_type,
                    "confidence": confidence,
                    "reasoning": reasoning
                }
                results.append(result)
                
                # Estimate cost (rough: ~150 tokens input + 50 output = 200 tokens * $0.15/1M)
                estimated_cost = 200 * 0.00000015  # $0.00003 per classification
                total_cost += estimated_cost
                
                # Update database if not dry run
                if not dry_run:
                    await self.update_chunk_classification(
                        chunk_dict['id'], section_type, confidence, reasoning
                    )
                
                # Rate limiting - small delay between API calls
                await asyncio.sleep(0.5)
            
            return {
                "success": True,
                "document_id": document_id,
                "chunks_classified": len(results),
                "results": results,
                "estimated_cost": total_cost
            }
            
        finally:
            await conn.close()
    
    async def get_classification_statistics(self) -> Dict:
        """Get classification statistics"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_chunks,
                    COUNT(section_type) as classified_chunks,
                    COUNT(*) - COUNT(section_type) as unclassified_chunks,
                    AVG(section_confidence) as avg_confidence
                FROM extracted_document_chunks
            """)
            
            # Section distribution
            sections = await conn.fetch("""
                SELECT section_type, COUNT(*) as count,
                       AVG(section_confidence) as avg_confidence
                FROM extracted_document_chunks 
                WHERE section_type IS NOT NULL
                GROUP BY section_type
                ORDER BY count DESC
            """)
            
            return {
                "overall": dict(stats) if stats else {},
                "section_distribution": [dict(row) for row in sections]
            }
            
        finally:
            await conn.close()


# Database schema updates needed
async def add_classification_columns():
    """Add classification columns to extracted_document_chunks table"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        # Check if columns already exist
        existing_columns = await conn.fetch("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'extracted_document_chunks'
            AND column_name IN ('section_type', 'section_confidence', 'classification_reasoning', 'classified_at')
        """)
        
        existing_column_names = [row['column_name'] for row in existing_columns]
        
        # Add missing columns
        if 'section_type' not in existing_column_names:
            await conn.execute("ALTER TABLE extracted_document_chunks ADD COLUMN section_type VARCHAR(50)")
            logger.info("Added section_type column")
            
        if 'section_confidence' not in existing_column_names:
            await conn.execute("ALTER TABLE extracted_document_chunks ADD COLUMN section_confidence FLOAT")
            logger.info("Added section_confidence column")
            
        if 'classification_reasoning' not in existing_column_names:
            await conn.execute("ALTER TABLE extracted_document_chunks ADD COLUMN classification_reasoning TEXT")
            logger.info("Added classification_reasoning column")
            
        if 'classified_at' not in existing_column_names:
            await conn.execute("ALTER TABLE extracted_document_chunks ADD COLUMN classified_at TIMESTAMP")
            logger.info("Added classified_at column")
        
        # Add index for efficient section queries
        try:
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_chunks_section_type ON extracted_document_chunks(section_type)")
            logger.info("Added section_type index")
        except Exception as e:
            logger.warning(f"Index creation warning: {e}")
            
    finally:
        await conn.close()


if __name__ == "__main__":
    # For testing
    async def test_classifier():
        # Add database columns first
        await add_classification_columns()
        
        # Test classification
        api_key = os.getenv('OPENAI_API_KEY')
        classifier = FinancialSectionClassifier(api_key=api_key)
        
        # Get classification statistics
        stats = await classifier.get_classification_statistics()
        print("📊 Classification Statistics:", json.dumps(stats, indent=2, default=str))
        
        # Test on a few unclassified chunks
        # chunks = await classifier.get_unclassified_chunks(limit=2)
        # print(f"\n📄 Found {len(chunks)} unclassified chunks for testing")
        
        # for chunk in chunks:
        #     result = await classifier.classify_chunk(
        #         chunk['chunk_text'][:500], 
        #         {"company": chunk['company_name']}
        #     )
        #     print(f"Chunk {chunk['chunk_index']}: {result}")
    
    asyncio.run(test_classifier())