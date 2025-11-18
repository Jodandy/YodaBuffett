#!/usr/bin/env python3
"""
Financial Section Parser CLI

Test the smart financial section parser on real Nordic documents
to see how well it identifies natural section boundaries.
"""

import asyncio
import logging
import sys
import os
import json
from pathlib import Path

# Add backend to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from domains.document_intelligence.services.financial_section_parser import FinancialSectionParser, SectionType
import asyncpg
from domains.document_intelligence.factory import get_database_url

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FinancialParserController:
    """Controller for testing financial section parsing"""
    
    def __init__(self):
        self.parser = FinancialSectionParser()
    
    async def get_test_documents(self, limit: int = 5, company_filter: str = None):
        """Get documents to test parsing on"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            where_clause = ""
            params = [limit]
            
            if company_filter:
                where_clause = "WHERE company_name ILIKE $2"
                params.append(f"%{company_filter}%")
            
            query = f"""
                SELECT id, company_name, form_type, year, 
                       LENGTH(extracted_text) as text_length
                FROM extracted_documents 
                {where_clause}
                ORDER BY LENGTH(extracted_text) DESC
                LIMIT $1
            """
            
            result = await conn.fetch(query, *params)
            return [dict(row) for row in result]
            
        finally:
            await conn.close()
    
    async def get_document_text(self, document_id: str) -> str:
        """Get the full text of a document"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            result = await conn.fetchrow("""
                SELECT extracted_text, company_name, form_type, year
                FROM extracted_documents 
                WHERE id = $1
            """, document_id)
            
            if result:
                return result['extracted_text'], {
                    'company': result['company_name'],
                    'form_type': result['form_type'], 
                    'year': result['year']
                }
            else:
                return None, None
                
        finally:
            await conn.close()
    
    async def test_parsing(self, limit: int = 3, company_filter: str = None):
        """Test section parsing on real documents"""
        logger.info("🔍 TESTING FINANCIAL SECTION PARSING")
        logger.info("=" * 50)
        
        try:
            # Get test documents
            documents = await self.get_test_documents(limit, company_filter)
            
            if not documents:
                logger.info("📭 No documents found for testing")
                return
            
            logger.info(f"🎯 Testing parsing on {len(documents)} documents")
            
            for i, doc in enumerate(documents, 1):
                logger.info(f"\n📄 Document {i}/{len(documents)}:")
                logger.info(f"   Company: {doc['company_name']}")
                logger.info(f"   Type: {doc['form_type']}")
                logger.info(f"   Year: {doc['year']}")
                logger.info(f"   Length: {doc['text_length']:,} characters")
                
                # Get full document text
                text, metadata = await self.get_document_text(doc['id'])
                
                if not text:
                    logger.warning(f"   ❌ Could not retrieve text")
                    continue
                
                # Parse the document
                result = self.parser.parse_document(text, doc['id'])
                
                if result['parsing_success']:
                    logger.info(f"   ✅ Found {result['total_sections']} sections")
                    
                    # Show section summary
                    for section in result['sections']:
                        section_name = section.section_type.value
                        confidence = section.confidence
                        content_length = len(section.content)
                        
                        logger.info(f"      {section_name:20} confidence: {confidence:.2f} length: {content_length:,} chars")
                        logger.info(f"      Title: '{section.title}'")
                    
                    # Show financial statements found
                    statements = self.parser.extract_financial_statements(result['sections'])
                    if statements:
                        logger.info(f"   💰 Financial statements found: {list(statements.keys())}")
                    
                else:
                    logger.warning(f"   ❌ No clear sections identified")
                
                # Show a preview of one interesting section
                interesting_sections = [s for s in result['sections'] 
                                     if s.section_type in [SectionType.BALANCE_SHEET, SectionType.INCOME_STATEMENT]
                                     and s.confidence > 0.7]
                
                if interesting_sections:
                    sample_section = interesting_sections[0]
                    preview = sample_section.content[:300].replace('\n', ' ')
                    logger.info(f"   🔍 Preview of {sample_section.section_type.value}:")
                    logger.info(f"      {preview}...")
        
        except Exception as e:
            logger.error(f"❌ Testing failed: {e}")
    
    async def parse_specific_document(self, document_id: str = None, company_filter: str = None):
        """Parse a specific document and show detailed results"""
        logger.info("📋 DETAILED DOCUMENT PARSING")
        logger.info("=" * 50)
        
        try:
            if not document_id:
                # Find a good document to parse
                documents = await self.get_test_documents(1, company_filter)
                if not documents:
                    logger.info("📭 No documents found")
                    return
                document_id = documents[0]['id']
                logger.info(f"🎯 Selected document: {documents[0]['company_name']} - {documents[0]['form_type']}")
            
            # Get document text
            text, metadata = await self.get_document_text(document_id)
            
            if not text:
                logger.error(f"❌ Could not retrieve document text")
                return
            
            logger.info(f"📄 Document: {metadata['company']} - {metadata['form_type']} ({metadata['year']})")
            logger.info(f"📏 Text length: {len(text):,} characters")
            
            # Parse the document
            result = self.parser.parse_document(text, document_id)
            
            logger.info(f"\n🔍 Parsing Results:")
            logger.info(f"   Total sections found: {result['total_sections']}")
            logger.info(f"   Parsing successful: {result['parsing_success']}")
            
            if result['sections']:
                logger.info(f"\n📋 Detailed Section Analysis:")
                
                for section in result['sections']:
                    logger.info(f"\n   Section: {section.section_type.value}")
                    logger.info(f"   Title: '{section.title}'")
                    logger.info(f"   Position: {section.start_pos:,} - {section.end_pos:,}")
                    logger.info(f"   Length: {len(section.content):,} characters")
                    logger.info(f"   Confidence: {section.confidence:.3f}")
                    
                    # Show content preview
                    preview = section.content[:200].replace('\n', ' ').strip()
                    if preview:
                        logger.info(f"   Content preview: {preview}...")
                    else:
                        logger.info(f"   Content preview: [Empty or whitespace only]")
                
                # Extract and show financial statements
                statements = self.parser.extract_financial_statements(result['sections'])
                if statements:
                    logger.info(f"\n💰 Core Financial Statements Identified:")
                    for statement_type, section in statements.items():
                        logger.info(f"   {statement_type:20} confidence: {section.confidence:.3f} length: {len(section.content):,}")
                
                # Show section type distribution
                logger.info(f"\n📊 Section Type Summary:")
                section_counts = {}
                for section in result['sections']:
                    section_type = section.section_type.value
                    section_counts[section_type] = section_counts.get(section_type, 0) + 1
                
                for section_type, count in sorted(section_counts.items()):
                    logger.info(f"   {section_type:20} {count} section(s)")
            else:
                logger.warning("❌ No sections could be identified in this document")
        
        except Exception as e:
            logger.error(f"❌ Detailed parsing failed: {e}")
    
    async def analyze_parsing_patterns(self, limit: int = 10):
        """Analyze parsing patterns across multiple documents"""
        logger.info("📊 ANALYZING PARSING PATTERNS")
        logger.info("=" * 50)
        
        try:
            documents = await self.get_test_documents(limit)
            
            if not documents:
                logger.info("📭 No documents found")
                return
            
            logger.info(f"🎯 Analyzing {len(documents)} documents")
            
            all_results = []
            section_type_counts = {}
            success_count = 0
            
            for doc in documents:
                text, metadata = await self.get_document_text(doc['id'])
                
                if not text:
                    continue
                
                result = self.parser.parse_document(text, doc['id'])
                all_results.append(result)
                
                if result['parsing_success']:
                    success_count += 1
                    
                    for section in result['sections']:
                        section_type = section.section_type.value
                        if section_type not in section_type_counts:
                            section_type_counts[section_type] = {
                                'count': 0, 
                                'total_confidence': 0.0,
                                'total_length': 0
                            }
                        
                        section_type_counts[section_type]['count'] += 1
                        section_type_counts[section_type]['total_confidence'] += section.confidence
                        section_type_counts[section_type]['total_length'] += len(section.content)
            
            # Show analysis results
            logger.info(f"\n📊 Analysis Results:")
            logger.info(f"   Documents processed: {len(all_results)}")
            logger.info(f"   Successful parses: {success_count} ({success_count/len(all_results)*100:.1f}%)")
            
            if section_type_counts:
                logger.info(f"\n📋 Section Type Statistics:")
                
                for section_type, stats in sorted(section_type_counts.items(), key=lambda x: x[1]['count'], reverse=True):
                    count = stats['count']
                    avg_confidence = stats['total_confidence'] / count
                    avg_length = stats['total_length'] / count
                    
                    logger.info(f"   {section_type:20} {count:3} occurrences, avg confidence: {avg_confidence:.2f}, avg length: {avg_length:,.0f}")
                
                # Financial statement coverage
                financial_statements = ['balance_sheet', 'income_statement', 'cash_flow', 'equity_statement']
                found_statements = [s for s in financial_statements if s in section_type_counts]
                
                logger.info(f"\n💰 Financial Statement Coverage:")
                logger.info(f"   Found {len(found_statements)}/{len(financial_statements)} core financial statements")
                
                for statement in financial_statements:
                    if statement in section_type_counts:
                        count = section_type_counts[statement]['count']
                        coverage = count / len(all_results) * 100
                        logger.info(f"   {statement:20} found in {count:2}/{len(all_results)} documents ({coverage:.1f}%)")
        
        except Exception as e:
            logger.error(f"❌ Pattern analysis failed: {e}")


async def main():
    """CLI entry point"""
    controller = FinancialParserController()
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python cli_financial_parser.py test [limit] [company]        # Test parsing on documents")
        print("  python cli_financial_parser.py parse [company]               # Detailed parse of one document") 
        print("  python cli_financial_parser.py analyze [limit]               # Analyze parsing patterns")
        print("")
        print("Examples:")
        print("  python cli_financial_parser.py test 3 Volvo")
        print("  python cli_financial_parser.py parse Ericsson")
        print("  python cli_financial_parser.py analyze 20")
        return
    
    command = sys.argv[1]
    
    if command == "test":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 3
        company_filter = sys.argv[3] if len(sys.argv) > 3 else None
        await controller.test_parsing(limit, company_filter)
        
    elif command == "parse":
        company_filter = sys.argv[2] if len(sys.argv) > 2 else None
        await controller.parse_specific_document(company_filter=company_filter)
        
    elif command == "analyze":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        await controller.analyze_parsing_patterns(limit)
        
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    asyncio.run(main())