#!/usr/bin/env python3
"""
Complete Analysis Flow Test Script
Tests the full pipeline: Document Processing → LLM Analysis → Results

Usage:
  python test_analysis.py                    # Analyze all files in data/
  python test_analysis.py filename.pdf       # Analyze specific file
  python test_analysis.py --type risk        # Use risk assessment mode
"""

import sys
import os
import asyncio
import argparse
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from document_processor import DocumentProcessor
from llm_analyzer import LLMAnalyzer

class AnalysisFlowTester:
    """Test runner for the complete analysis pipeline."""
    
    def __init__(self):
        self.processor = DocumentProcessor()
        self.analyzer = None  # Will initialize when needed
        self.data_dir = Path(__file__).parent / 'data'
    
    def initialize_analyzer(self):
        """Initialize the LLM analyzer, handling potential errors."""
        try:
            self.analyzer = LLMAnalyzer()
            return True
        except Exception as e:
            print(f"⚠️  LLM Analyzer not available: {e}")
            print("   Continuing with document processing only...")
            return False
    
    async def analyze_file(self, file_path, analysis_type="comprehensive"):
        """Run complete analysis on a single file."""
        
        print(f"\n{'='*80}")
        print(f"🔍 ANALYZING: {file_path.name}")
        print(f"{'='*80}")
        
        # Step 1: Document Processing
        print("📄 Step 1: Processing document...")
        try:
            doc = self.processor.process_file(file_path)
            
            print(f"✅ Document processed successfully:")
            print(f"   📋 Company: {doc.company_name or 'Unknown'}")
            print(f"   📊 Filing: {doc.filing_type or 'Unknown'}")
            print(f"   📝 Total text: {len(doc.full_text):,} characters")
            print(f"   📑 Sections found: {len(doc.sections)}")
            
            if doc.sections:
                for section in doc.sections:
                    print(f"      • {section.name}: {len(section.content):,} chars (confidence: {section.confidence:.1f})")
            
        except Exception as e:
            print(f"❌ Document processing failed: {e}")
            return None
        
        # Step 2: LLM Analysis
        if self.analyzer:
            print(f"\n🤖 Step 2: AI Analysis ({analysis_type} mode)...")
            try:
                result = await self.analyzer.analyze_document(doc, analysis_type)
                
                print(f"✅ AI analysis completed:")
                print(f"   🧠 Model: {result.model_used}")
                print(f"   🎯 Insights: {len(result.insights)} found")
                print(f"   🎲 Confidence: {result.confidence_score:.2f}")
                print(f"   ⚠️  Risk Level: {result.risk_level}")
                print(f"   💰 Tokens: {result.tokens_used:,} (~${result.tokens_used * 0.000002:.4f})")
                
                return result
                
            except Exception as e:
                print(f"❌ AI analysis failed: {e}")
                print("   (Document processing was successful)")
                return doc
        else:
            print("⚠️  Step 2: Skipping AI analysis (not available)")
            return doc
    
    async def run_analysis(self, file_filter=None, analysis_type="comprehensive"):
        """Run analysis on files in data directory."""
        
        print("🚀 YodaBuffett MVP 1 - Complete Analysis Flow")
        print("="*60)
        
        # Initialize components
        print("🔧 Initializing components...")
        print(f"   📄 Document processor: {', '.join(self.processor.supported_formats)}")
        llm_available = self.initialize_analyzer()
        if llm_available:
            print(f"   🤖 LLM analyzer: OpenAI GPT-4o-mini")
        
        # Find test files
        if file_filter:
            test_files = [self.data_dir / file_filter]
            test_files = [f for f in test_files if f.exists()]
        else:
            test_files = []
            for ext in ['.pdf', '.html', '.htm']:
                test_files.extend(self.data_dir.glob(f'*{ext}'))
        
        if not test_files:
            print(f"\n❌ No files found in {self.data_dir}")
            if file_filter:
                print(f"   Specifically looking for: {file_filter}")
            self.show_usage_help()
            return
        
        print(f"\n📁 Found {len(test_files)} file(s) to analyze:")
        for f in test_files:
            print(f"   • {f.name}")
        
        # Analyze each file
        results = []
        for file_path in test_files:
            try:
                result = await self.analyze_file(file_path, analysis_type)
                if result:
                    results.append((file_path.name, result))
            except Exception as e:
                print(f"❌ Error with {file_path.name}: {e}")
        
        # Summary
        if results:
            print(f"\n{'='*80}")
            print("📊 ANALYSIS SUMMARY")
            print(f"{'='*80}")
            
            for filename, result in results:
                if hasattr(result, 'insights'):  # Full analysis result
                    print(f"\n📋 {filename}:")
                    print(f"   Company: {result.company_name}")
                    print(f"   Risk Level: {result.risk_level}")
                    print(f"   Insights: {len(result.insights)}")
                    print(f"   Executive Summary: {result.executive_summary[:100]}...")
                else:  # Just document processing
                    print(f"\n📋 {filename}:")
                    print(f"   Company: {result.company_name or 'Unknown'}")
                    print(f"   Filing: {result.filing_type or 'Unknown'}")
                    print(f"   Sections: {len(result.sections)}")
        
        # Show full results
        if results and any(hasattr(r[1], 'insights') for r in results):
            print(f"\n{'='*80}")
            print("📈 DETAILED ANALYSIS RESULTS")
            print(f"{'='*80}")
            
            for filename, result in results:
                if hasattr(result, 'insights'):
                    print(f"\n🔍 {filename.upper()}")
                    self.analyzer.print_analysis_result(result)
    
    def show_usage_help(self):
        """Show usage instructions."""
        print(f"\n💡 Usage Instructions:")
        print(f"   1. Add a financial document to {self.data_dir}/")
        print(f"      • SEC 10-K/10-Q filings (PDF format recommended)")
        print(f"      • Annual reports from company websites")
        print(f"      • HTML filings from SEC EDGAR")
        print(f"   2. Run: python test_analysis.py")
        print(f"   3. Try different analysis modes:")
        print(f"      • python test_analysis.py --type comprehensive")
        print(f"      • python test_analysis.py --type risk")
        print(f"      • python test_analysis.py --type financial_health")
        print(f"\n📚 Example sources:")
        print(f"   • SEC EDGAR: https://www.sec.gov/edgar")
        print(f"   • Apple 10-K: https://investor.apple.com/sec-filings/")
        print(f"   • Tesla 10-K: https://ir.tesla.com/sec-filings")

async def main():
    """Main entry point with command line argument parsing."""
    
    parser = argparse.ArgumentParser(description='Test the complete analysis pipeline')
    parser.add_argument('file', nargs='?', help='Specific file to analyze (optional)')
    parser.add_argument('--type', default='comprehensive', 
                       choices=['comprehensive', 'risk', 'growth', 'financial_health'],
                       help='Analysis type to perform')
    
    args = parser.parse_args()
    
    tester = AnalysisFlowTester()
    await tester.run_analysis(args.file, args.type)

if __name__ == "__main__":
    # Handle the async main function
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⏹️  Analysis interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()