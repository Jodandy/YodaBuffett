#!/usr/bin/env python3
"""
Demo Analysis Result for Apple 10-K
Shows what the LLM analysis output would look like.
"""

import sys
sys.path.insert(0, './src')

from src.document_processor import DocumentProcessor
from src.llm_analyzer import AnalysisResult, AnalysisInsight, LLMAnalyzer
from datetime import datetime

def create_demo_result(doc):
    """Create a demo analysis result based on Apple 10-K sections."""
    
    # Demo insights based on typical Apple 10-K content
    insights = [
        AnalysisInsight(
            insight="Apple maintains strong diversification across product lines with iPhone generating majority revenue but Services segment showing fastest growth",
            supporting_evidence="iPhone revenue represents 52% of total revenue while Services grew 16% year-over-year, demonstrating reduced dependency on single product category",
            confidence=0.92,
            source_section="Financial Statements"
        ),
        AnalysisInsight(
            insight="Significant geographic revenue concentration in Americas and China creates exposure to trade policy and regional economic risks",
            supporting_evidence="Americas accounts for 42% and Greater China 19% of total revenue, making company vulnerable to US-China trade tensions",
            confidence=0.88,
            source_section="Business"
        ),
        AnalysisInsight(
            insight="Strong balance sheet with substantial cash reserves provides financial flexibility for strategic investments and shareholder returns",
            supporting_evidence="Cash and marketable securities exceed $150 billion, with minimal debt burden relative to cash generation capabilities",
            confidence=0.95,
            source_section="Financial Statements"
        ),
        AnalysisInsight(
            insight="Supply chain complexity and semiconductor dependencies present operational risks that could impact production and margins",
            supporting_evidence="Heavy reliance on third-party manufacturers and semiconductor suppliers creates vulnerability to supply disruptions and cost fluctuations",
            confidence=0.85,
            source_section="Risk Factors"
        ),
        AnalysisInsight(
            insight="Increasing focus on services ecosystem creates recurring revenue streams and higher-margin business model transformation",
            supporting_evidence="Services gross margin significantly exceeds product margins, with App Store, iCloud, and subscription services driving growth",
            confidence=0.90,
            source_section="Financial Statements"
        ),
        AnalysisInsight(
            insight="R&D investment levels indicate continued innovation focus but face pressure from intensifying competition in AI and emerging technologies",
            supporting_evidence="R&D spending increased to over $29 billion annually, representing 6%+ of revenue, with focus on AI, AR/VR, and autonomous systems",
            confidence=0.87,
            source_section="Financial Statements"
        )
    ]
    
    return AnalysisResult(
        analysis_type="comprehensive",
        company_name=doc.company_name or "Apple Inc.",
        filing_type=doc.filing_type or "10-K",
        insights=insights,
        executive_summary="Apple demonstrates strong financial performance with diversified revenue streams, though faces headwinds from supply chain complexity, geopolitical tensions, and intensifying competition. The company's transition toward services and focus on ecosystem integration position it well for sustained growth, while substantial cash reserves provide strategic flexibility.",
        risk_level="Medium",
        model_used="gpt-4o-mini (demo)",
        tokens_used=3847,
        analysis_date=datetime.now(),
        confidence_score=0.89
    )

def main():
    """Run demo analysis."""
    print("🚀 YodaBuffett MVP 1 - Demo Analysis")
    print("="*60)
    
    try:
        # Process the document
        processor = DocumentProcessor()
        print("📄 Processing Apple 10-K document...")
        doc = processor.process_file('./data/apple-10-k.pdf')
        
        print(f"✅ Document processed successfully:")
        print(f"   Company: {doc.company_name}")
        print(f"   Filing: {doc.filing_type}")
        print(f"   Full text: {len(doc.full_text):,} characters")
        print(f"   Sections identified: {len(doc.sections)}")
        
        for section in doc.sections:
            print(f"     • {section.name}: {len(section.content):,} chars")
        
        print("\n🤖 Generating AI analysis (demo)...")
        
        # Create demo result
        result = create_demo_result(doc)
        
        # Print using the same formatter
        analyzer = LLMAnalyzer()
        analyzer.print_analysis_result(result)
        
        print("\n💡 Analysis Features Demonstrated:")
        print("   ✅ Document processing (PDF extraction)")
        print("   ✅ Section identification (Financial Statements, Controls)")
        print("   ✅ LLM analysis framework (ready for API)")
        print("   ✅ Structured insights with confidence scores")
        print("   ✅ Risk assessment and executive summary")
        print("   ✅ Token usage tracking and cost estimation")
        
        print(f"\n💰 Estimated cost: ~$0.0077 (3,847 tokens × $0.002/1K)")
        print("\n🎯 Next Steps:")
        print("   • Add API credits to test live LLM analysis")
        print("   • Implement risk assessment and growth analysis modes")
        print("   • Add web interface for document upload")
        print("   • Expand to more document types (10-Q, 8-K)")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()