"""
Simple Demo for Research Service
Basic demo that doesn't require all dependencies
"""
import asyncio
import json
from pathlib import Path

async def demo_basic_features():
    """Demo basic research service concepts"""
    print("🎯 YodaBuffett Research Service Demo")
    print("====================================")
    
    print("\n📄 Document Processing Capabilities:")
    print("• Extract text from Swedish/English PDFs")
    print("• Parse financial metrics (revenue, EBITDA, margins)")
    print("• Break documents into semantic chunks")
    print("• Detect language automatically")
    
    print("\n🧠 AI Analysis Features:")
    print("• Comprehensive company analysis")
    print("• Financial performance evaluation") 
    print("• Risk assessment and competitive analysis")
    print("• Growth trajectory insights")
    print("• Swedish/English report support")
    
    print("\n🔍 Search Capabilities:")
    print("• Semantic search across all documents")
    print("• Find documents by meaning, not keywords")
    print("• Cross-document pattern recognition")
    print("• Question answering about companies")
    
    print("\n📊 Research Outputs:")
    print("• Executive summaries")
    print("• Structured insights with confidence scores")
    print("• Key metrics extraction")
    print("• Risk assessments")
    print("• Historical trend analysis")
    
    print("\n💰 Cost Efficiency:")
    print(f"• PDF Processing: ~$0.001 per document")
    print(f"• Embeddings: ~$0.01 per document (one-time)")
    print(f"• AI Analysis: ~$0.10-0.50 per analysis")
    print(f"• For 6,047 PDFs: ~$60 setup + $0.25/analysis")

def demo_analysis_example():
    """Show example analysis output"""
    print("\n🤖 Example Analysis Output")
    print("=" * 50)
    
    example_analysis = {
        "company_name": "Volvo Group",
        "analysis_type": "comprehensive",
        "executive_summary": """
        Volvo Group demonstrates strong financial performance in Q3 2025 with 15% revenue 
        growth and improved EBITA margins. The company's electric vehicle strategy is gaining 
        traction with increasing market share in sustainable transport solutions.
        """.strip(),
        "key_insights": [
            {
                "category": "financial",
                "insight": "Revenue growth of 15% to SEK 35.2B demonstrates strong market demand",
                "confidence": 0.9,
                "supporting_evidence": ["Q3 revenue increased from SEK 30.6B to SEK 35.2B"]
            },
            {
                "category": "strategic", 
                "insight": "Electric vehicle transition driving competitive advantage",
                "confidence": 0.85,
                "supporting_evidence": ["Strong EV demand", "Market expansion in Asia"]
            },
            {
                "category": "risk",
                "insight": "Supply chain disruptions remain a medium-term concern",
                "confidence": 0.8,
                "supporting_evidence": ["Management mentions supply constraints"]
            }
        ],
        "key_metrics": {
            "revenue_growth": "15%",
            "ebita_margin": "18.5%",
            "order_intake": "SEK 38.1B"
        },
        "risk_assessment": {
            "overall_risk": "medium",
            "key_risks": [
                {"risk": "Supply chain disruptions", "severity": "medium"},
                {"risk": "Increased EV competition", "severity": "medium"}
            ]
        },
        "cost": 0.25,
        "processing_time": 12.3
    }
    
    print(f"Company: {example_analysis['company_name']}")
    print(f"Analysis Type: {example_analysis['analysis_type']}")
    print(f"\nExecutive Summary:")
    print(example_analysis['executive_summary'])
    
    print(f"\nKey Insights ({len(example_analysis['key_insights'])}):")
    for insight in example_analysis['key_insights']:
        print(f"  • {insight['category'].title()}: {insight['insight']}")
        print(f"    Confidence: {insight['confidence']:.1%}")
    
    print(f"\nKey Metrics:")
    for metric, value in example_analysis['key_metrics'].items():
        print(f"  • {metric.replace('_', ' ').title()}: {value}")
    
    print(f"\nOverall Risk: {example_analysis['risk_assessment']['overall_risk'].title()}")
    
    print(f"\nAnalysis Cost: ${example_analysis['cost']:.2f}")
    print(f"Processing Time: {example_analysis['processing_time']:.1f}s")

def demo_api_usage():
    """Show API usage examples"""
    print("\n🚀 API Usage Examples")
    print("=" * 50)
    
    print("1. Analyze a Company:")
    print("""
    POST /api/v1/research/company/{company_id}/analyze
    {
        "analysis_type": "comprehensive",
        "years": [2024, 2025],
        "focus_areas": ["growth", "profitability", "ev_strategy"]
    }
    """)
    
    print("2. Search Documents:")
    print("""
    POST /api/v1/research/search  
    {
        "query": "electric vehicle strategy and market position",
        "company_id": "volvo-id",
        "limit": 5
    }
    """)
    
    print("3. Ask Questions:")
    print("""
    POST /api/v1/research/company/{company_id}/ask
    {
        "question": "What are the main risks mentioned in recent reports?",
        "include_sources": true
    }
    """)
    
    print("4. Get Metric Timeline:")
    print("""
    GET /api/v1/research/company/{company_id}/timeline?metric=revenue
    """)

async def demo_file_structure():
    """Show what we can process"""
    print("\n📁 Your Document Corpus")
    print("=" * 50)
    
    # Check data directory
    data_dir = Path("data/companies")
    if data_dir.exists():
        # Count PDFs
        pdf_count = len(list(data_dir.glob("**/*.pdf")))
        
        # Count companies
        company_dirs = [d for d in data_dir.glob("*/*") if d.is_dir()]
        
        print(f"📊 Documents Available:")
        print(f"  • Total PDFs: {pdf_count:,}")
        print(f"  • Companies: {len(company_dirs):,}")
        
        # Show some examples
        if company_dirs:
            print(f"\n📂 Sample Companies:")
            for company_dir in sorted(company_dirs)[:5]:
                country, company_name = company_dir.parts[-2:]
                pdf_files = list(company_dir.glob("**/*.pdf"))
                print(f"  • {company_name}: {len(pdf_files)} documents")
        
        # Show document types
        doc_types = set()
        for pdf in data_dir.glob("**/*.pdf"):
            if len(pdf.parts) >= 4:
                doc_type = pdf.parts[-2]  # document type directory
                doc_types.add(doc_type)
        
        if doc_types:
            print(f"\n📋 Document Types:")
            for doc_type in sorted(doc_types):
                print(f"  • {doc_type.replace('_', ' ').title()}")
                
    else:
        print("No document directory found at data/companies")
        print("The research service would process your Swedish financial PDFs")

async def main():
    """Run the demo"""
    await demo_basic_features()
    demo_analysis_example()
    demo_api_usage()
    await demo_file_structure()
    
    print("\n" + "=" * 60)
    print("✅ Research Service Ready!")
    print("=" * 60)
    print("\nTo start the service:")
    print("  1. Install dependencies: pip install -r research-service/requirements.txt")
    print("  2. Set OPENAI_API_KEY environment variable")
    print("  3. Run: python -m research-service.main")
    print("\nAPI will be available at: http://localhost:8002")
    print("Interactive docs: http://localhost:8002/docs")
    
    print(f"\n🎯 What you can do with your {6047} PDFs:")
    print("  • Generate embeddings for semantic search (~$60 one-time cost)")
    print("  • Analyze any Swedish company comprehensively (~$0.25 per analysis)")
    print("  • Ask questions about companies in natural language")
    print("  • Track financial metrics over time")
    print("  • Find cross-company patterns and trends")
    
    print(f"\n💡 Total setup cost for full corpus: ~$60")
    print(f"💡 Per-analysis cost: ~$0.25 (institutional-grade research!)")

if __name__ == "__main__":
    asyncio.run(main())