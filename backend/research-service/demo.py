"""
Demo Script for Research Service
Shows how to use the AI-powered company research features
"""
import asyncio
import json
from pathlib import Path

from services.document_service import DocumentService
from services.analysis_service import AnalysisService, AnalysisRequest
from services.embedding_service import EmbeddingService
from services.insight_service import InsightService
from processors.pdf_processor import PDFProcessor

async def demo_pdf_processing():
    """Demo PDF processing on real documents"""
    print("📄 PDF Processing Demo")
    print("=" * 50)
    
    # Find some PDF files
    data_dir = Path("../data/companies")
    pdf_files = list(data_dir.glob("**/*.pdf"))[:3]  # First 3 PDFs
    
    if not pdf_files:
        print("No PDF files found in data directory")
        return
    
    processor = PDFProcessor()
    
    for pdf_file in pdf_files:
        print(f"\nProcessing: {pdf_file.name}")
        
        # Process PDF
        result = await processor.process_pdf(str(pdf_file))
        
        print(f"  Pages: {result.total_pages}")
        print(f"  Language: {result.language}")
        print(f"  Chunks: {len(result.chunks)}")
        print(f"  Text preview: {result.full_text[:200]}...")
        
        if result.processing_errors:
            print(f"  Errors: {result.processing_errors}")


async def demo_embeddings():
    """Demo embedding generation"""
    print("\n🧠 Embeddings Demo")
    print("=" * 50)
    
    # Sample text chunks
    sample_texts = [
        "Volvo Group reported strong Q3 results with 15% revenue growth and improved margins.",
        "The company faces challenges from increased competition in the EV market.",
        "Management is optimistic about the long-term growth prospects in sustainable transport."
    ]
    
    embedding_service = EmbeddingService()
    
    # Create mock chunks
    from processors.pdf_processor import PDFChunk
    chunks = [
        PDFChunk(text=text, page_numbers=[1], chunk_index=i, metadata={})
        for i, text in enumerate(sample_texts)
    ]
    
    print("Generating embeddings...")
    embeddings = await embedding_service.generate_embeddings(chunks)
    
    for i, embedding in enumerate(embeddings):
        print(f"  Chunk {i}: {embedding.tokens} tokens, ${embedding.cost:.4f}")


async def demo_analysis():
    """Demo LLM analysis"""
    print("\n🤖 Analysis Demo")
    print("=" * 50)
    
    # Mock document data
    mock_documents = [{
        'id': 'doc-1',
        'title': 'Volvo Group Q3 2025 Report',
        'document_type': 'quarterly_report',
        'report_period': 'Q3 2025'
    }]
    
    # Mock chunks
    mock_chunks = [{
        'document_id': 'doc-1',
        'text': """
        Volvo Group Q3 2025 Results
        
        Revenue increased by 15% to SEK 35.2 billion compared to Q3 2024.
        EBITA margin improved to 18.5% from 16.2% in the previous quarter.
        
        Key highlights:
        - Strong demand for electric vehicles
        - Improved operational efficiency
        - Successful market expansion in Asia
        
        The company maintained its full-year guidance of 12-15% revenue growth.
        Management is confident about the transition to sustainable transport.
        
        Risks include supply chain disruptions and increased competition.
        """,
        'pages': [1, 2]
    }]
    
    analysis_service = AnalysisService()
    
    # Create analysis request
    request = AnalysisRequest(
        company_id="company-1",
        company_name="Volvo Group",
        analysis_type="comprehensive",
        documents=mock_documents,
        focus_areas=["financial_performance", "growth_strategy"]
    )
    
    print("Running analysis...")
    try:
        result = await analysis_service.analyze_company(request, mock_chunks)
        
        print(f"\nExecutive Summary:")
        print(result.executive_summary[:300] + "...")
        
        print(f"\nKey Insights ({len(result.insights)}):")
        for insight in result.insights[:3]:
            print(f"  • {insight.category}: {insight.insight[:100]}...")
        
        print(f"\nCost: ${result.cost:.4f}")
        print(f"Model: {result.model_used}")
        
    except Exception as e:
        print(f"Analysis failed: {e}")
        print("Note: This demo requires OpenAI API key in environment")


async def demo_search():
    """Demo semantic search"""
    print("\n🔍 Search Demo")
    print("=" * 50)
    
    embedding_service = EmbeddingService()
    
    # Demo query
    query = "What are the main growth drivers for the company?"
    
    print(f"Query: {query}")
    print("Note: Search requires vector database setup")
    
    # This would normally search the vector DB
    # For demo, just show query embedding generation
    query_embedding = await embedding_service._generate_query_embedding(query)
    
    if query_embedding:
        print(f"Generated query embedding: {len(query_embedding)} dimensions")
    else:
        print("Could not generate embedding (check API key)")


async def demo_insights():
    """Demo insight extraction"""
    print("\n💡 Insights Demo")
    print("=" * 50)
    
    insight_service = InsightService()
    
    # Mock analysis results
    from services.analysis_service import AnalysisResult, AnalysisInsight
    from datetime import datetime
    
    insights = [
        AnalysisInsight(
            category="financial",
            insight="Revenue growth of 15% demonstrates strong market demand",
            confidence=0.9,
            supporting_evidence=["Q3 revenue: SEK 35.2B vs Q3 2024: SEK 30.6B"],
            source_documents=["Q3 2025 Report"],
            metrics={"revenue_growth": 15.0}
        ),
        AnalysisInsight(
            category="strategic",
            insight="Electric vehicle transition is driving competitive advantage",
            confidence=0.8,
            supporting_evidence=["Strong EV demand", "Market expansion"],
            source_documents=["Q3 2025 Report"],
            metrics={"ev_market_share": 25.0}
        )
    ]
    
    result = AnalysisResult(
        request_id="demo",
        company_name="Volvo Group",
        analysis_type="comprehensive",
        executive_summary="Strong performance",
        insights=insights,
        key_metrics={},
        risk_assessment={},
        recommendations=[],
        model_used="demo",
        tokens_used=1000,
        cost=0.01,
        timestamp=datetime.now()
    )
    
    # Extract key insights
    categorized = insight_service.extract_key_insights([result])
    
    print("Categorized insights:")
    for category, insights_list in categorized.items():
        print(f"  {category}: {len(insights_list)} insights")
    
    # Rank insights
    ranked = insight_service.rank_insights_by_importance(insights)
    
    print("\nRanked insights:")
    for score, insight in ranked:
        print(f"  Score: {score:.2f} - {insight.insight[:80]}...")


async def main():
    """Run all demos"""
    print("🎯 YodaBuffett Research Service Demo")
    print("====================================")
    
    await demo_pdf_processing()
    await demo_embeddings()
    await demo_analysis()
    await demo_search()
    await demo_insights()
    
    print("\n✅ Demo complete!")
    print("\nTo start the service:")
    print("  python -m research-service.main")
    print("\nAPI will be available at: http://localhost:8002")
    print("API docs: http://localhost:8002/docs")


if __name__ == "__main__":
    asyncio.run(main())