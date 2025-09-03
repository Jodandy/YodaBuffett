"""
Local LLM Demo for Research Service
Shows how to use the AI research features with locally hosted Ollama models
"""
import asyncio
import json
from pathlib import Path

from services.local_llm_service import OllamaService, LocalAnalysisService
from processors.pdf_processor import PDFProcessor


async def demo_ollama_setup():
    """Demo Ollama setup and model availability"""
    print("🚀 Ollama Local LLM Setup")
    print("=" * 50)
    
    ollama = OllamaService()
    
    # Check connection
    print("Checking Ollama connection...")
    is_connected = await ollama.check_connection()
    
    if not is_connected:
        print("❌ Ollama is not running!")
        print("\nTo start Ollama:")
        print("  1. Install: https://ollama.ai/download")
        print("  2. Start server: ollama serve")
        print("  3. Pull model: ollama pull llama3.1:8b")
        return False
    
    print("✅ Ollama is running!")
    
    # List available models
    models = await ollama.list_models()
    print(f"\n📋 Available models ({len(models)}):")
    
    if not models:
        print("  No models installed")
        print("\nRecommended models for financial analysis:")
        for name, model in ollama.RECOMMENDED_MODELS.items():
            print(f"  • {name}: {model.size_gb}GB - {', '.join(model.good_for)}")
        
        print(f"\nTo install recommended model:")
        print(f"  ollama pull llama3.1:8b")
        return False
    
    for model in models:
        name = model.get("name", "unknown")
        size = model.get("size", 0) / (1024**3)  # Convert to GB
        print(f"  • {name}: {size:.1f}GB")
    
    return True


async def demo_local_analysis():
    """Demo local LLM analysis"""
    print("\n🤖 Local LLM Analysis Demo")
    print("=" * 50)
    
    # Initialize services
    ollama = OllamaService()
    analysis_service = LocalAnalysisService()
    
    # Check if we can proceed
    if not await ollama.check_connection():
        print("❌ Ollama not available. Skipping analysis demo.")
        return
    
    models = await ollama.list_models()
    if not models:
        print("❌ No models available. Skipping analysis demo.")
        return
    
    # Use first available model
    model_name = models[0]["name"]
    print(f"Using model: {model_name}")
    
    # Mock financial document text
    document_text = """
    Volvo Group Q3 2025 Results
    
    Revenue increased by 15% to SEK 35.2 billion compared to Q3 2024.
    EBITA margin improved to 18.5% from 16.2% in the previous quarter.
    
    Key highlights:
    - Strong demand for electric vehicles, with EV sales up 45%
    - Improved operational efficiency through automation
    - Successful market expansion in Asia-Pacific region
    - Order intake reached SEK 38.1 billion, up 12%
    
    Financial metrics:
    - Net income: SEK 4.2 billion (+22% YoY)
    - Operating cash flow: SEK 5.8 billion
    - Return on equity: 18.5%
    
    The company maintained its full-year guidance of 12-15% revenue growth.
    Management is confident about the transition to sustainable transport.
    
    Key risks include supply chain disruptions, increased competition in EV market,
    and potential economic slowdown affecting commercial vehicle demand.
    """
    
    print("Running comprehensive analysis...")
    try:
        result = await analysis_service.analyze_company_local(
            company_name="Volvo Group",
            documents_text=document_text,
            analysis_type="comprehensive",
            model=model_name
        )
        
        print(f"\n📊 Analysis Results:")
        print(f"Model: {result['model_used']}")
        print(f"Processing time: {result['processing_time']:.1f}s")
        print(f"Cost: ${result['cost']:.2f} (FREE!)")
        
        if "executive_summary" in result:
            print(f"\nExecutive Summary:")
            print(result["executive_summary"][:300] + "...")
        
        if "insights" in result and result["insights"]:
            print(f"\nKey Insights ({len(result['insights'])}):") 
            for insight in result["insights"][:3]:
                if isinstance(insight, dict):
                    category = insight.get("category", "general")
                    text = insight.get("insight", "No insight text")
                    confidence = insight.get("confidence", 0.5)
                    print(f"  • {category.title()}: {text[:100]}...")
                    print(f"    Confidence: {confidence:.1%}")
        
        if "key_metrics" in result and result["key_metrics"]:
            print(f"\nKey Metrics:")
            for metric, value in result["key_metrics"].items():
                print(f"  • {metric.replace('_', ' ').title()}: {value}")
        
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        print("This might be due to model limitations or formatting issues")


async def demo_model_setup():
    """Demo automatic model setup"""
    print("\n⚙️ Automatic Model Setup Demo")
    print("=" * 50)
    
    ollama = OllamaService()
    
    # Check if recommended model is available
    recommended_model = "llama3.1:8b"
    
    print(f"Setting up recommended model: {recommended_model}")
    print("This may take a few minutes if the model needs to be downloaded...")
    
    success = await ollama.setup_recommended_model(recommended_model)
    
    if success:
        print(f"✅ Model {recommended_model} is ready!")
        
        # Test quick generation
        print("\nTesting model...")
        response = await ollama.generate_text(
            "Explain what EBITA means in one sentence.",
            model=recommended_model
        )
        
        if response:
            print(f"Test response: {response[:150]}...")
        
    else:
        print(f"❌ Failed to set up {recommended_model}")


async def demo_pdf_with_local_llm():
    """Demo PDF processing with local LLM analysis"""
    print("\n📄 PDF + Local LLM Demo")
    print("=" * 50)
    
    # Look for PDFs
    data_dir = Path("../data/companies")
    pdf_files = list(data_dir.glob("**/*.pdf"))[:1]  # Just one PDF
    
    if not pdf_files:
        print("No PDF files found. Using mock data.")
        return await demo_local_analysis()
    
    pdf_file = pdf_files[0]
    print(f"Processing: {pdf_file.name}")
    
    # Process PDF
    processor = PDFProcessor()
    pdf_result = await processor.process_pdf(str(pdf_file))
    
    print(f"Extracted {len(pdf_result.full_text)} characters")
    print(f"Language: {pdf_result.language}")
    
    # Extract company name from path
    parts = pdf_file.parts
    company_name = "Unknown Company"
    for part in parts:
        if part not in ['data', 'companies', 'sweden', 'denmark', 'norway']:
            company_name = part.replace('-', ' ').title()
            break
    
    # Analyze with local LLM
    analysis_service = LocalAnalysisService()
    
    # Use first 8000 chars to stay within context limits
    text_sample = pdf_result.full_text[:8000]
    
    try:
        result = await analysis_service.analyze_company_local(
            company_name=company_name,
            documents_text=text_sample,
            analysis_type="financial"
        )
        
        print(f"\n📊 Analysis of {company_name}:")
        print(f"Processing time: {result['processing_time']:.1f}s")
        
        if "executive_summary" in result:
            print(f"\nSummary: {result['executive_summary'][:200]}...")
        
    except Exception as e:
        print(f"Analysis failed: {e}")


async def demo_question_answering():
    """Demo Q&A with local LLM"""
    print("\n❓ Question Answering Demo")
    print("=" * 50)
    
    ollama = OllamaService()
    
    if not await ollama.check_connection():
        print("❌ Ollama not available")
        return
    
    # Mock context
    context = """
    Volvo Group reported strong Q3 2025 results with 15% revenue growth.
    The company's electric vehicle sales increased by 45% year-over-year.
    Key risks include supply chain disruptions and increased EV competition.
    Management is optimistic about sustainable transport growth prospects.
    """
    
    questions = [
        "What was the revenue growth in Q3?",
        "How are electric vehicle sales performing?",
        "What are the main risks facing the company?"
    ]
    
    for question in questions:
        print(f"\n🤔 Q: {question}")
        
        try:
            result = await ollama.answer_question(
                question=question,
                context=context,
                company_name="Volvo Group"
            )
            
            print(f"💡 A: {result['answer'][:200]}...")
            
        except Exception as e:
            print(f"❌ Error: {e}")


def print_setup_instructions():
    """Print complete setup instructions"""
    print("\n📋 Complete Setup Instructions")
    print("=" * 60)
    
    print("1. Install Ollama:")
    print("   • Visit: https://ollama.ai/download")
    print("   • Download and install for your platform")
    
    print("\n2. Start Ollama server:")
    print("   ollama serve")
    
    print("\n3. Install recommended model:")
    print("   ollama pull llama3.1:8b")
    
    print("\n4. Verify installation:")
    print("   ollama list")
    
    print("\n5. Run this demo:")
    print("   python demo_local_llm.py")
    
    print("\n6. Integration with research service:")
    print("   • Update services/analysis_service.py to use LocalAnalysisService")
    print("   • Set USE_LOCAL_LLM=true in environment")
    print("   • Start research service: python -m research-service.main")
    
    print("\n💰 Benefits of Local LLM:")
    print("   • Zero ongoing costs (free inference)")
    print("   • Complete data privacy (no external API calls)")
    print("   • No rate limits or API quotas")
    print("   • Offline capability")
    
    print("\n📊 Model Recommendations:")
    print("   • llama3.1:8b (4.7GB) - Best balance of speed/quality")
    print("   • phi3:14b (7.9GB) - Efficient, good for analysis")
    print("   • mistral-nemo:12b (7.1GB) - Strong analytical capabilities")


async def main():
    """Run all local LLM demos"""
    print("🎯 YodaBuffett Research Service - Local LLM Demo")
    print("=" * 60)
    
    # Setup check
    is_ready = await demo_ollama_setup()
    
    if is_ready:
        await demo_local_analysis()
        await demo_question_answering()
        # await demo_pdf_with_local_llm()  # Uncomment if you have PDFs
    
    await demo_model_setup()
    
    print_setup_instructions()
    
    print("\n✅ Local LLM demo complete!")
    print("\n🚀 Next steps:")
    print("   1. Install and set up Ollama")
    print("   2. Pull recommended models")
    print("   3. Update research service configuration")
    print("   4. Enjoy free, private AI-powered financial analysis!")


if __name__ == "__main__":
    asyncio.run(main())