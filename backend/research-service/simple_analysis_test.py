"""
Simple Financial Analysis Test
Just test the basic analysis functionality
"""
import asyncio
import httpx
import json


async def test_financial_analysis():
    """Test basic financial analysis"""
    print("🎯 Simple Financial Analysis Test")
    print("=" * 40)
    
    base_url = "http://localhost:11434"
    
    # Simple financial document text
    document_text = """
    Company XYZ Q3 2024 Results:
    - Revenue: $50 million (up 20% from last year)
    - Profit margin: 15%
    - Cash flow: $8 million
    - Main risk: Market competition
    """
    
    # Simple analysis prompt
    prompt = f"""
    Analyze this financial document and provide a brief summary:
    
    {document_text}
    
    Please respond with:
    1. One sentence summary
    2. One key insight
    3. One risk
    
    Keep it short and simple.
    """
    
    request_data = {
        "model": "llama3:latest",
        "messages": [
            {"role": "system", "content": "You are a financial analyst. Be concise."},
            {"role": "user", "content": prompt}
        ],
        "stream": False
    }
    
    print("🤖 Running financial analysis...")
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            response = await client.post(
                f"{base_url}/api/chat",
                json=request_data
            )
            
            if response.status_code == 200:
                data = response.json()
                analysis = data.get("message", {}).get("content", "No response")
                
                print("✅ Analysis complete!")
                print("\n📊 Results:")
                print("-" * 30)
                print(analysis)
                print("-" * 30)
                
            else:
                print(f"❌ Analysis failed: {response.status_code}")
                print(response.text)
                
        except Exception as e:
            print(f"❌ Error: {e}")


if __name__ == "__main__":
    asyncio.run(test_financial_analysis())