#!/usr/bin/env python3
"""
Test Swedish financial reports analysis
"""

import asyncio
import sys
import json
import re
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / 'src'))

from document_processor import DocumentProcessor
from llm_analyzer import LLMAnalyzer


async def analyze_swedish_report(file_path: str):
    """Analyze a Swedish financial report."""
    
    processor = DocumentProcessor()
    analyzer = LLMAnalyzer()
    
    print(f'📄 Processing {Path(file_path).name}...')
    doc = processor.process_file(file_path)
    
    # Extract company name from filename if not detected
    if not doc.company_name:
        filename = Path(file_path).stem
        doc.company_name = filename.replace('-', ' ').title()
    
    print(f'✅ Company: {doc.company_name}')
    print(f'✅ Total text: {len(doc.full_text):,} characters')
    
    # Take first part of document
    content = doc.full_text[:10000]
    
    prompt = f"""Analyze this Swedish financial report and extract key metrics.

CONTENT:
{content}

Extract financial data and return ONLY valid JSON:
{{
    "executive_summary": "Brief financial overview",
    "report_type": "Annual Report|Quarterly Report|Other",
    "period": "Reporting period",
    "earnings_metrics": {{
        "revenue": "Total revenue/Nettoomsättning",
        "revenue_growth": "Growth percentage",
        "order_intake": "Orderingång if available",
        "order_growth": "Order growth if available", 
        "operating_income": "EBIT/EBITA/Rörelseresultat",
        "operating_margin": "Operating margin %",
        "net_income": "Net result if available",
        "earnings_per_share": "EPS if available"
    }},
    "insights": [{{
        "insight": "Key finding from report",
        "supporting_evidence": "Specific data",
        "confidence": 0.85
    }}]
}}"""
    
    response = analyzer.client.chat.completions.create(
        model='gpt-4o-mini',
        messages=[
            {'role': 'system', 'content': 'You are a financial analyst. Return ONLY valid JSON, no markdown formatting.'},
            {'role': 'user', 'content': prompt}
        ],
        max_tokens=2000,
        temperature=0.1
    )
    
    result_text = response.choices[0].message.content.strip()
    
    # Clean up markdown if present
    if result_text.startswith('```'):
        result_text = re.sub(r'^```[a-z]*\n', '', result_text)
        result_text = re.sub(r'\n```$', '', result_text)
    
    try:
        result = json.loads(result_text)
        
        print(f"\n{'='*60}")
        print(f"{doc.company_name.upper()} FINANCIAL ANALYSIS")
        print(f"{'='*60}")
        
        print(f'\n📋 Report Type: {result.get("report_type", "Unknown")}')
        print(f'📅 Period: {result.get("period", "Unknown")}')
        
        print(f'\n📝 Executive Summary:')
        print(result.get('executive_summary', 'N/A'))
        
        metrics = result.get('earnings_metrics', {})
        if metrics:
            print(f'\n📊 Financial Metrics:')
            for key, value in metrics.items():
                if value and value != "N/A":
                    label = key.replace('_', ' ').title()
                    print(f'   {label}: {value}')
        
        insights = result.get('insights', [])
        if insights:
            print(f'\n💡 Key Insights:')
            for i, insight in enumerate(insights, 1):
                print(f'\n   {i}. {insight.get("insight", "N/A")}')
                evidence = insight.get("supporting_evidence", "N/A")
                if len(evidence) > 100:
                    evidence = evidence[:100] + '...'
                print(f'      Evidence: {evidence}')
                
    except json.JSONDecodeError as e:
        print(f'\n❌ JSON parsing error: {e}')
        print('Raw response:')
        print(result_text[:500] + '...')
        
    print(f'\n🧠 Tokens used: {response.usage.total_tokens:,}')


async def main():
    """Test multiple Swedish reports."""
    
    print("🇸🇪 SWEDISH FINANCIAL REPORTS ANALYSIS")
    print("="*60)
    
    # Test Alfa Laval
    try:
        await analyze_swedish_report('./data/alfalaval.pdf')
    except Exception as e:
        print(f'❌ Error analyzing Alfa Laval: {e}')
    
    print("\n" + "="*80 + "\n")
    
    # Test Inission  
    try:
        await analyze_swedish_report('./data/inission.pdf')
    except Exception as e:
        print(f'❌ Error analyzing Inission: {e}')


if __name__ == "__main__":
    asyncio.run(main())