#!/usr/bin/env python3
"""
Analyze Atlas Copco PDF text to find missed financial data
"""
import asyncio
from financial_extraction_service_v2 import PDFTextExtractor
import re

async def analyze_atlas_text():
    """Analyze what financial data exists in Atlas Copco PDF"""
    
    print("🔍 ANALYZING ATLAS COPCO PDF TEXT FOR MISSED FINANCIAL DATA")
    print("=" * 80)
    
    pdf_path = "/Users/jdandemar/Documents/YodaBuffett/backend/data/companies/SE/A/Atlas_Copco_AB/2025/quarterly_report/2025-07-18-atlas-copco-rapport-för-andra-kvartalet-2025.pdf"
    
    extractor = PDFTextExtractor()
    text_result = await extractor.extract_text(pdf_path)
    text = text_result.get("full_text", "") if text_result else ""
    
    print(f"📄 Extracted text length: {len(text)} characters")
    
    # Search for potential balance sheet items
    balance_sheet_terms = [
        'total assets', 'totala tillgångar', 'balansomslutning',
        'current assets', 'omsättningstillgångar',
        'equity', 'eget kapital', 'total equity',
        'liabilities', 'skulder', 'total liabilities',
        'cash', 'kassa', 'likvida medel', 'cash and cash equivalents',
        'inventory', 'varulager', 'lager',
        'receivables', 'fordringar', 'kundfordringar'
    ]
    
    print(f"\n🏦 BALANCE SHEET DATA SEARCH:")
    for term in balance_sheet_terms:
        # Search for term with numbers
        pattern = f"({term}).*?([0-9,\\s]+).*?(?:MSEK|SEK|miljoner|Mkr)"
        matches = re.findall(pattern, text, re.IGNORECASE)
        if matches:
            print(f"   ✅ Found {term.upper()}:")
            for match in matches[:3]:  # Show first 3 matches
                print(f"      {match[0]}: {match[1]}")
    
    # Search for cash flow items
    cash_flow_terms = [
        'operating cash flow', 'kassaflöde från den löpande verksamheten',
        'investing cash flow', 'kassaflöde från investeringsverksamheten',
        'financing cash flow', 'kassaflöde från finansieringsverksamheten',
        'free cash flow', 'fritt kassaflöde',
        'capex', 'capital expenditure', 'investeringar'
    ]
    
    print(f"\n💰 CASH FLOW DATA SEARCH:")
    for term in cash_flow_terms:
        pattern = f"({term}).*?([0-9,\\s-]+).*?(?:MSEK|SEK|miljoner|Mkr)"
        matches = re.findall(pattern, text, re.IGNORECASE)
        if matches:
            print(f"   ✅ Found {term.upper()}:")
            for match in matches[:3]:
                print(f"      {match[0]}: {match[1]}")
    
    # Search for per-share data
    per_share_terms = [
        'earnings per share', 'resultat per aktie',
        'dividend per share', 'utdelning per aktie', 
        'book value per share', 'bokfört värde per aktie',
        'shares outstanding', 'antal aktier', 'utestående aktier'
    ]
    
    print(f"\n📈 PER-SHARE DATA SEARCH:")
    for term in per_share_terms:
        pattern = f"({term}).*?([0-9,.-]+).*?(?:SEK|kr|million|miljoner)?"
        matches = re.findall(pattern, text, re.IGNORECASE)
        if matches:
            print(f"   ✅ Found {term.upper()}:")
            for match in matches[:3]:
                print(f"      {match[0]}: {match[1]}")
    
    # Search for additional income statement items
    income_terms = [
        'gross profit', 'bruttovinst', 'bruttoresultat',
        'cost of goods sold', 'kostnad sålda varor',
        'operating expenses', 'rörelsekostnader',
        'depreciation', 'avskrivningar',
        'amortization', 'goodwill'
    ]
    
    print(f"\n📊 ADDITIONAL INCOME STATEMENT DATA:")
    for term in income_terms:
        pattern = f"({term}).*?([0-9,\\s-]+).*?(?:MSEK|SEK|miljoner|Mkr)"
        matches = re.findall(pattern, text, re.IGNORECASE)
        if matches:
            print(f"   ✅ Found {term.upper()}:")
            for match in matches[:3]:
                print(f"      {match[0]}: {match[1]}")
    
    # Look for structured tables with numbers
    print(f"\n📋 STRUCTURED FINANCIAL TABLES:")
    lines = text.split('\n')
    table_lines = []
    
    for line in lines:
        # Look for lines with multiple numbers that might be financial tables
        if re.search(r'(\d+[,\s]*\d+.*?){2,}(?:MSEK|SEK|Mkr)', line, re.IGNORECASE):
            table_lines.append(line.strip())
    
    print(f"   Found {len(table_lines)} potential table lines:")
    for i, line in enumerate(table_lines[:10]):  # Show first 10
        if len(line) > 20:  # Only show substantial lines
            print(f"   {i+1:2d}: {line[:120]}...")
    
    # Search for quarterly comparison data
    print(f"\n📅 QUARTERLY/PERIOD COMPARISON DATA:")
    quarterly_patterns = [
        r'(Q1|Q2|Q3|Q4|First|Second|Third|Fourth).*?(\d+[,\s]*\d*).*?(?:MSEK|SEK)',
        r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec).*?(\d+[,\s]*\d*).*?(?:MSEK|SEK)',
        r'(2024|2025).*?(\d+[,\s]*\d*).*?(?:MSEK|SEK)'
    ]
    
    for pattern in quarterly_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        if matches:
            print(f"   Period data found: {len(matches)} matches")
            for match in matches[:5]:  # Show first 5
                print(f"      {match[0]}: {match[1]}")

if __name__ == "__main__":
    asyncio.run(analyze_atlas_text())