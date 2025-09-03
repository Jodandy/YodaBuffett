#!/usr/bin/env python3
"""
Analyze the full AAK document to find all financial statement sections
"""
import asyncio
from financial_extraction_service_v2 import PDFTextExtractor
import re


async def analyze_full_document():
    """Analyze the complete document structure"""
    
    print("🔍 ANALYZING FULL AAK DOCUMENT")
    print("=" * 80)
    
    pdf_path = "/Users/jdandemar/Documents/YodaBuffett/backend/data/companies/SE/A/AAK/2025/quarterly_report/2025-07-17-aaks-interim-report-for-the-second-quarter-2025.pdf"
    
    extractor = PDFTextExtractor()
    pdf_data = await extractor.extract_text(pdf_path)
    text = pdf_data["full_text"]
    
    print(f"📄 Document length: {len(text)} characters")
    print(f"📄 Document lines: {len(text.split('\\n'))} lines")
    
    # Look for major financial statement sections
    print("\\n📊 SEARCHING FOR FINANCIAL STATEMENT SECTIONS:")
    print("-" * 60)
    
    sections_to_find = [
        ("Income Statement", ["income statement", "condensed income statement", "profit and loss"]),
        ("Balance Sheet", ["balance sheet", "statement of financial position", "condensed balance sheet"]),
        ("Cash Flow", ["cash flow", "statement of cash flows", "cash flows"]),
        ("Notes", ["notes to", "note 1", "accounting policies"]),
        ("Segment Information", ["segment", "operating segments", "business segments"]),
        ("Key Ratios", ["key ratios", "financial ratios", "key figures"]),
    ]
    
    for section_name, keywords in sections_to_find:
        print(f"\\n🎯 {section_name}:")
        found_sections = []
        
        for keyword in keywords:
            # Find all occurrences of this keyword
            matches = []
            start = 0
            while True:
                idx = text.lower().find(keyword.lower(), start)
                if idx == -1:
                    break
                matches.append(idx)
                start = idx + 1
            
            if matches:
                for idx in matches[:3]:  # Show first 3 matches
                    # Get line number
                    line_num = text[:idx].count('\\n') + 1
                    
                    # Get context
                    context_start = max(0, idx - 100)
                    context_end = min(len(text), idx + 200)
                    context = text[context_start:context_end].replace('\\n', ' ')
                    
                    found_sections.append((keyword, idx, line_num, context))
        
        if found_sections:
            for keyword, idx, line_num, context in found_sections[:2]:  # Show top 2
                print(f"  ✓ Found '{keyword}' at position {idx} (line {line_num})")
                print(f"    Context: ...{context}...")
        else:
            print(f"  ❌ Not found")
    
    # Look for specific table headers that indicate financial data
    print("\\n📈 SEARCHING FOR TABLE HEADERS:")
    print("-" * 60)
    
    table_patterns = [
        r"SEK million\\s+\\d{4}\\s+\\d{4}",  # Year comparison tables
        r"Q\\d\\s+\\d{4}\\s+Q\\d\\s+\\d{4}",  # Quarterly comparison
        r"\\d{4}\\s+\\d{4}\\s+\\d{4}",  # Three year comparison
        r"Total assets",
        r"Total equity",
        r"Net debt",
        r"Cash and cash equivalents",
        r"Capital expenditure",
        r"Free cash flow",
        r"EBITDA",
        r"Gross profit"
    ]
    
    for pattern in table_patterns:
        matches = list(re.finditer(pattern, text, re.IGNORECASE))
        if matches:
            print(f"\\n🎯 Pattern '{pattern}' found {len(matches)} times:")
            for match in matches[:2]:  # Show first 2
                idx = match.start()
                line_num = text[:idx].count('\\n') + 1
                context = text[max(0, idx-50):idx+150].replace('\\n', ' ')
                print(f"  Line {line_num}: ...{context}...")
    
    # Extract specific sections for detailed analysis
    print("\\n📋 EXTRACTING KEY SECTIONS:")
    print("-" * 60)
    
    # Find condensed income statement
    income_idx = text.lower().find("condensed income statement")
    if income_idx != -1:
        income_section = text[income_idx:income_idx+2000]
        print(f"\\n💰 CONDENSED INCOME STATEMENT (starting at pos {income_idx}):")
        print(income_section[:1000] + "..." if len(income_section) > 1000 else income_section)
    
    # Find balance sheet
    balance_idx = text.lower().find("balance sheet")
    if balance_idx != -1:
        balance_section = text[balance_idx:balance_idx+2000]
        print(f"\\n🏦 BALANCE SHEET (starting at pos {balance_idx}):")
        print(balance_section[:1000] + "..." if len(balance_section) > 1000 else balance_section)
    
    # Find cash flow
    cashflow_idx = text.lower().find("cash flow")
    if cashflow_idx != -1:
        cashflow_section = text[cashflow_idx:cashflow_idx+2000]
        print(f"\\n💸 CASH FLOW (starting at pos {cashflow_idx}):")
        print(cashflow_section[:1000] + "..." if len(cashflow_section) > 1000 else cashflow_section)
    
    # Look for numbers we know should exist but haven't found
    print("\\n🔢 SEARCHING FOR MISSING KEY NUMBERS:")
    print("-" * 60)
    
    # Balance sheet numbers (these should exist)
    search_numbers = [
        ("Total Assets", ["total assets", "sum assets", "assets total"]),
        ("Total Equity", ["total equity", "shareholders.* equity", "equity total"]),
        ("Net Debt", ["net debt", "total debt", "debt total"]),
        ("Cash", ["cash and", "cash &", "cash equivalents"]),
        ("EBITDA", ["ebitda"]),
        ("Free Cash Flow", ["free cash flow"]),
        ("Capex", ["capital expenditure", "capex", "investments"]),
    ]
    
    for metric_name, patterns in search_numbers:
        print(f"\\n🎯 {metric_name}:")
        for pattern in patterns:
            matches = list(re.finditer(pattern, text, re.IGNORECASE))
            if matches:
                for match in matches[:2]:
                    idx = match.start()
                    # Look for numbers in the next 200 characters
                    context = text[idx:idx+200]
                    numbers = re.findall(r'([\\d,]+)', context)
                    if numbers:
                        print(f"  Found near '{pattern}': {numbers[:3]}")
                        context_clean = context.replace('\\n', ' ')[:150]
                        print(f"    Context: {context_clean}...")
                break
    
    return text


if __name__ == "__main__":
    asyncio.run(analyze_full_document())