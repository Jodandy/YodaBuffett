#!/usr/bin/env python3
"""
Analyze exactly which fields are still null and search for them in the document
"""
import asyncio
from financial_extraction_service_production import ProductionFinancialExtractionService
from financial_extraction_service_v2 import PDFTextExtractor
import re


async def analyze_missing_fields():
    """Analyze which fields are missing and search for them"""
    
    print("🔍 ANALYZING MISSING FIELDS IN AAK DOCUMENT")
    print("=" * 80)
    
    # First get our current extraction results
    service = ProductionFinancialExtractionService()
    pdf_path = "/Users/jdandemar/Documents/YodaBuffett/backend/data/companies/SE/A/AAK/2025/quarterly_report/2025-07-17-aaks-interim-report-for-the-second-quarter-2025.pdf"
    
    metrics = await service.extract_from_pdf(pdf_path, "AAK")
    
    # Get the full document text for searching
    extractor = PDFTextExtractor()
    pdf_data = await extractor.extract_text(pdf_path)
    text = pdf_data["full_text"]
    
    print(f"📄 Document length: {len(text)} characters")
    
    # Define all possible fields and check which are missing
    all_fields = {
        # Revenue fields
        "revenue_adjusted": "Revenue (Adjusted)",
        "revenue_adjustments": "Revenue Adjustments", 
        "revenue_growth_pct": "Revenue Growth %",
        
        # Profitability fields
        "operating_profit_adjusted": "Operating Profit (Adjusted)",
        "operating_adjustments": "Operating Adjustments",
        "ebitda_adjusted": "EBITDA (Adjusted)",
        "ebitda_adjustments": "EBITDA Adjustments",
        
        # Cash flow fields
        "investing_cash_flow": "Investing Cash Flow",
        "financing_cash_flow": "Financing Cash Flow", 
        "free_cash_flow": "Free Cash Flow",
        "capex": "Capital Expenditures",
        
        # Per share fields
        "earnings_per_share_adjusted": "EPS (Adjusted)",
        "eps_adjustments": "EPS Adjustments",
        "shares_outstanding": "Shares Outstanding",
        
        # Ratios
        "current_ratio": "Current Ratio",
    }
    
    print(f"\n❌ MISSING FIELDS ANALYSIS:")
    print("-" * 60)
    
    missing_fields = []
    
    for field_name, display_name in all_fields.items():
        value = getattr(metrics, field_name, None)
        if value is None or value == [] or value == {} or value == "unknown" or value == "":
            missing_fields.append((field_name, display_name))
            print(f"  ❌ {display_name}")
    
    print(f"\n📊 Missing: {len(missing_fields)} out of {len(all_fields)} fields")
    
    # Now search for these missing fields in the document
    print(f"\n🔍 SEARCHING FOR MISSING FIELDS IN DOCUMENT:")
    print("-" * 60)
    
    # Search patterns for missing fields
    search_patterns = {
        "revenue_adjusted": [
            r"sales.*?excluding.*?([\\d,]+)",
            r"net sales.*?adjusted.*?([\\d,]+)",
            r"revenue.*?excluding.*?([\\d,]+)"
        ],
        "operating_profit_adjusted": [
            r"operating profit.*?excluding.*?([\\d,]+)",
            r"operating profit.*?excl.*?([\\d,]+)",
            r"adjusted operating profit.*?([\\d,]+)",
            r"operating profit.*?items affecting.*?([\\d,]+)"
        ],
        "ebitda_adjusted": [
            r"ebitda.*?excluding.*?([\\d,]+)", 
            r"ebitda.*?adjusted.*?([\\d,]+)",
            r"ebitda.*?excl.*?([\\d,]+)"
        ],
        "earnings_per_share_adjusted": [
            r"earnings per share.*?excluding.*?([\\d.,]+)",
            r"eps.*?excluding.*?([\\d.,]+)",
            r"earnings per share.*?excl.*?([\\d.,]+)",
            r"per share equaled sek\\s*([\\d.,]+).*?excluding"
        ],
        "investing_cash_flow": [
            r"cash flow from investing.*?([\\d,-]+)",
            r"investing.*?cash flow.*?([\\d,-]+)",
            r"investments.*?cash.*?([\\d,-]+)"
        ],
        "financing_cash_flow": [
            r"cash flow from financing.*?([\\d,-]+)",
            r"financing.*?cash flow.*?([\\d,-]+)",
            r"financing activities.*?([\\d,-]+)"
        ],
        "free_cash_flow": [
            r"free cash flow.*?([\\d,-]+)",
            r"fcf.*?([\\d,-]+)",
            r"operating cash flow.*?capex.*?([\\d,-]+)"
        ],
        "capex": [
            r"capital expenditure.*?([\\d,]+)",
            r"capex.*?([\\d,]+)",
            r"investments.*?property.*?([\\d,]+)",
            r"capital expenditure was.*?([\\d,]+)"
        ],
        "shares_outstanding": [
            r"shares outstanding.*?([\\d,]+)",
            r"number of shares.*?([\\d,]+)",
            r"outstanding shares.*?([\\d,]+)",
            r"shares.*?([\\d,]+)\\s*million"
        ],
        "current_ratio": [
            r"current ratio.*?([\\d.]+)",
            r"current assets.*?current liabilities.*?([\\d.]+)"
        ]
    }
    
    found_data = {}
    
    for field, patterns in search_patterns.items():
        print(f"\n🎯 Searching for {field}:")
        field_found = False
        
        for pattern in patterns:
            matches = list(re.finditer(pattern, text, re.IGNORECASE | re.DOTALL))
            if matches:
                field_found = True
                for match in matches[:3]:  # Show first 3 matches
                    value = match.group(1)
                    pos = match.start()
                    context_start = max(0, pos - 100)
                    context_end = min(len(text), pos + 200)
                    context = text[context_start:context_end].replace('\\n', ' ')
                    
                    print(f"  ✓ Found: {value}")
                    print(f"    Pattern: {pattern}")
                    print(f"    Context: ...{context}...")
                    
                    # Store the best match
                    if field not in found_data:
                        found_data[field] = value
                break
        
        if not field_found:
            print(f"  ❌ Not found with any pattern")
    
    # Look for cash flow statement specifically
    print(f"\n💸 CASH FLOW STATEMENT ANALYSIS:")
    print("-" * 60)
    
    cashflow_sections = []
    for match in re.finditer(r'cash flow.*?statement|statement.*?cash flow', text, re.IGNORECASE):
        pos = match.start()
        section = text[pos:pos+1000]
        cashflow_sections.append((pos, section))
        print(f"Cash flow section at position {pos}:")
        print(section[:300] + "...")
    
    # Look for detailed cash flow data
    cash_flow_items = [
        "Cash flow from operating activities",
        "Cash flow from investing activities", 
        "Cash flow from financing activities",
        "Capital expenditure",
        "Free cash flow",
        "Dividends paid",
        "Change in working capital"
    ]
    
    for item in cash_flow_items:
        matches = list(re.finditer(item, text, re.IGNORECASE))
        if matches:
            print(f"\n✓ Found '{item}' {len(matches)} times:")
            for match in matches[:2]:
                pos = match.start()
                context = text[pos:pos+150].replace('\\n', ' ')
                numbers = re.findall(r'([\\d,.-]+)', context)
                print(f"  Numbers nearby: {numbers[:5] if numbers else 'None'}")
                print(f"  Context: {context}...")
    
    # Summary of what we found
    print(f"\n📈 SEARCH RESULTS SUMMARY:")
    print("-" * 60)
    print(f"✅ Found patterns for: {len(found_data)} fields")
    for field, value in found_data.items():
        print(f"  • {field}: {value}")
    
    print(f"\n❌ Still missing patterns for: {len(missing_fields) - len(found_data)} fields")
    missing_not_found = [f for f, _ in missing_fields if f not in found_data]
    for field in missing_not_found:
        print(f"  • {field}")
    
    return found_data


if __name__ == "__main__":
    asyncio.run(analyze_missing_fields())