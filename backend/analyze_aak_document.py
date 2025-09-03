"""
Analyze AAK Document Structure
Find actual locations of financial data
"""
import asyncio
from financial_extraction_service_v2 import PDFTextExtractor
import re


async def analyze_document_structure():
    """Analyze the structure of AAK document to find financial data"""
    
    print("🔍 ANALYZING AAK DOCUMENT STRUCTURE")
    print("=" * 70)
    
    pdf_path = "/Users/jdandemar/Documents/YodaBuffett/backend/data/companies/SE/A/AAK/2025/quarterly_report/2025-07-17-aaks-interim-report-for-the-second-quarter-2025.pdf"
    
    extractor = PDFTextExtractor()
    pdf_data = await extractor.extract_text(pdf_path)
    
    text = pdf_data["full_text"]
    
    # Look for financial tables by finding lines with multiple numbers
    print("\n📊 SEARCHING FOR FINANCIAL TABLES:")
    print("-" * 50)
    
    lines = text.split('\n')
    table_sections = []
    
    for i, line in enumerate(lines):
        # Look for lines with multiple numbers that might be table rows
        numbers = re.findall(r'[\d,]+(?:\.\d+)?', line)
        if len(numbers) >= 3:  # Line with at least 3 numbers
            # Get context
            start = max(0, i-3)
            end = min(len(lines), i+3)
            context = '\n'.join(lines[start:end])
            table_sections.append((i, line, context))
    
    # Show first 10 potential table rows
    for idx, (line_num, line, context) in enumerate(table_sections[:10]):
        print(f"\n📍 Line {line_num}:")
        print(f"Context:\n{context}")
        print("-" * 30)
    
    # Search for specific key metrics with context
    print("\n🔍 SEARCHING FOR KEY METRICS:")
    print("-" * 50)
    
    key_patterns = {
        "Revenue/Sales": [
            r'(?:Net sales|Revenue|Omsättning|Nettoomsättning)[:\s]+([^\n]+)',
            r'Sales reached SEK\s*([\d,]+)\s*million',
        ],
        "Operating Profit": [
            r'(?:Operating profit|Rörelseresultat)[:\s]+([^\n]+)',
            r'Operating profit.*?SEK\s*([\d,]+)\s*million',
        ],
        "EBITDA": [
            r'(?:EBITDA|Adjusted EBITDA)[:\s]+([^\n]+)',
            r'EBITDA.*?SEK\s*([\d,]+)\s*million',
        ],
        "Net Income": [
            r'(?:Net income|Profit for the period|Periodens resultat)[:\s]+([^\n]+)',
            r'Profit for the period.*?SEK\s*([\d,]+)\s*million',
        ],
        "EPS": [
            r'(?:Earnings per share|Vinst per aktie)[:\s]+([^\n]+)',
            r'Earnings per share.*?SEK\s*([\d.,]+)',
        ],
        "Cash Flow": [
            r'(?:Operating cash flow|Cash flow from operating)[:\s]+([^\n]+)',
            r'cash flow.*?SEK\s*([\d,]+)\s*million',
        ]
    }
    
    for metric_name, patterns in key_patterns.items():
        print(f"\n🎯 {metric_name}:")
        found = False
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE | re.MULTILINE)
            if matches:
                found = True
                for match in matches[:3]:  # Show first 3 matches
                    print(f"  ✓ Found: {match}")
                    
                    # Try to find the context
                    match_idx = text.find(str(match))
                    if match_idx != -1:
                        context_start = max(0, match_idx - 100)
                        context_end = min(len(text), match_idx + 100)
                        context = text[context_start:context_end].replace('\n', ' ')
                        print(f"    Context: ...{context}...")
                break
        
        if not found:
            print(f"  ✗ Not found with current patterns")
    
    # Look for specific number patterns that we know should exist
    print("\n🔢 SEARCHING FOR SPECIFIC NUMBERS:")
    print("-" * 50)
    
    known_numbers = {
        "11,300": "Revenue",
        "11 300": "Revenue (alt format)",
        "912": "Operating Profit",
        "1,162": "Adjusted Operating Profit",
        "643": "Net Income",
        "851": "Adjusted Net Income",
        "2.47": "EPS Reported",
        "2,47": "EPS Reported (Swedish)",
        "3.26": "EPS Adjusted",
        "3,26": "EPS Adjusted (Swedish)",
        "524": "Cash Flow",
    }
    
    for number, description in known_numbers.items():
        indices = [m.start() for m in re.finditer(re.escape(number), text)]
        if indices:
            print(f"\n✓ {number} ({description}) found {len(indices)} times:")
            for idx in indices[:2]:  # Show first 2 occurrences
                context_start = max(0, idx - 50)
                context_end = min(len(text), idx + 50)
                context = text[context_start:context_end].replace('\n', ' ')
                print(f"  ...{context}...")
        else:
            print(f"\n✗ {number} ({description}) not found")
    
    # Extract a focused section for LLM
    print("\n📄 EXTRACTING FOCUSED SECTION FOR LLM:")
    print("-" * 50)
    
    # Find the financial highlights section
    highlights_idx = text.lower().find("financial highlights")
    if highlights_idx != -1:
        section = text[highlights_idx:highlights_idx+3000]
        print("Financial Highlights Section:")
        print(section[:1000] + "...")
    else:
        print("Financial highlights section not found")
    
    return text


if __name__ == "__main__":
    asyncio.run(analyze_document_structure())