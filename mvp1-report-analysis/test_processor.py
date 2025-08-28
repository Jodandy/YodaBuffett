#!/usr/bin/env python3
"""
Quick test script for the document processor.
Run this to test PDF/HTML parsing before building the full web interface.
"""

import sys
import os
from pathlib import Path

# Add src to path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from document_processor import DocumentProcessor

def test_processor():
    """Test the document processor with any files in the data directory."""
    
    processor = DocumentProcessor()
    print(f"Document Processor initialized")
    print(f"Supported formats: {processor.supported_formats}")
    
    # Look for test files in data directory
    data_dir = Path(__file__).parent / 'data'
    data_dir.mkdir(exist_ok=True)
    
    test_files = []
    for extension in ['.pdf', '.html', '.htm']:
        test_files.extend(data_dir.glob(f'*{extension}'))
    
    if not test_files:
        print(f"\nNo test files found in {data_dir}")
        print("Please add a PDF or HTML file to the data/ directory and run again.")
        print("\nExample files you could try:")
        print("- Download a 10-K from SEC EDGAR in PDF format")
        print("- Save a company's annual report as HTML")
        return
    
    print(f"\nFound {len(test_files)} test file(s):")
    for file in test_files:
        print(f"  - {file.name}")
    
    # Test each file
    for test_file in test_files:
        print(f"\n{'='*80}")
        print(f"TESTING: {test_file.name}")
        print(f"{'='*80}")
        
        try:
            # Process the document
            doc = processor.process_file(test_file)
            
            # Print summary
            processor.print_document_summary(doc)
            
            # Test section extraction
            if doc.sections:
                print(f"\nSECTION DETAILS:")
                for section in doc.sections:
                    print(f"\n{section.name}:")
                    print(f"  Length: {len(section.content)} chars")
                    print(f"  Confidence: {section.confidence}")
                    
                    # Show a sample of the content
                    sample = section.content[:300].replace('\n', ' ').strip()
                    print(f"  Sample: {sample}...")
            else:
                print("\nWARNING: No sections identified in this document")
                print("This might be normal for non-SEC filings or unusual formatting")
        
        except Exception as e:
            print(f"ERROR processing {test_file.name}: {e}")
            import traceback
            traceback.print_exc()

def create_sample_html():
    """Create a sample HTML file for testing if none exists."""
    
    data_dir = Path(__file__).parent / 'data'
    sample_file = data_dir / 'sample_filing.html'
    
    if sample_file.exists():
        return
    
    sample_html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Sample Company 10-K Filing</title>
    </head>
    <body>
        <h1>SAMPLE COMPANY INC.</h1>
        <h2>FORM 10-K</h2>
        <p>Filed on: December 15, 2023</p>
        
        <h2>Item 1. Business</h2>
        <p>Sample Company Inc. operates in the technology sector, providing innovative software solutions 
        to enterprise customers. Our business model focuses on subscription-based services and has shown 
        strong growth over the past five years. We serve clients across multiple industries including 
        healthcare, finance, and manufacturing.</p>
        
        <h2>Item 1A. Risk Factors</h2>
        <p>The following risk factors may materially affect our business operations:</p>
        <ul>
            <li>Competition from larger technology companies</li>
            <li>Dependence on key personnel</li>
            <li>Cybersecurity threats</li>
            <li>Economic downturn affecting customer spending</li>
        </ul>
        
        <h2>Item 2. Management's Discussion and Analysis</h2>
        <p>Management believes that the company is well-positioned for continued growth. 
        Revenue increased by 25% year-over-year, driven by strong adoption of our core platform. 
        We expect this growth trend to continue as we expand into new markets and enhance our 
        product offerings.</p>
        
        <h2>Item 8. Financial Statements</h2>
        <p>See consolidated financial statements attached as exhibits to this filing.</p>
    </body>
    </html>
    """
    
    with open(sample_file, 'w') as f:
        f.write(sample_html)
    
    print(f"Created sample HTML file: {sample_file}")

if __name__ == "__main__":
    print("YodaBuffett MVP 1 - Document Processor Test")
    print("=" * 50)
    
    # Create sample file if no test files exist
    data_dir = Path(__file__).parent / 'data'
    if not any(data_dir.glob('*.pdf')) and not any(data_dir.glob('*.html')):
        print("No test files found. Creating sample HTML file...")
        create_sample_html()
    
    # Run the test
    test_processor()
    
    print(f"\nTest complete!")
    print(f"Next steps:")
    print(f"1. Try adding a real SEC 10-K PDF to the data/ directory")
    print(f"2. Run this script again to see how it performs")
    print(f"3. Once satisfied, we'll build the web interface")