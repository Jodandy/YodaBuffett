#!/usr/bin/env python3
"""
Custom batch extraction script - easily customizable for your specific needs
"""
import asyncio
from pathlib import Path
from test_success_rate_improvements import OptimizedFinancialExtractionService


async def extract_custom_batch():
    """Extract from your custom list of companies/documents"""
    
    # CUSTOMIZE THIS LIST with your specific documents
    custom_extractions = [
        {
            'company_name': 'AAK',
            'pdf_path': '/Users/jdandemar/Documents/YodaBuffett/backend/data/companies/SE/A/AAK/2025/quarterly_report/2025-07-17-aaks-interim-report-for-the-second-quarter-2025.pdf',
            'document_id': '987f81e1-dc8c-4aab-9a06-2b254599cd60'  # Must exist in nordic_documents
        },
        # Add more companies here:
        # {
        #     'company_name': 'Hexagon AB',
        #     'pdf_path': '/path/to/hexagon/pdf',
        #     'document_id': 'hexagon-doc-id'
        # },
        # {
        #     'company_name': 'Atlas Copco AB', 
        #     'pdf_path': '/path/to/atlas/pdf',
        #     'document_id': 'atlas-doc-id'
        # },
    ]
    
    print("🎯 CUSTOM BATCH EXTRACTION")
    print("=" * 60)
    print(f"Processing {len(custom_extractions)} companies with 97% success rate service")
    
    service = OptimizedFinancialExtractionService()
    results = []
    
    for i, extraction in enumerate(custom_extractions, 1):
        print(f"\n🏢 {i}/{len(custom_extractions)}: {extraction['company_name']}")
        print("-" * 40)
        
        # Check if file exists
        if not Path(extraction['pdf_path']).exists():
            print(f"❌ File not found: {extraction['pdf_path']}")
            continue
        
        try:
            # Extract
            metrics = await service.extract_from_pdf(
                extraction['pdf_path'], 
                extraction['company_name']
            )
            
            if metrics:
                print(f"✅ Extracted: {metrics.extraction_confidence:.1%} confidence")
                
                # Show key results
                if metrics.revenue_reported:
                    print(f"   💰 Revenue: {metrics.revenue_reported/1000000:.0f}M SEK")
                if metrics.operating_profit_reported:
                    print(f"   📈 Operating Profit: {metrics.operating_profit_reported/1000000:.0f}M SEK")
                if metrics.total_assets:
                    print(f"   🏦 Total Assets: {metrics.total_assets/1000000000:.1f}B SEK")
                
                # Save to database
                save_success = await service.save_metrics(metrics, extraction['document_id'])
                print(f"   💾 Database: {'✅ Saved' if save_success else '❌ Failed'}")
                
                results.append({
                    'company': extraction['company_name'],
                    'success': True,
                    'confidence': metrics.extraction_confidence,
                    'saved': save_success
                })
            else:
                print("❌ Extraction failed")
                results.append({
                    'company': extraction['company_name'],
                    'success': False
                })
                
        except Exception as e:
            print(f"❌ Error: {e}")
            results.append({
                'company': extraction['company_name'],
                'success': False,
                'error': str(e)
            })
    
    # Summary
    print(f"\n📊 BATCH EXTRACTION SUMMARY")
    print("=" * 60)
    
    successful = [r for r in results if r.get('success')]
    print(f"✅ Successful: {len(successful)}/{len(results)}")
    
    if successful:
        avg_confidence = sum(r['confidence'] for r in successful) / len(successful)
        saved_count = sum(1 for r in successful if r.get('saved'))
        print(f"📈 Average confidence: {avg_confidence:.1%}")
        print(f"💾 Saved to database: {saved_count}/{len(successful)}")
    
    return results


if __name__ == "__main__":
    asyncio.run(extract_custom_batch())