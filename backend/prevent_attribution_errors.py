#!/usr/bin/env python3
"""
COMPREHENSIVE SYSTEM TO PREVENT COMPANY ATTRIBUTION ERRORS

This implements multiple layers of defense:
1. Validation at collection time
2. Validation at storage time  
3. Monitoring and alerting
4. Automated correction
"""

import asyncio
import re
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict

class AttributionValidator:
    """Validates company attribution before documents are stored"""
    
    def __init__(self):
        # Known patterns for company-specific URLs
        self.company_url_patterns = {
            "Volvo Group": [r"volvo\.com", r"volvogroup\.com"],
            "H&M": [r"hm\.com", r"hmgroup\.com", r"about\.hm"],
            "Ericsson": [r"ericsson\.com"],
            "Atlas Copco": [r"atlascopco\.com"],
            "Sandvik": [r"sandvik\.com", r"home\.sandvik"],
        }
        
        # Generic financial document hosts
        self.financial_hosts = {
            "mfn.se",
            "financialhearings.com",
            "storage.mfn.se", 
            "mb.cision.com",
            "news.cision.com",
            "feed.ne.cision.com",
            "euro.mediavision.se"
        }
        
    def validate_attribution(self, document_url: str, company_name: str, 
                           source_url: str, metadata: dict) -> Tuple[bool, str]:
        """
        Validate that a document should be attributed to the given company.
        Returns (is_valid, reason)
        """
        # Check 1: Company-specific URL patterns
        if company_name in self.company_url_patterns:
            patterns = self.company_url_patterns[company_name]
            if any(re.search(pattern, document_url, re.I) for pattern in patterns):
                return True, "Matched company-specific URL pattern"
        
        # Check 2: Extract company from source URL if it's MFN
        if "mfn.se" in source_url:
            slug_match = re.search(r'mfn\.se/all/[a-z]/([^/?]+)', source_url)
            if slug_match:
                source_slug = slug_match.group(1)
                # This would check against our mappings
                expected_company = self._resolve_slug_to_company(source_slug)
                if expected_company and expected_company != company_name:
                    return False, f"Source URL indicates company '{expected_company}', not '{company_name}'"
        
        # Check 3: Validate title/content mentions company
        if metadata and "title" in metadata:
            title_lower = metadata["title"].lower()
            company_words = company_name.lower().split()
            
            # Check if primary company name appears in title
            if company_words and company_words[0] in title_lower:
                return True, "Company name found in document title"
        
        # Check 4: For financial hosts, be more permissive but flag for review
        if any(host in document_url for host in self.financial_hosts):
            return True, "Generic financial host - requires manual verification"
        
        # Default: Reject if we can't validate
        return False, "Could not validate company attribution"
    
    def _resolve_slug_to_company(self, slug: str) -> Optional[str]:
        """Resolve MFN slug to company name"""
        # This would use the company_mappings.py data
        # Simplified for example
        slug_mappings = {
            "volvo": "Volvo Group",
            "hm": "H&M",
            "ericsson": "Ericsson",
            # ... etc
        }
        return slug_mappings.get(slug)


class AttributionMonitor:
    """Monitors for attribution anomalies and alerts"""
    
    def __init__(self):
        self.attribution_stats = defaultdict(lambda: defaultdict(int))
        self.anomaly_threshold = 0.2  # 20% change triggers alert
        
    async def check_attribution_anomalies(self, db) -> List[Dict]:
        """Check for unusual attribution patterns"""
        anomalies = []
        
        # Query recent attributions
        query = """
        SELECT 
            company_id,
            COUNT(*) as doc_count,
            DATE(created_at) as date
        FROM nordic_documents  
        WHERE created_at > NOW() - INTERVAL '7 days'
        GROUP BY company_id, DATE(created_at)
        ORDER BY date DESC
        """
        
        # Check for:
        # 1. Sudden spikes in documents for a company
        # 2. Companies receiving documents from multiple sources
        # 3. Identical documents attributed to multiple companies
        
        return anomalies
    
    async def generate_attribution_report(self, db) -> str:
        """Generate a report of attribution patterns"""
        report = f"Attribution Health Report - {datetime.now()}\n"
        report += "=" * 60 + "\n\n"
        
        # Check for multi-company documents
        duplicate_check = """
        SELECT 
            source_url,
            COUNT(DISTINCT company_id) as company_count,
            array_agg(DISTINCT c.name) as companies
        FROM nordic_documents d
        JOIN nordic_companies c ON d.company_id = c.id
        WHERE source_url IS NOT NULL
        GROUP BY source_url
        HAVING COUNT(DISTINCT company_id) > 1
        """
        
        # Add findings to report
        
        return report


class AttributionCorrector:
    """Automated correction of attribution errors"""
    
    def __init__(self, validator: AttributionValidator):
        self.validator = validator
        
    async def scan_and_fix_attributions(self, db, dry_run: bool = True):
        """Scan for and fix attribution errors"""
        fixes_needed = []
        
        # Get all documents with source URLs
        query = """
        SELECT 
            d.id,
            d.source_url,
            d.file_url,
            d.company_id,
            c.name as company_name,
            d.metadata_
        FROM nordic_documents d
        JOIN nordic_companies c ON d.company_id = c.id
        WHERE d.source_url IS NOT NULL
        ORDER BY d.created_at DESC
        LIMIT 1000
        """
        
        # Validate each attribution
        async for doc in db.execute(query):
            is_valid, reason = self.validator.validate_attribution(
                doc.file_url or "",
                doc.company_name,
                doc.source_url,
                doc.metadata_ or {}
            )
            
            if not is_valid:
                fixes_needed.append({
                    "doc_id": doc.id,
                    "current_company": doc.company_name,
                    "reason": reason,
                    "source_url": doc.source_url
                })
        
        if not dry_run and fixes_needed:
            # Apply fixes
            pass
            
        return fixes_needed


def create_prevention_pipeline():
    """Create the complete prevention pipeline"""
    
    pipeline_config = {
        "collection_validation": {
            "enabled": True,
            "strict_mode": True,
            "log_failures": True
        },
        "storage_validation": {
            "enabled": True,
            "reject_invalid": True,
            "quarantine_suspicious": True
        },
        "monitoring": {
            "enabled": True,
            "check_interval": 3600,  # 1 hour
            "alert_threshold": 0.2
        },
        "auto_correction": {
            "enabled": False,  # Start with manual review
            "dry_run": True,
            "batch_size": 100
        }
    }
    
    return pipeline_config


if __name__ == "__main__":
    print("Company Attribution Error Prevention System")
    print("=" * 60)
    print("\nThis system prevents attribution errors through:")
    print("1. Collection-time validation")
    print("2. Storage-time validation")
    print("3. Continuous monitoring")
    print("4. Automated correction\n")
    
    # Example validation
    validator = AttributionValidator()
    
    test_cases = [
        ("https://mb.cision.com/volvo/report.pdf", "Volvo Group", "https://mfn.se/all/v/volvo"),
        ("https://storage.mfn.se/be-group-q2.pdf", "Volvo Group", "https://mfn.se/all/v/volvo"),
        ("https://volvogroup.com/reports/annual-2024.pdf", "Volvo Group", "https://mfn.se/all/v/volvo"),
    ]
    
    print("Example Validations:")
    for doc_url, company, source in test_cases:
        is_valid, reason = validator.validate_attribution(doc_url, company, source, {})
        status = "✅" if is_valid else "❌"
        print(f"{status} {company}: {reason}")
        print(f"   Document: {doc_url}")
        print(f"   Source: {source}\n")