#!/usr/bin/env python3
"""
AI Documentation Validator for YodaBuffett

Script for AI assistants to validate documentation currency and consistency.
Run this after making code changes to identify outdated documentation.

Usage:
    python tools/ai_docs_validator.py
    python tools/ai_docs_validator.py --domain analytics
    python tools/ai_docs_validator.py --check-performance
"""

import os
import re
import json
import argparse
from pathlib import Path
from typing import List, Dict, Set, Optional
from datetime import datetime

class DocumentationValidator:
    def __init__(self, project_root: str = None):
        self.project_root = Path(project_root) if project_root else Path(__file__).parent.parent
        self.domains_path = self.project_root / "backend" / "domains"
        self.docs_path = self.project_root / "docs"
        
    def validate_all(self) -> Dict[str, List[str]]:
        """Run comprehensive validation across all domains and documentation."""
        issues = {
            "missing_files": [],
            "obsolete_docs": [],
            "inconsistent_performance": [],
            "outdated_cross_references": [],
            "missing_api_endpoints": []
        }
        
        # Validate each domain
        for domain_path in self.domains_path.iterdir():
            if domain_path.is_dir() and not domain_path.name.startswith('.'):
                domain_issues = self.validate_domain(domain_path.name)
                for category, domain_issues_list in domain_issues.items():
                    issues[category].extend(domain_issues_list)
        
        # Validate global architecture consistency
        global_issues = self.validate_global_consistency()
        for category, global_issues_list in global_issues.items():
            issues[category].extend(global_issues_list)
            
        return issues
    
    def validate_domain(self, domain_name: str) -> Dict[str, List[str]]:
        """Validate a specific domain's documentation against actual code."""
        domain_path = self.domains_path / domain_name
        domain_doc_path = domain_path / "__domain__.md"
        
        issues = {
            "missing_files": [],
            "obsolete_docs": [],
            "inconsistent_performance": [],
            "missing_api_endpoints": []
        }
        
        if not domain_doc_path.exists():
            issues["missing_files"].append(f"Missing {domain_name}/__domain__.md")
            return issues
            
        # Check if documented files still exist
        documented_files = self.extract_documented_files(domain_doc_path)
        actual_files = self.list_python_files(domain_path)
        
        # Find missing documentation
        missing_files = actual_files - documented_files
        if missing_files:
            issues["missing_files"].extend([
                f"{domain_name}: Missing documentation for {file}" 
                for file in missing_files
            ])
        
        # Find obsolete documentation
        obsolete_docs = documented_files - actual_files
        if obsolete_docs:
            issues["obsolete_docs"].extend([
                f"{domain_name}: Obsolete documentation for {file}" 
                for file in obsolete_docs
            ])
        
        # Check API endpoints consistency
        documented_endpoints = self.extract_api_endpoints(domain_doc_path)
        actual_endpoints = self.find_actual_endpoints(domain_path)
        missing_endpoints = actual_endpoints - documented_endpoints
        if missing_endpoints:
            issues["missing_api_endpoints"].extend([
                f"{domain_name}: Missing documentation for endpoint {endpoint}" 
                for endpoint in missing_endpoints
            ])
        
        return issues
    
    def validate_global_consistency(self) -> Dict[str, List[str]]:
        """Validate consistency between global architecture docs and domain implementations."""
        issues = {
            "inconsistent_performance": [],
            "outdated_cross_references": []
        }
        
        # Check ARCHITECTURE_MAP.md consistency
        arch_map_path = self.project_root / "ARCHITECTURE_MAP.md"
        if arch_map_path.exists():
            arch_content = arch_map_path.read_text()
            
            # Check if all domains are documented in architecture map
            actual_domains = {d.name for d in self.domains_path.iterdir() 
                            if d.is_dir() and not d.name.startswith('.')}
            documented_domains = self.extract_domains_from_architecture_map(arch_content)
            
            missing_arch_docs = actual_domains - documented_domains
            if missing_arch_docs:
                issues["outdated_cross_references"].extend([
                    f"ARCHITECTURE_MAP.md: Missing domain {domain}" 
                    for domain in missing_arch_docs
                ])
        
        return issues
    
    def extract_documented_files(self, domain_doc_path: Path) -> Set[str]:
        """Extract list of files mentioned in domain documentation."""
        if not domain_doc_path.exists():
            return set()
            
        content = domain_doc_path.read_text()
        # Look for file references in various formats
        patterns = [
            r'`([^`]+\.py)`',  # `filename.py`
            r'services/([^`\s]+\.py)',  # services/filename.py
            r'models/([^`\s]+\.py)',    # models/filename.py
            r'repositories/([^`\s]+\.py)',  # repositories/filename.py
            r'api/([^`\s]+\.py)'        # api/filename.py
        ]
        
        documented_files = set()
        for pattern in patterns:
            matches = re.findall(pattern, content)
            documented_files.update(matches)
            
        return documented_files
    
    def list_python_files(self, domain_path: Path) -> Set[str]:
        """List all Python files in a domain directory."""
        if not domain_path.exists():
            return set()
            
        python_files = set()
        for py_file in domain_path.rglob("*.py"):
            if py_file.name != "__init__.py":
                # Get relative path from domain root
                rel_path = py_file.relative_to(domain_path)
                python_files.add(str(rel_path))
                
        return python_files
    
    def extract_api_endpoints(self, domain_doc_path: Path) -> Set[str]:
        """Extract API endpoints mentioned in domain documentation."""
        if not domain_doc_path.exists():
            return set()
            
        content = domain_doc_path.read_text()
        # Look for API endpoint patterns
        endpoint_pattern = r'`(GET|POST|PUT|DELETE|PATCH)\s+([^`]+)`'
        matches = re.findall(endpoint_pattern, content)
        
        endpoints = set()
        for method, path in matches:
            endpoints.add(f"{method} {path}")
            
        return endpoints
    
    def find_actual_endpoints(self, domain_path: Path) -> Set[str]:
        """Find actual API endpoints defined in domain code."""
        # This is a simplified implementation - in practice, you'd want
        # more sophisticated parsing of Flask/FastAPI route definitions
        api_path = domain_path / "api"
        if not api_path.exists():
            return set()
            
        endpoints = set()
        for py_file in api_path.rglob("*.py"):
            content = py_file.read_text()
            # Look for Flask/FastAPI route decorators
            patterns = [
                r'@app\.route\([\'"]([^\'"]+)[\'"].*methods=\[[\'"]([^\'"]+)[\'"]',
                r'@router\.(get|post|put|delete|patch)\([\'"]([^\'"]+)[\'"]'
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, content)
                for match in matches:
                    if len(match) == 2:
                        path, method = match
                        endpoints.add(f"{method.upper()} {path}")
                    else:
                        method, path = match
                        endpoints.add(f"{method.upper()} {path}")
                        
        return endpoints
    
    def extract_domains_from_architecture_map(self, content: str) -> Set[str]:
        """Extract domain names mentioned in ARCHITECTURE_MAP.md."""
        # Look for domain references in various formats
        patterns = [
            r'domains/([^/\s]+)/',  # domains/domain_name/
            r'Domain:\s+([^\n]+)',   # Domain: domain_name
            r'├─\s+([^─\s]+)',       # Directory tree format
        ]
        
        domains = set()
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            domains.update([match.lower().replace(' ', '_') for match in matches])
            
        return domains
    
    def check_performance_claims(self, domain_name: str) -> List[str]:
        """Check if performance characteristics in docs match actual benchmarks."""
        # This would require actual performance testing infrastructure
        # For now, just check if performance sections exist and are recent
        domain_doc_path = self.domains_path / domain_name / "__domain__.md"
        if not domain_doc_path.exists():
            return [f"{domain_name}: No domain documentation found"]
            
        content = domain_doc_path.read_text()
        issues = []
        
        # Check if performance section exists
        if "Performance Characteristics" not in content:
            issues.append(f"{domain_name}: Missing performance characteristics section")
        
        # Check if performance data seems recent (has recent dates)
        current_year = datetime.now().year
        if str(current_year) not in content:
            issues.append(f"{domain_name}: Performance data may be outdated (no {current_year} dates)")
            
        return issues
    
    def generate_update_suggestions(self, issues: Dict[str, List[str]]) -> List[str]:
        """Generate specific update suggestions for AI assistants."""
        suggestions = []
        
        if issues["missing_files"]:
            suggestions.append("## Missing File Documentation")
            suggestions.extend([f"- Add documentation for: {issue}" for issue in issues["missing_files"]])
            suggestions.append("")
        
        if issues["obsolete_docs"]:
            suggestions.append("## Obsolete Documentation")
            suggestions.extend([f"- Remove documentation for: {issue}" for issue in issues["obsolete_docs"]])
            suggestions.append("")
        
        if issues["missing_api_endpoints"]:
            suggestions.append("## Missing API Documentation")
            suggestions.extend([f"- Document endpoint: {issue}" for issue in issues["missing_api_endpoints"]])
            suggestions.append("")
        
        if issues["outdated_cross_references"]:
            suggestions.append("## Outdated Cross-References")
            suggestions.extend([f"- Update reference: {issue}" for issue in issues["outdated_cross_references"]])
            suggestions.append("")
        
        return suggestions

def main():
    parser = argparse.ArgumentParser(description="Validate YodaBuffett documentation")
    parser.add_argument("--domain", help="Validate specific domain only")
    parser.add_argument("--check-performance", action="store_true", 
                       help="Check performance characteristics")
    parser.add_argument("--json", action="store_true", 
                       help="Output results in JSON format")
    
    args = parser.parse_args()
    
    validator = DocumentationValidator()
    
    if args.domain:
        issues = validator.validate_domain(args.domain)
        if args.check_performance:
            perf_issues = validator.check_performance_claims(args.domain)
            issues["performance"] = perf_issues
    else:
        issues = validator.validate_all()
        if args.check_performance:
            perf_issues = []
            for domain_path in validator.domains_path.iterdir():
                if domain_path.is_dir() and not domain_path.name.startswith('.'):
                    perf_issues.extend(validator.check_performance_claims(domain_path.name))
            issues["performance"] = perf_issues
    
    if args.json:
        print(json.dumps(issues, indent=2))
    else:
        # Human-readable output
        total_issues = sum(len(issue_list) for issue_list in issues.values())
        
        if total_issues == 0:
            print("✅ All documentation appears to be current and consistent!")
        else:
            print(f"⚠️ Found {total_issues} documentation issues:")
            print()
            
            for category, issue_list in issues.items():
                if issue_list:
                    print(f"## {category.replace('_', ' ').title()}:")
                    for issue in issue_list:
                        print(f"  - {issue}")
                    print()
            
            # Generate update suggestions
            suggestions = validator.generate_update_suggestions(issues)
            if suggestions:
                print("## AI Assistant Update Suggestions:")
                for suggestion in suggestions:
                    print(suggestion)

if __name__ == "__main__":
    main()