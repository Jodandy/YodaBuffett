#!/usr/bin/env python3

"""
Per-Company Multi-Tier Pipeline
Each company has its own data collection strategy
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Callable
from datetime import datetime
from enum import Enum
import json
from pathlib import Path


class DataCollectionMethod(Enum):
    RSS_FEED = "rss_feed"
    EMAIL_SUBSCRIPTION = "email_subscription"
    IR_PAGE_SCRAPING = "ir_page_scraping"
    PLAYWRIGHT_BROWSER = "playwright_browser"
    MANUAL_COLLECTION = "manual_collection"
    NOT_CONFIGURED = "not_configured"


class CollectionStatus(Enum):
    WORKING = "working"
    BROKEN = "broken"
    NOT_TESTED = "not_tested"
    RATE_LIMITED = "rate_limited"
    BLOCKED = "blocked"


@dataclass
class CollectionTier:
    """Single tier in a company's collection strategy"""
    method: DataCollectionMethod
    priority: int  # 1 = primary, 2 = secondary, etc.
    config: Dict  # Method-specific configuration
    status: CollectionStatus = CollectionStatus.NOT_TESTED
    last_success: Optional[datetime] = None
    last_failure: Optional[datetime] = None
    failure_count: int = 0
    notes: str = ""


@dataclass 
class CompanyPipeline:
    """Complete data collection pipeline for one company"""
    
    # Company identification
    company_name: str
    ticker: str
    market_cap: str  # 'Large', 'Mid', 'Small'
    
    # Collection tiers (ordered by priority)
    tiers: List[CollectionTier] = field(default_factory=list)
    
    # Issue tracking
    open_issues: List[Dict] = field(default_factory=list)
    
    # Metadata
    created_date: datetime = field(default_factory=datetime.now)
    last_updated: datetime = field(default_factory=datetime.now)
    
    def add_tier(self, method: DataCollectionMethod, config: Dict, priority: int = None):
        """Add a collection tier"""
        if priority is None:
            priority = len(self.tiers) + 1
            
        tier = CollectionTier(
            method=method,
            priority=priority,
            config=config
        )
        self.tiers.append(tier)
        self.tiers.sort(key=lambda x: x.priority)
        
    def get_active_tier(self) -> Optional[CollectionTier]:
        """Get the highest priority working tier"""
        for tier in self.tiers:
            if tier.status == CollectionStatus.WORKING:
                return tier
        return None
    
    def report_failure(self, tier: CollectionTier, error: str):
        """Report a tier failure and potentially create issue"""
        tier.failure_count += 1
        tier.last_failure = datetime.now()
        
        # Create issue if tier was previously working
        if tier.status == CollectionStatus.WORKING:
            self.create_issue(
                title=f"{self.company_name}: {tier.method.value} stopped working",
                description=f"Error: {error}\nFailure count: {tier.failure_count}",
                priority="high" if tier.priority == 1 else "medium"
            )
            
        # Update status based on failure count
        if tier.failure_count >= 3:
            tier.status = CollectionStatus.BROKEN
        elif tier.failure_count >= 1:
            tier.status = CollectionStatus.RATE_LIMITED
            
    def create_issue(self, title: str, description: str, priority: str = "medium"):
        """Create an issue for tracking"""
        issue = {
            "id": f"{self.ticker}_{datetime.now().timestamp()}",
            "title": title,
            "description": description,
            "priority": priority,
            "created": datetime.now().isoformat(),
            "status": "open",
            "company": self.company_name,
            "ticker": self.ticker
        }
        self.open_issues.append(issue)
        
    def get_next_tier(self) -> Optional[CollectionTier]:
        """Get next tier to try if current fails"""
        for tier in self.tiers:
            if tier.status != CollectionStatus.BROKEN:
                return tier
        return None


class PipelineManager:
    """Manages all company pipelines"""
    
    def __init__(self, storage_dir: str = "data/pipelines"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.pipelines: Dict[str, CompanyPipeline] = {}
        
    def create_pipeline(self, company_name: str, ticker: str, market_cap: str) -> CompanyPipeline:
        """Create a new company pipeline"""
        pipeline = CompanyPipeline(
            company_name=company_name,
            ticker=ticker,
            market_cap=market_cap
        )
        self.pipelines[ticker] = pipeline
        return pipeline
    
    def configure_swedish_majors(self):
        """Configure pipelines for major Swedish companies"""
        
        # Volvo Group
        volvo = self.create_pipeline("Volvo Group", "VOLV-B", "Large")
        volvo.add_tier(
            DataCollectionMethod.RSS_FEED,
            {"url": "https://www.volvogroup.com/se/news-and-media/events/_jcr_content/root/responsivegrid/eventlist.feed.xml"},
            priority=1
        )
        volvo.add_tier(
            DataCollectionMethod.IR_PAGE_SCRAPING,
            {"url": "https://www.volvogroup.com/investors/reports/", "selector": ".report-link"},
            priority=2
        )
        volvo.add_tier(
            DataCollectionMethod.PLAYWRIGHT_BROWSER,
            {"url": "https://www.volvogroup.com/investors/reports/"},
            priority=3
        )
        
        # H&M
        hm = self.create_pipeline("H&M", "HM-B", "Large")
        hm.add_tier(
            DataCollectionMethod.EMAIL_SUBSCRIPTION,
            {"email": "investor.relations@hm.com", "signup_url": "https://hmgroup.com/investors/"},
            priority=1
        )
        hm.add_tier(
            DataCollectionMethod.IR_PAGE_SCRAPING,
            {"url": "https://hmgroup.com/investors/reports/"},
            priority=2
        )
        
        # Ericsson
        ericsson = self.create_pipeline("Ericsson", "ERIC-B", "Large")
        ericsson.add_tier(
            DataCollectionMethod.IR_PAGE_SCRAPING,
            {"url": "https://www.ericsson.com/en/investors/financial-reports"},
            priority=1
        )
        ericsson.add_tier(
            DataCollectionMethod.PLAYWRIGHT_BROWSER,
            {"url": "https://www.ericsson.com/en/investors/financial-reports"},
            priority=2
        )
        
        # More companies...
        
    def get_all_issues(self) -> List[Dict]:
        """Get all open issues across all pipelines"""
        all_issues = []
        for ticker, pipeline in self.pipelines.items():
            all_issues.extend(pipeline.open_issues)
        return sorted(all_issues, key=lambda x: x['created'], reverse=True)
    
    def generate_status_report(self) -> str:
        """Generate status report of all pipelines"""
        
        working_count = 0
        broken_count = 0
        manual_count = 0
        
        for pipeline in self.pipelines.values():
            active_tier = pipeline.get_active_tier()
            if active_tier:
                if active_tier.method == DataCollectionMethod.MANUAL_COLLECTION:
                    manual_count += 1
                else:
                    working_count += 1
            else:
                broken_count += 1
                
        report = f"""
📊 PIPELINE STATUS REPORT
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}
{'='*50}

📈 Overview:
  Total companies: {len(self.pipelines)}
  Automated working: {working_count}
  Manual only: {manual_count}
  Broken: {broken_count}
  
🚨 Open Issues: {sum(len(p.open_issues) for p in self.pipelines.values())}

📋 Per-Company Status:
"""
        
        for ticker, pipeline in sorted(self.pipelines.items()):
            active_tier = pipeline.get_active_tier()
            if active_tier:
                status_icon = "✅" if active_tier.method != DataCollectionMethod.MANUAL_COLLECTION else "👤"
                method = active_tier.method.value
            else:
                status_icon = "❌"
                method = "No working method"
                
            issues_count = len(pipeline.open_issues)
            issues_text = f" ({issues_count} issues)" if issues_count > 0 else ""
            
            report += f"\n{status_icon} {pipeline.company_name} ({ticker}): {method}{issues_text}"
            
        return report
    
    def save_state(self):
        """Save all pipeline states"""
        state_file = self.storage_dir / "pipeline_state.json"
        
        state = {
            "last_updated": datetime.now().isoformat(),
            "pipelines": {}
        }
        
        for ticker, pipeline in self.pipelines.items():
            state["pipelines"][ticker] = {
                "company_name": pipeline.company_name,
                "market_cap": pipeline.market_cap,
                "tiers": [
                    {
                        "method": tier.method.value,
                        "priority": tier.priority,
                        "status": tier.status.value,
                        "config": tier.config,
                        "failure_count": tier.failure_count,
                        "last_success": tier.last_success.isoformat() if tier.last_success else None,
                        "last_failure": tier.last_failure.isoformat() if tier.last_failure else None
                    }
                    for tier in pipeline.tiers
                ],
                "open_issues": pipeline.open_issues
            }
            
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
            
        print(f"💾 Pipeline state saved to {state_file}")


class EmailSubscriptionManager:
    """Manages email subscriptions for investor relations"""
    
    def __init__(self):
        self.subscriptions: List[Dict] = []
        
    def add_subscription(self, company: str, email: str, signup_url: str):
        """Track email subscription"""
        sub = {
            "company": company,
            "email": email,
            "signup_url": signup_url,
            "subscribed": False,
            "confirmed": False,
            "last_email_received": None
        }
        self.subscriptions.append(sub)
        
    def generate_signup_guide(self) -> str:
        """Generate manual signup instructions"""
        
        guide = f"""
📧 EMAIL SUBSCRIPTION GUIDE
{'='*40}

Manual Setup Required for Email Notifications:

"""
        for i, sub in enumerate(self.subscriptions, 1):
            if not sub['subscribed']:
                guide += f"""
{i}. {sub['company']}
   URL: {sub['signup_url']}
   Email list: {sub['email']}
   Status: ❌ Not subscribed
   
   Steps:
   1. Visit URL above
   2. Find "Investor Relations" or "Subscribe" section
   3. Enter designated email address
   4. Confirm subscription via email
   5. Mark as complete in system
"""
        
        return guide


def main():
    """Demo the per-company pipeline system"""
    
    manager = PipelineManager()
    
    # Configure major Swedish companies
    manager.configure_swedish_majors()
    
    # Simulate some failures
    volvo = manager.pipelines["VOLV-B"]
    volvo.tiers[0].status = CollectionStatus.WORKING  # RSS works
    
    hm = manager.pipelines["HM-B"]
    hm.report_failure(hm.tiers[0], "Email parsing failed")
    
    ericsson = manager.pipelines["ERIC-B"]
    ericsson.report_failure(ericsson.tiers[0], "403 Forbidden")
    
    # Generate reports
    print(manager.generate_status_report())
    
    # Show issues
    issues = manager.get_all_issues()
    if issues:
        print(f"\n🚨 OPEN ISSUES:")
        for issue in issues[:5]:  # Show first 5
            print(f"\n[{issue['priority'].upper()}] {issue['title']}")
            print(f"   {issue['description']}")
            print(f"   Created: {issue['created']}")
    
    # Save state
    manager.save_state()
    
    # Email subscription guide
    email_mgr = EmailSubscriptionManager()
    email_mgr.add_subscription("H&M", "investor.relations@hm.com", "https://hmgroup.com/investors/")
    
    print(f"\n{email_mgr.generate_signup_guide()}")
    
    print(f"\n💡 TICKET MANAGER INTEGRATION:")
    print(f"Export issues to:")
    print(f"  - GitHub Issues (via API)")
    print(f"  - Jira (via REST API)")
    print(f"  - Trello (via API)")
    print(f"  - Local markdown files (simple start)")


if __name__ == "__main__":
    main()