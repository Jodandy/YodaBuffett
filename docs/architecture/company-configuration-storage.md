# Company Configuration Storage Strategy

## Overview
Each Nordic company gets its own configuration "profile" containing all data collection strategies, patterns, and operational knowledge.

## Folder Structure Approach

### Option 1: Code-Based Company Profiles
```
nordic-reports-service/
├── companies/
│   ├── sweden/
│   │   ├── volvo_group/
│   │   │   ├── config.yaml           # Main configuration
│   │   │   ├── url_patterns.py       # URL prediction logic
│   │   │   ├── scrapers.py          # Custom scrapers if needed
│   │   │   ├── email_patterns.py    # Email parsing rules
│   │   │   └── calendar_config.py   # IR calendar specifics
│   │   ├── hm/
│   │   │   ├── config.yaml
│   │   │   ├── url_patterns.py
│   │   │   └── custom_scraper.py    # H&M has complex website
│   │   └── ericsson/
│   │       └── config.yaml
│   ├── norway/
│   │   ├── equinor/
│   │   └── telenor/
│   ├── denmark/
│   │   └── novo_nordisk/
│   └── finland/
│       └── nokia/
└── base/
    ├── abstract_company.py          # Base company class
    ├── pattern_matcher.py           # Common pattern logic
    └── fallback_strategies.py       # Default behaviors
```

### Example: Volvo Group Configuration
```yaml
# companies/sweden/volvo_group/config.yaml
company:
  name: "Volvo Group"
  ticker: "VOLV-B"  
  country: "SE"
  exchange: "OMXS30"
  website: "https://www.volvogroup.com"
  ir_website: "https://www.volvogroup.com/investors/"
  
contact:
  ir_email: "investor.relations@volvo.com"
  support_email: "info@volvo.com"
  
data_sources:
  rss:
    - url: "https://www.volvogroup.com/se/news-and-media/events/_jcr_content/root/responsivegrid/eventlist.feed.xml"
      type: "financial_events"
      priority: 1
      status: "active"
      last_success: "2024-01-27T10:30:00Z"
      
  email_subscriptions:
    - email: "yodabuffett.ir@gmail.com"
      signup_url: "https://www.volvogroup.com/investors/subscribe/"
      keywords: ["rapport", "report", "quarterly", "annual"]
      priority: 2
      
  ir_calendar:
    - url: "https://www.volvogroup.com/investors/calendar/"
      scraping_method: "css_selectors"
      selectors:
        events: ".calendar-event"
        date: ".event-date"
        title: ".event-title"
        type: ".event-type"
      priority: 3

url_patterns:
  primary_pattern: "https://volvogroup.com/content/dam/reports/{year}/q{quarter}/volvo-group-q{quarter}-{year}-sve.pdf"
  confidence: 0.95
  last_updated: "2024-01-15"
  
  variations:
    - "https://volvogroup.com/content/dam/reports/{year}/q{quarter}/volvo-group-q{quarter}-{year}-eng.pdf"
    - "https://volvogroup.com/media/documents/{year}/q{quarter}/volvo-group-{quarter}q{year}.pdf" 
    - "https://volvogroup.com/content/dam/reports/{year}/{quarter}/volvo-group-interim-report-{quarter}-{year}.pdf"
    
  annual_pattern: "https://volvogroup.com/content/dam/reports/{year}/annual/volvo-group-annual-report-{year}.pdf"

scraping:
  user_agent: "YodaBuffett-Nordic/1.0 (+https://yodabuffett.com/about)"
  rate_limit: "1 request per 5 seconds"
  retry_attempts: 3
  timeout: 30
  
  custom_headers:
    Accept-Language: "sv-SE,sv;q=0.9,en;q=0.8"
    
  blocked_indicators:
    - "Access Denied"
    - "Cloudflare"
    - "Please complete the security check"

reporting_schedule:
  # Based on historical data
  q1_expected: "April 25, 07:30 CET"
  q2_expected: "July 20, 07:30 CET"  
  q3_expected: "October 25, 07:30 CET"
  annual_expected: "February 15, 07:30 CET"
  
  # Variance (how much dates can shift)
  date_variance_days: 3
  early_warning_days: 7  # Alert if no report 7 days after expected

language:
  primary: "sv"          # Swedish
  secondary: "en"        # English fallback
  nlp_model: "sv_core_news_sm"  # spaCy model for Swedish

operational:
  priority: "high"       # Large cap company
  manual_fallback: true
  automation_target: 95  # Aim for 95% automation
  
  escalation:
    tier1_failure_threshold: 2    # Create ticket after 2 failures
    tier2_failure_threshold: 5    # Escalate after 5 failures
    manual_timeout_hours: 24      # Create manual ticket after 24h
    
  monitoring:
    health_check_frequency: "daily"
    pattern_validation_frequency: "weekly"
    contact_verification_frequency: "monthly"
```

### Company-Specific Code
```python
# companies/sweden/volvo_group/url_patterns.py
from datetime import datetime
from base.pattern_matcher import BasePatternMatcher

class VolvoPatternMatcher(BasePatternMatcher):
    """Volvo-specific URL pattern logic"""
    
    def __init__(self):
        super().__init__()
        self.base_pattern = "https://volvogroup.com/content/dam/reports/{year}/q{quarter}/volvo-group-q{quarter}-{year}-{language}.pdf"
        self.languages = ["sve", "eng"]
        
    def predict_quarterly_url(self, year: int, quarter: int) -> List[str]:
        """Generate Volvo Q1/Q2/Q3 URL predictions"""
        urls = []
        
        for lang in self.languages:
            url = self.base_pattern.format(
                year=year,
                quarter=quarter,
                language=lang
            )
            urls.append(url)
            
        # Volvo-specific variations
        urls.extend([
            f"https://volvogroup.com/content/dam/reports/{year}/interim/volvo-group-interim-q{quarter}-{year}.pdf",
            f"https://volvogroup.com/media/publications/{year}/q{quarter}/quarterly-report-{quarter}-{year}.pdf"
        ])
        
        return urls
        
    def predict_annual_url(self, year: int) -> List[str]:
        """Generate Volvo annual report URLs"""
        return [
            f"https://volvogroup.com/content/dam/reports/{year}/annual/volvo-group-annual-report-{year}.pdf",
            f"https://volvogroup.com/content/dam/reports/{year}/annual/annual-report-{year}-eng.pdf",
            f"https://volvogroup.com/media/publications/{year}/volvo-annual-report-{year}.pdf"
        ]
        
    def validate_downloaded_content(self, content: bytes, expected_period: str) -> bool:
        """Volvo-specific content validation"""
        text = self.extract_text_from_pdf(content)
        
        # Check for Volvo-specific markers
        volvo_markers = [
            "AB Volvo",
            "Volvo Group", 
            "Aktiebolaget Volvo",
            expected_period
        ]
        
        found_markers = sum(1 for marker in volvo_markers if marker.lower() in text.lower())
        return found_markers >= 3  # At least 3 markers must be present
```

### H&M Complex Configuration
```python
# companies/sweden/hm/custom_scraper.py
from selenium import webdriver
from base.scrapers import BasePlaywrightScraper

class HMCustomScraper(BasePlaywrightScraper):
    """H&M has complex JavaScript-heavy IR page"""
    
    async def scrape_reports_page(self, year: int, quarter: int):
        """H&M requires JavaScript execution and form interaction"""
        
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page()
            
            await page.goto("https://hmgroup.com/investors/reports/")
            
            # H&M has year filter dropdown
            await page.select_option("#year-filter", str(year))
            await page.click("#filter-button")
            await page.wait_for_load_state("networkidle")
            
            # Find quarterly reports
            report_links = await page.locator(f'a:has-text("Q{quarter} {year}")').all()
            
            urls = []
            for link in report_links:
                href = await link.get_attribute("href")
                if href and href.endswith('.pdf'):
                    urls.append(href)
                    
            await browser.close()
            return urls
```

## Database Integration

### Company Configuration Loading
```python
# System startup loads all company configurations
class CompanyConfigManager:
    def __init__(self):
        self.companies = {}
        self.load_all_companies()
        
    def load_all_companies(self):
        """Load all company configurations from filesystem"""
        
        countries = ["sweden", "norway", "denmark", "finland"]
        
        for country in countries:
            country_path = Path(f"companies/{country}")
            if country_path.exists():
                for company_dir in country_path.iterdir():
                    if company_dir.is_dir():
                        config = self.load_company_config(company_dir)
                        self.companies[config.ticker] = config
                        
    def load_company_config(self, company_dir: Path):
        """Load single company configuration"""
        
        # Load YAML config
        config_file = company_dir / "config.yaml"
        with open(config_file) as f:
            yaml_config = yaml.safe_load(f)
            
        # Load custom Python modules if they exist
        custom_patterns = None
        patterns_file = company_dir / "url_patterns.py"
        if patterns_file.exists():
            spec = importlib.util.spec_from_file_location("patterns", patterns_file)
            patterns_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(patterns_module)
            custom_patterns = patterns_module
            
        # Create company configuration object
        return CompanyConfig(
            yaml_config=yaml_config,
            custom_patterns=custom_patterns,
            company_dir=company_dir
        )
```

### Runtime Configuration Updates
```python
async def update_company_config(ticker: str, updates: dict):
    """Update company configuration at runtime"""
    
    # Update in-memory config
    company = company_manager.get_company(ticker)
    company.update_config(updates)
    
    # Update database
    await db.nordic_companies.update_one(
        {"ticker": ticker},
        {"$set": updates}
    )
    
    # Update YAML file
    config_path = Path(f"companies/{company.country.lower()}/{company.slug}/config.yaml")
    with open(config_path, 'w') as f:
        yaml.dump(company.to_dict(), f)
        
    # Commit to git for version control
    subprocess.run([
        "git", "add", str(config_path),
        "git", "commit", "-m", f"Update {ticker} configuration: {list(updates.keys())}"
    ])
```

## Version Control & Deployment

### Git Integration
```bash
# Company configurations are version controlled
git log companies/sweden/volvo_group/config.yaml

# Example commits:
# feat(volvo): add new URL pattern variation for Q4 reports
# fix(volvo): update RSS feed URL after website redesign  
# config(volvo): increase retry timeout to 45 seconds
```

### Configuration Deployment
```python
# Deploy configuration changes without service restart
async def deploy_config_updates():
    """Hot-reload company configurations"""
    
    # Pull latest configs from git
    subprocess.run(["git", "pull", "origin", "main"])
    
    # Reload configurations
    company_manager.reload_all_companies()
    
    # Validate configurations
    validation_errors = company_manager.validate_all_configs()
    if validation_errors:
        logger.error(f"Configuration validation failed: {validation_errors}")
        # Rollback
        subprocess.run(["git", "reset", "--hard", "HEAD~1"])
        company_manager.reload_all_companies()
    
    logger.info("Configuration update deployed successfully")
```

## Benefits of This Approach

### 🏗️ **Organized & Maintainable**
- Each company is self-contained
- Easy to find and modify company-specific logic
- Clear separation of concerns

### 🔄 **Version Controlled**
- All changes tracked in git
- Easy rollbacks if configuration breaks
- Collaboration-friendly

### ⚡ **Performance Optimized**
- Configurations loaded once at startup
- Custom code compiled and cached
- No database queries for basic pattern matching

### 🧪 **Testable**
- Each company configuration can be unit tested
- Pattern validation against historical data
- A/B testing of different approaches

### 📈 **Scalable**
- Easy to add new companies
- Template-based setup for similar companies
- Shared base classes for common functionality

This creates a **company-centric architecture** where each Nordic company has its own "playbook" for data collection! 🎯