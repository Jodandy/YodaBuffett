# AI-Optimized Code Standards for YodaBuffett

## Overview
This guide establishes coding standards specifically designed to maximize AI understanding and effectiveness when working with the YodaBuffett codebase. These standards make it easier for LLMs to understand context, generate accurate code, and provide meaningful assistance.

## Core Principle: Code as Communication with AI

**Traditional Approach**: Write code for humans to read  
**AI-First Approach**: Write code for both humans AND AI to understand

AI models excel at pattern recognition and context understanding, but struggle with ambiguity, implicit knowledge, and complex nested logic. Our standards leverage AI strengths while mitigating its limitations.

## Naming Conventions for AI Clarity

### 1. Descriptive, Self-Documenting Names
**❌ Avoid**:
```python
def calc(d, r, p):  # AI cannot infer purpose or parameters
    return d * r * p

def process_data(data):  # Generic, no context
    return transform(data)
```

**✅ AI-Friendly**:
```python
def calculate_financial_metrics(price_data: List[PricePoint], 
                               risk_factor: float, 
                               portfolio_weight: float) -> FinancialMetrics:
    """Calculate risk-adjusted financial metrics for portfolio position."""
    return price_data * risk_factor * portfolio_weight

def extract_revenue_from_annual_report(report_text: str) -> RevenueExtraction:
    """Extract revenue figures from parsed annual report text using ML models."""
    return ml_revenue_extractor.extract(report_text)
```

### 2. Domain-Specific Naming Patterns
**Financial Domain Context**:
```python
# Company data patterns
class CompanyFinancials:
    revenue_annual: Decimal
    revenue_quarterly: Decimal
    ebitda_margin: float
    debt_to_equity_ratio: float

# Document processing patterns  
class DocumentProcessor:
    def extract_income_statement(self, pdf_content: bytes) -> IncomeStatement
    def parse_balance_sheet(self, html_content: str) -> BalanceSheet
    def analyze_cash_flow_statement(self, text: str) -> CashFlowAnalysis

# Market data patterns
class MarketDataService:
    def fetch_real_time_prices(self, symbols: List[str]) -> PriceData
    def get_historical_ohlc(self, symbol: str, timeframe: TimeFrame) -> OHLC
    def stream_market_updates(self, symbols: List[str]) -> AsyncIterator[MarketUpdate]
```

### 3. AI-Readable Function Names
**Action + Object + Context Pattern**:
```python
# Clear action verbs that AI can understand
def validate_financial_data_completeness(data: FinancialData) -> ValidationResult
def transform_pdf_to_structured_data(pdf_bytes: bytes) -> StructuredData  
def calculate_risk_adjusted_returns(returns: Series, risk_free_rate: float) -> Series
def aggregate_multi_source_market_data(sources: List[DataSource]) -> MarketData

# Avoid ambiguous verbs
def handle_data(data)  # ❌ "handle" is vague
def process_request(req)  # ❌ "process" is generic
def do_analysis(input)  # ❌ "do" provides no information
```

## Code Structure for AI Understanding

### 1. Single Responsibility with Clear Interfaces
**AI can easily understand and generate code for focused functions**:

```python
# ✅ AI-Friendly: Single responsibility, clear purpose
class RevenueExtractor:
    """Extracts revenue figures from financial documents using ML models."""
    
    def extract_annual_revenue(self, document: FinancialDocument) -> Optional[Decimal]:
        """Extract annual revenue figure from financial document."""
        
    def extract_quarterly_revenue(self, document: FinancialDocument) -> List[Decimal]:
        """Extract quarterly revenue figures for the past year."""
        
    def validate_revenue_extraction(self, extracted: Decimal, document: FinancialDocument) -> bool:
        """Validate extracted revenue against document context."""

# ❌ Harder for AI: Multiple responsibilities, unclear purpose  
class DocumentHandler:
    def process(self, doc, type, options=None):  # Too generic
        if type == "revenue":
            # Complex nested logic that AI struggles with
            if options and options.get("quarterly"):
                # More nested conditions...
```

### 2. Explicit Type Hints and Contracts
**AI relies heavily on type information for understanding**:

```python
# ✅ Rich type information for AI understanding
from typing import List, Optional, Dict, Union, Literal
from dataclasses import dataclass
from decimal import Decimal

@dataclass
class CompanyMetrics:
    """Financial metrics for a company at a specific point in time."""
    company_id: int
    reporting_date: date
    revenue: Decimal
    net_income: Optional[Decimal]  # May be null for preliminary reports
    currency: Literal["SEK", "NOK", "DKK", "EUR", "USD"]
    
def analyze_financial_trends(
    historical_metrics: List[CompanyMetrics],
    comparison_period: Literal["quarterly", "annual"],
    risk_adjustment: float = 1.0
) -> TrendAnalysis:
    """Analyze financial trends with specified comparison period and risk adjustment."""

# ❌ No type information - AI has to guess
def analyze(data, period, risk=1.0):
    pass
```

### 3. Documentation as Code Context
**Use docstrings to provide AI with business context**:

```python
class NordicMarketDataCollector:
    """
    Collects financial data from Nordic markets (Sweden, Norway, Denmark, Finland).
    
    This collector is optimized for Nordic regulatory requirements and handles:
    - MFN.se integration for Swedish companies
    - Oslo Børs API for Norwegian companies  
    - Nasdaq Copenhagen for Danish companies
    - Nasdaq Helsinki for Finnish companies
    
    Business Context:
    - Nordic markets have unique reporting requirements
    - Data collection must respect rate limiting (10 requests/minute per market)
    - Some data sources require authentication, others are public
    - Financial calendar events drive collection priorities
    
    Usage:
        collector = NordicMarketDataCollector()
        swedish_data = collector.collect_swedish_companies(priority="annual_reports")
    """
    
    def collect_swedish_companies(
        self, 
        priority: Literal["annual_reports", "quarterly_reports", "all"] = "annual_reports"
    ) -> CollectionResult:
        """
        Collect Swedish company data with specified priority.
        
        Priority levels:
        - "annual_reports": Only annual reports (3,463 priority documents) 
        - "quarterly_reports": Quarterly and annual reports
        - "all": All document types including press releases
        
        Returns CollectionResult with success/failure counts and error details.
        """
```

## File and Module Organization for AI

### 1. Predictable Directory Structure
**AI can navigate and understand consistent patterns**:

```
backend/
├── services/
│   ├── document_processing/
│   │   ├── __init__.py
│   │   ├── text_extraction.py          # Clear purpose from filename
│   │   ├── financial_data_extraction.py
│   │   ├── document_classification.py
│   │   └── quality_validation.py
│   ├── market_data/
│   │   ├── __init__.py  
│   │   ├── real_time_feeds.py
│   │   ├── historical_data.py
│   │   └── data_validation.py
│   └── analytics/
│       ├── __init__.py
│       ├── risk_calculations.py
│       ├── performance_metrics.py
│       └── correlation_analysis.py
├── models/                              # Clear separation of concerns
│   ├── financial_data_models.py
│   ├── market_data_models.py
│   └── analytics_models.py
├── database/
│   ├── repositories/
│   └── migrations/
└── api/
    ├── endpoints/
    └── schemas/
```

### 2. Context Files (CLAUDE.md) Structure
**Standardized AI context in every module**:

```markdown
# Service: Document Processing

## Purpose
Handles extraction and processing of financial documents from Nordic markets.

## Key Components
- `text_extraction.py`: PDF/HTML text extraction using ML models
- `financial_data_extraction.py`: Structured data extraction from text
- `document_classification.py`: Automatic document type classification

## Business Context
- Processes 47K+ Nordic financial documents
- Prioritizes annual/quarterly reports over press releases
- Handles Swedish, Norwegian, Danish, Finnish languages
- Must maintain <2 minute processing time per document

## Dependencies
- Vector database for document embeddings
- ML models for text extraction
- PostgreSQL for metadata storage

## Common Patterns
```python
# Standard document processing flow
document = load_document(pdf_path)
text = extract_text(document)  
structured_data = extract_financial_data(text)
save_to_database(structured_data)
```

## Testing Approach
- Unit tests for each extraction function
- Integration tests with sample documents
- Performance tests for processing speed
- Quality tests for extraction accuracy
```

### 3. Clear Import Patterns
**Help AI understand dependencies and relationships**:

```python
# ✅ Explicit, descriptive imports
from services.document_processing.text_extraction import PDFTextExtractor
from services.market_data.real_time_feeds import NordicMarketDataFeed
from models.financial_data_models import IncomeStatement, BalanceSheet
from database.repositories.company_repository import CompanyRepository
from typing import List, Optional, Dict
from decimal import Decimal
from datetime import date, datetime

# ❌ Unclear imports that confuse AI
from utils import *
from helpers import process, validate, transform
import services as svc
```

## Error Handling for AI Clarity

### 1. Explicit Error Types
**AI can better handle specific error scenarios**:

```python
# ✅ Specific error types that AI can understand and handle
class DocumentProcessingError(Exception):
    """Base exception for document processing failures."""
    pass

class PDFExtractionError(DocumentProcessingError):
    """Raised when PDF text extraction fails."""
    def __init__(self, pdf_path: str, reason: str):
        self.pdf_path = pdf_path
        self.reason = reason
        super().__init__(f"PDF extraction failed for {pdf_path}: {reason}")

class FinancialDataValidationError(DocumentProcessingError):
    """Raised when extracted financial data fails validation."""
    def __init__(self, field: str, value: str, expected_type: str):
        self.field = field
        self.value = value
        self.expected_type = expected_type
        super().__init__(f"Invalid {field}: got '{value}', expected {expected_type}")

# Usage that AI can understand and replicate
try:
    extracted_data = extract_financial_data(document)
except PDFExtractionError as e:
    logger.error(f"PDF extraction failed: {e.reason}")
    return ProcessingResult.failure(error_type="pdf_extraction", details=e.reason)
except FinancialDataValidationError as e:
    logger.warning(f"Data validation failed for {e.field}")
    return ProcessingResult.partial_success(invalid_fields=[e.field])
```

### 2. Consistent Return Types
**AI can predict and generate consistent return patterns**:

```python
# ✅ Consistent result pattern across all services
@dataclass
class ProcessingResult:
    success: bool
    data: Optional[Any] = None
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    processing_time_ms: int = 0
    
    @classmethod
    def success_with_data(cls, data: Any, processing_time: int = 0) -> 'ProcessingResult':
        return cls(success=True, data=data, processing_time_ms=processing_time)
    
    @classmethod  
    def failure(cls, error: str) -> 'ProcessingResult':
        return cls(success=False, errors=[error])

# AI can now predict this pattern for any service
def extract_revenue_data(document: FinancialDocument) -> ProcessingResult:
    start_time = time.time()
    try:
        revenue_data = perform_extraction(document)
        processing_time = int((time.time() - start_time) * 1000)
        return ProcessingResult.success_with_data(revenue_data, processing_time)
    except ExtractionError as e:
        return ProcessingResult.failure(str(e))
```

## Testing Standards for AI

### 1. AI-Readable Test Names
**Test names should describe the exact scenario**:

```python
# ✅ AI can understand test intent and generate similar tests
def test_revenue_extraction_from_swedish_annual_report_with_currency_sek():
    """Test revenue extraction from Swedish annual report with SEK currency."""
    
def test_pdf_extraction_fails_gracefully_when_document_is_corrupted():
    """Test that PDF extraction handles corrupted files without crashing."""
    
def test_market_data_validation_rejects_negative_prices():
    """Test that market data validation rejects negative price values."""

# ❌ Generic test names that don't help AI understand intent  
def test_extraction():
def test_validation_error():
def test_edge_case():
```

### 2. Structured Test Patterns
**Consistent AAA (Arrange-Act-Assert) pattern that AI can replicate**:

```python
def test_financial_metrics_calculation_with_valid_data():
    """Test financial metrics calculation with complete, valid input data."""
    
    # Arrange - Set up test data with clear variable names
    company_data = CompanyFinancialData(
        revenue=Decimal("1000000"),
        expenses=Decimal("800000"), 
        shares_outstanding=1000000
    )
    expected_eps = Decimal("0.20")  # (1M - 0.8M) / 1M shares
    
    # Act - Perform the operation being tested
    calculated_metrics = calculate_financial_metrics(company_data)
    
    # Assert - Verify results with clear assertions
    assert calculated_metrics.earnings_per_share == expected_eps
    assert calculated_metrics.profit_margin == Decimal("0.20")
    assert calculated_metrics.is_profitable is True
```

## Performance Considerations for AI

### 1. Clear Performance Contracts
**Help AI understand and maintain performance requirements**:

```python
class DocumentProcessor:
    """
    Document processor with performance guarantees.
    
    Performance Requirements:
    - Process single document: <2 minutes
    - Batch process 100 documents: <30 minutes  
    - Memory usage: <500MB per document
    - Error rate: <1% for standard PDF formats
    """
    
    @performance_monitor(max_time_seconds=120)  # AI can see performance constraints
    def process_document(self, document_path: str) -> ProcessingResult:
        """Process single document within 2-minute time limit."""
        
    @batch_performance_monitor(max_time_per_item_seconds=18)  # 30 min / 100 docs
    def process_document_batch(self, document_paths: List[str]) -> BatchProcessingResult:
        """Process batch of documents with per-item time monitoring."""
```

## Configuration and Constants

### 1. Centralized, Well-Named Configuration
**AI can understand and modify configuration easily**:

```python
# ✅ AI-readable configuration with clear business context
class NordicMarketConfig:
    """Configuration for Nordic market data collection."""
    
    # Rate limiting to respect data source policies
    REQUESTS_PER_MINUTE_MFN_SE = 10
    REQUESTS_PER_MINUTE_OSLO_BORS = 15
    
    # Document processing priorities
    PRIORITY_DOCUMENT_TYPES = ["annual_report", "quarterly_report"] 
    SECONDARY_DOCUMENT_TYPES = ["press_release", "governance_report"]
    
    # Performance thresholds
    MAX_DOCUMENT_PROCESSING_TIME_SECONDS = 120
    MAX_BATCH_SIZE_FOR_CONCURRENT_PROCESSING = 50
    
    # Business logic constants
    MINIMUM_REVENUE_FOR_ANALYSIS = Decimal("1000000")  # 1M in local currency
    SUPPORTED_CURRENCIES = ["SEK", "NOK", "DKK", "EUR"]

# ❌ Generic configuration that provides no context
class Config:
    RATE_LIMIT = 10
    TIMEOUT = 120
    MIN_VAL = 1000000
```

## Large Codebase Navigation for AI

### The Challenge: AI Context Limits
As YodaBuffett grows into a large, complex platform, AI faces fundamental challenges:
- **Context Windows**: AI can only see ~100K tokens at once (not entire codebase)
- **Relationship Mapping**: Hard to understand connections between distant modules
- **Architecture Understanding**: Difficult to grasp overall system design from fragments
- **Finding Relevant Code**: Struggles to locate the right files/functions for specific tasks

### Solution: Hierarchical Context Architecture

#### 1. Domain-Driven Directory Structure
**Organize by business domains, not technical layers**:

```
backend/
├── domains/                           # Business domain grouping
│   ├── document_intelligence/         # Complete domain in one place
│   │   ├── __domain__.md             # Domain overview for AI
│   │   ├── models/                   # Domain-specific models
│   │   │   ├── document_models.py
│   │   │   └── extraction_models.py
│   │   ├── services/                 # Domain services
│   │   │   ├── pdf_processor.py
│   │   │   └── text_extractor.py
│   │   ├── repositories/             # Domain data access
│   │   │   └── document_repository.py
│   │   └── api/                      # Domain API endpoints
│   │       └── document_endpoints.py
│   ├── market_data/                   # Self-contained market data domain
│   │   ├── __domain__.md
│   │   ├── models/
│   │   ├── services/
│   │   └── repositories/
│   ├── analytics/                     # Analytics domain
│   │   ├── __domain__.md
│   │   ├── models/
│   │   ├── services/
│   │   └── repositories/
│   └── user_management/               # User domain
│       ├── __domain__.md
│       └── ...
├── shared/                            # Cross-domain utilities
│   ├── __shared__.md                 # What's shared and why
│   ├── database/
│   ├── auth/
│   └── monitoring/
└── integrations/                      # External service integrations
    ├── __integrations__.md
    ├── bloomberg_api/
    ├── mfn_se/
    └── openai/
```

#### 2. AI Context Files (__domain__.md)
**Each domain gets a comprehensive AI context file**:

```markdown
# Domain: Document Intelligence

## Business Purpose
Processes 47K+ Nordic financial documents to extract structured financial data.
Supports annual reports, quarterly reports, and press releases in Swedish, Norwegian, Danish, Finnish.

## Key Capabilities
- PDF text extraction with OCR fallback
- Financial data extraction using ML models  
- Document classification and validation
- Multi-language processing support

## Architecture Overview
```
Document Upload → Text Extraction → ML Analysis → Structured Data → Database Storage
     ↓              ↓                ↓             ↓               ↓
pdf_processor → text_extractor → financial_analyzer → models → repositories
```

## Core Models
- `FinancialDocument`: Represents any financial document
- `ExtractionResult`: Structured data extracted from documents
- `DocumentMetadata`: Document classification and context

## Key Services  
- `PDFProcessor`: Handles PDF parsing and text extraction
- `FinancialDataExtractor`: ML-based financial data extraction
- `DocumentClassifier`: Automatic document type detection

## API Endpoints
- `POST /documents/upload`: Upload new document for processing
- `GET /documents/{id}/extract`: Get extracted financial data
- `GET /documents/search`: Semantic search across documents

## Dependencies
- **Vector Database**: Document embeddings and semantic search
- **ML Models**: Text extraction and financial analysis
- **File Storage**: Original document storage
- **Market Data Domain**: Company information for validation

## Common Patterns
```python
# Standard document processing flow
document = PDFProcessor().extract_text(pdf_bytes)
extracted_data = FinancialDataExtractor().extract(document)
result = DocumentRepository().save_extraction(extracted_data)
```

## Performance Requirements
- Single document processing: <2 minutes
- Batch processing: 100 docs in <30 minutes
- Accuracy: >95% for standard Nordic financial reports

## Testing Strategy
- Unit tests: Individual service functionality
- Integration tests: End-to-end document processing
- Performance tests: Processing speed and memory usage
- Quality tests: Extraction accuracy validation
```

#### 3. AI-Automated Documentation Maintenance
**Instructions for AI assistants to keep documentation current**:

##### Auto-Update Triggers for AI
When working on YodaBuffett code, AI assistants should automatically update relevant .md files when:

1. **New files added to a domain**: Update `__domain__.md` with new components
2. **New API endpoints created**: Update both domain and `ARCHITECTURE_MAP.md`
3. **Dependencies changed**: Update cross-reference sections
4. **Performance requirements modified**: Update performance sections
5. **New models/services added**: Update component lists

##### AI Documentation Update Protocol
```markdown
# AI Assistant Documentation Update Instructions

## When Adding New Code
1. **Identify the domain**: Which __domain__.md file needs updating?
2. **Update component lists**: Add new classes/functions to relevant sections
3. **Update architecture diagrams**: Modify text-based flow diagrams if needed
4. **Update cross-references**: Add relationships to other domains
5. **Update ARCHITECTURE_MAP.md**: Global system overview changes

## Template for AI Updates
When adding new component, append to __domain__.md:

```markdown
## Recent Updates (AI-Generated)
### [DATE] - Added [COMPONENT_NAME]
- **Purpose**: [Brief description]
- **Location**: `[file_path]` 
- **Dependencies**: [List dependencies]
- **Used by**: [List components that use this]
- **API endpoints**: [If applicable]
```

## AI Self-Check Questions
Before finalizing code changes, AI should verify:
- [ ] Is __domain__.md component list current?
- [ ] Are cross-domain relationships documented?
- [ ] Is ARCHITECTURE_MAP.md flow diagram accurate?
- [ ] Are performance requirements still valid?
- [ ] Do examples in documentation still work?
```

##### Automated Validation Script for AI
```python
# File: tools/ai_docs_validator.py
"""
Script for AI assistants to validate documentation currency.
Run this after making code changes to identify outdated documentation.
"""

def validate_domain_docs():
    """Check if __domain__.md files match actual code structure."""
    
    # Check if all Python files are documented
    for domain_path in get_domain_paths():
        documented_files = extract_documented_files(domain_path / "__domain__.md")
        actual_files = list_python_files(domain_path)
        
        missing_files = actual_files - documented_files
        if missing_files:
            print(f"⚠️ {domain_path.name}: Missing documentation for {missing_files}")
            
    # Check if documented files still exist
    for domain_path in get_domain_paths():
        documented_files = extract_documented_files(domain_path / "__domain__.md")
        actual_files = list_python_files(domain_path)
        
        obsolete_docs = documented_files - actual_files  
        if obsolete_docs:
            print(f"⚠️ {domain_path.name}: Obsolete documentation for {obsolete_docs}")

def suggest_doc_updates():
    """Generate suggested updates for AI assistants."""
    # Analyze recent git commits to identify changes needing doc updates
    # Compare current code structure with documented structure
    # Generate specific update suggestions
```

#### 4. Global Architecture Map with Auto-Update Instructions
**File**: `ARCHITECTURE_MAP.md` (in project root)
```markdown
# YodaBuffett: AI-Readable Architecture Map

<!-- AI: This file should be updated whenever domain relationships change -->
<!-- AI: Update domain count in "System Overview" when new domains added -->
<!-- AI: Update data flow diagram when new services or databases added -->

## System Overview
Multi-product financial intelligence platform with 4 core domains and 3 database types.

## Domain Relationships
<!-- AI: Update this diagram when adding new domains or changing relationships -->
```
┌─ Document Intelligence ──────────┐    ┌─ Market Data ─────────────┐
│ • PDF Processing                 │    │ • Real-time Feeds          │
│ • Financial Extraction           │────│ • Historical Data          │
│ • 47K Nordic Documents          │    │ • Multi-source Validation  │
└───────────────────────────────────┘    └────────────────────────────┘
            │                                        │
            ▼                                        ▼
┌─ Analytics ───────────────────────┐    ┌─ User Management ──────────┐
│ • Cross-company Analysis          │    │ • Authentication           │
│ • Predictive Models              │    │ • Subscriptions            │ 
│ • Pattern Detection              │    │ • Multi-tenant Support     │
└───────────────────────────────────┘    └────────────────────────────┘
```

<!-- AI: When adding new domain, copy template and update connections -->
<!-- Template:
┌─ [DOMAIN_NAME] ───────────────────┐
│ • [Key capability 1]              │
│ • [Key capability 2]              │
│ • [Key capability 3]              │
└───────────────────────────────────┘
-->

## Finding Code Guidelines
<!-- AI: Update these paths when moving or renaming files -->
<!-- AI: Add new sections when creating new domains -->

### For Document Processing:
- Text extraction: `domains/document_intelligence/services/`
- Models: `domains/document_intelligence/models/`
- API: `domains/document_intelligence/api/`

### For Market Data:
- Real-time feeds: `domains/market_data/services/real_time_feeds.py`
- Historical data: `domains/market_data/services/historical_data.py`
- Data validation: `domains/market_data/services/data_validation.py`

<!-- AI: Add new domain section template:
### For [DOMAIN_NAME]:
- [Key functionality]: `domains/[domain_name]/services/[main_service].py`
- [Another functionality]: `domains/[domain_name]/services/[other_service].py`
- Models: `domains/[domain_name]/models/`
- API: `domains/[domain_name]/api/`
-->
```

#### 5. Semantic Naming with AI Update Hints
```python
# ✅ AI-discoverable names with maintenance hints
class SwedishAnnualReportProcessor:           # AI: Update __domain__.md when modifying
    """
    AI Maintenance Note: This class is documented in:
    - domains/document_intelligence/__domain__.md (Key Services section)
    - ARCHITECTURE_MAP.md (Document Processing paths)
    
    When modifying this class, also update performance requirements
    in __domain__.md if processing time changes.
    """
    
def extract_revenue_from_income_statement():     # AI: Cross-referenced in analytics domain
    """
    Extract revenue figures from income statement text.
    
    AI Maintenance Note: 
    - Used by: domains/analytics/services/financial_metrics_calculator.py
    - Update cross-references if function signature changes
    - Performance requirement: <5 seconds per document
    """
```

#### 6. Cross-Reference Documentation with Auto-Update
```markdown
# Service: Financial Data Extractor

## Related Components (AI: Keep This Section Current)
<!-- AI: Scan codebase for usages of FinancialDataExtractor and update this list -->
<!-- AI: When adding new dependencies, add them here with brief description -->

- **Market Data Validation** (`domains/market_data/services/data_validation.py`): 
  Validates extracted financial metrics against market data
- **Company Repository** (`domains/market_data/repositories/company_repository.py`):
  Company information for context during extraction
- **ML Feature Store** (`shared/database/ml_database.py`):
  Stores extracted features for machine learning models

<!-- AI: Use this template for new relationships:
- **[Component Name]** (`[file_path]`):
  [Brief description of relationship]
-->

## AI Update Instructions
When modifying FinancialDataExtractor:
1. Check if new dependencies need to be added to "Related Components"
2. Update usage examples if method signatures change
3. Update performance requirements if processing time changes
4. Scan for new files importing this class and add to relationships
```

## AI Documentation Maintenance Workflow

### Daily Automation (for AI assistants):
1. **Scan for new files**: Check if any new .py files need documentation
2. **Validate cross-references**: Ensure documented relationships still exist
3. **Check performance claims**: Verify documented performance requirements
4. **Update component counts**: Keep architecture overview current

### Weekly Validation (for AI assistants):
1. **Run docs_validator.py**: Identify stale documentation
2. **Review git commits**: Find changes that need doc updates
3. **Validate examples**: Ensure code examples in docs still work
4. **Update architecture diagrams**: Reflect any structural changes

### AI Prompt for Documentation Updates:
```
When making code changes to YodaBuffett:

1. Identify which domain(s) are affected
2. Update the relevant __domain__.md file(s) with:
   - New components added
   - Changed dependencies  
   - Modified API endpoints
   - Updated performance characteristics
3. Update ARCHITECTURE_MAP.md if:
   - New domains added
   - Domain relationships changed
   - New major services created
4. Update cross-references in related domains
5. Validate examples still work with your changes

Use the AI validation script to check your updates:
`python tools/ai_docs_validator.py`
```

## Summary: Self-Maintaining AI-Friendly Architecture

**✅ AI Can Maintain When:**
- **Clear update triggers**: Know when to update docs
- **Automated validation**: Scripts to check doc currency  
- **Template-driven updates**: Standard formats for additions
- **Cross-reference tracking**: Systematic relationship documentation
- **Embedded update instructions**: Hints in code about what to update

**❌ Documentation Becomes Stale When:**
- No clear ownership of doc maintenance
- Manual processes without automation
- Missing relationships between distant code
- No validation of documentation accuracy
- Updates scattered across multiple files

This creates a **self-healing documentation system** where AI assistants automatically maintain context as the codebase evolves, ensuring the platform remains navigable even at enterprise scale.