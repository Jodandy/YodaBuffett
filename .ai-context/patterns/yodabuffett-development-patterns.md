# YodaBuffett Development Patterns for AI Assistants

*Last updated: 2025-01-12*

## Overview
This document captures successful patterns, approaches, and anti-patterns discovered during YodaBuffett development. AI assistants should follow these patterns for consistency and efficiency.

## Architectural Patterns

### Domain-Driven Development Pattern
**When to use**: Any new feature development or major component addition

**Successful Pattern**:
```
1. Identify primary business domain (document_intelligence, market_data, analytics, user_management)
2. Design within domain boundaries following established patterns:
   - models/ (data structures)
   - services/ (business logic) 
   - repositories/ (data access)
   - api/ (external interfaces)
3. Update domain __domain__.md with new components
4. Handle cross-domain integration through shared interfaces
```

**Why it works**: 
- Maintains clear separation of concerns
- AI assistants can focus on single domain context
- Reduces complexity and coupling
- Enables parallel development across domains

**Anti-pattern to avoid**: 
❌ Creating components that span multiple domains without clear interface boundaries

### AI-Friendly Code Organization Pattern
**When to use**: Any code creation or refactoring

**Successful Pattern**:
```python
# Descriptive, searchable names
class NordicFinancialDocumentProcessor:
    """
    Processes Nordic financial documents for YodaBuffett platform.
    
    AI Context: This handles PDF extraction and financial data parsing
    for Swedish, Norwegian, Danish, and Finnish company reports.
    """
    
    def extract_revenue_from_income_statement(self, document: FinancialDocument) -> RevenueExtraction:
        """Extract revenue figures with confidence scoring."""
        # Implementation with clear error handling
        try:
            result = self._perform_extraction(document)
            return RevenueExtraction.success(result)
        except SpecificExtractionError as e:
            logger.error(f"Revenue extraction failed for {document.id}: {e}")
            return RevenueExtraction.failure(str(e))
```

**Why it works**:
- AI assistants can find relevant code through natural language search
- Clear business domain terminology
- Explicit error handling patterns
- Self-documenting through names and types

## Performance Optimization Patterns

### Profile-First Optimization Pattern
**When to use**: Addressing performance issues or optimization requests

**Successful Pattern**:
1. **Profile before optimizing**: Use actual profiling tools, don't guess bottlenecks
2. **Measure current state**: Get baseline performance metrics
3. **Identify specific bottleneck**: Focus on the actual constraint
4. **Implement targeted solution**: Address root cause, not symptoms
5. **Measure improvement**: Validate optimization effectiveness
6. **Update documentation**: Record new performance characteristics

**Example Implementation**:
```python
# Before optimization - always profile first
import cProfile
import time

def profile_correlation_analysis():
    profiler = cProfile.Profile()
    profiler.enable()
    
    # Run the operation
    start_time = time.time()
    result = calculate_company_correlations(companies)
    end_time = time.time()
    
    profiler.disable()
    profiler.print_stats(sort='cumulative')
    
    print(f"Total time: {end_time - start_time:.2f}s")
    return result
```

**Anti-pattern to avoid**: 
❌ Optimizing based on assumptions without profiling
❌ Micro-optimizing non-bottlenecks
❌ Forgetting to update performance documentation

### Vectorized Operations Pattern
**When to use**: Mathematical computations, especially involving arrays or matrices

**Successful Pattern**:
```python
# Use NumPy for mathematical operations instead of loops
import numpy as np

# ❌ Slow: Individual calculations
def calculate_similarities_slow(embeddings):
    similarities = []
    for i, emb_a in enumerate(embeddings):
        row = []
        for j, emb_b in enumerate(embeddings):
            similarity = np.dot(emb_a, emb_b)
            row.append(similarity)
        similarities.append(row)
    return np.array(similarities)

# ✅ Fast: Vectorized operations  
def calculate_similarities_fast(embeddings):
    # Single matrix multiplication handles all pairs
    embeddings_matrix = np.array(embeddings)
    return np.dot(embeddings_matrix, embeddings_matrix.T)
```

**Performance Impact**: Often 10-100x speedup for mathematical operations

## Database Access Patterns

### Multi-Database Strategy Pattern
**When to use**: Data access that involves multiple database types

**Successful Pattern**:
```python
class AnalyticsDataService:
    """Coordinates data access across YodaBuffett's multi-database architecture."""
    
    def __init__(self):
        self.pg_client = PostgreSQLClient()      # Financial data
        self.vector_client = VectorDBClient()    # Document embeddings  
        self.ml_client = MLDatabaseClient()      # Pre-computed features
        self.cache_client = RedisClient()        # Fast access cache
    
    def get_company_analysis_data(self, company_id: str) -> CompanyAnalysisData:
        # Check cache first
        cached = self.cache_client.get(f"analysis:{company_id}")
        if cached:
            return CompanyAnalysisData.from_cache(cached)
        
        # Fetch from appropriate databases
        financial_data = self.pg_client.get_company_metrics(company_id)
        embeddings = self.vector_client.get_company_embeddings(company_id)
        ml_features = self.ml_client.get_precomputed_features(company_id)
        
        # Combine and cache result
        result = CompanyAnalysisData.combine(financial_data, embeddings, ml_features)
        self.cache_client.set(f"analysis:{company_id}", result.to_cache(), expire=3600)
        
        return result
```

**Why it works**: 
- Each database used for its strengths
- Caching reduces expensive operations
- Clear separation of data types

### Batch Database Operations Pattern  
**When to use**: Processing multiple records or large datasets

**Successful Pattern**:
```python
def update_company_metrics_batch(metrics_updates: List[CompanyMetrics]) -> BatchResult:
    """Update multiple company metrics in efficient batch operations."""
    
    # Group by operation type for optimal database usage
    inserts = [m for m in metrics_updates if m.is_new]
    updates = [m for m in metrics_updates if m.is_modified]
    
    results = BatchResult()
    
    # Batch insert new metrics
    if inserts:
        insert_result = db.bulk_insert('company_metrics', [m.to_dict() for m in inserts])
        results.add_inserts(insert_result)
    
    # Batch update existing metrics
    if updates:
        update_result = db.bulk_update('company_metrics', [m.to_update_dict() for m in updates])
        results.add_updates(update_result)
    
    return results
```

**Anti-pattern to avoid**: 
❌ Individual database operations in loops (N+1 query problem)

## Error Handling Patterns

### Explicit Error Types Pattern
**When to use**: Any operation that can fail in predictable ways

**Successful Pattern**:
```python
# Define specific error types for different failure modes
class DocumentProcessingError(Exception):
    """Base class for document processing failures."""
    pass

class PDFExtractionError(DocumentProcessingError):
    """PDF text extraction failed."""
    def __init__(self, document_path: str, reason: str):
        self.document_path = document_path
        self.reason = reason
        super().__init__(f"PDF extraction failed for {document_path}: {reason}")

class FinancialDataValidationError(DocumentProcessingError):
    """Extracted financial data failed validation."""
    def __init__(self, field: str, value: Any, expected_type: type):
        self.field = field
        self.value = value  
        self.expected_type = expected_type
        super().__init__(f"Invalid {field}: got {value}, expected {expected_type}")

# Usage with clear error handling
def process_financial_document(document_path: str) -> ProcessingResult:
    try:
        text = extract_pdf_text(document_path)
        financial_data = parse_financial_data(text)
        validated_data = validate_financial_data(financial_data)
        return ProcessingResult.success(validated_data)
    
    except PDFExtractionError as e:
        logger.error(f"PDF processing failed: {e}")
        return ProcessingResult.failure("pdf_extraction", str(e))
    
    except FinancialDataValidationError as e:
        logger.warning(f"Data validation failed: {e}")
        return ProcessingResult.partial_success(validated_fields=e.field)
```

**Why it works**:
- AI assistants can understand and handle specific error scenarios
- Clear debugging information
- Enables appropriate error recovery strategies

## Testing Patterns

### AI-Readable Test Pattern
**When to use**: Writing any tests for YodaBuffett components

**Successful Pattern**:
```python
def test_nordic_financial_document_processing_with_valid_swedish_annual_report():
    """Test complete processing pipeline with real Swedish annual report data."""
    
    # Arrange - Set up test data with clear context
    test_document_path = "test_data/swedish_annual_report_aak_2023.pdf"
    expected_revenue = Decimal("15_847_000_000")  # 15.847 billion SEK
    expected_currency = "SEK"
    
    processor = NordicFinancialDocumentProcessor()
    
    # Act - Perform the operation being tested
    result = processor.process_document(test_document_path)
    
    # Assert - Verify specific outcomes
    assert result.success is True, f"Processing failed: {result.error_message}"
    assert result.extracted_data.revenue == expected_revenue
    assert result.extracted_data.currency == expected_currency
    assert result.processing_time_seconds < 120  # Under 2-minute requirement
    assert result.confidence_score > 0.95  # High confidence for known good document
```

**Why it works**:
- Test name describes exact scenario
- Clear arrangement of test data
- Specific assertions with business context
- Performance and quality expectations explicit

## AI Assistant Workflow Patterns

### Session Start Pattern
**When to use**: Beginning any development work in YodaBuffett

**Successful Pattern**:
1. **Read current context**: Always check `.ai-context/current-work.md` first
2. **Load domain context**: Read relevant `backend/domains/[domain]/__domain__.md`
3. **Create session file**: Use template from `.ai-context/sessions/session-template.md`
4. **Verify understanding**: Confirm current state before making changes
5. **Update current-work**: Update active development tracking

**Example Workflow**:
```bash
# 1. Check current work
cat .ai-context/current-work.md

# 2. Load domain context  
cat backend/domains/analytics/__domain__.md

# 3. Create session file
cp .ai-context/sessions/session-template.md .ai-context/sessions/2025-01-12-analytics-correlation-optimization.md

# 4. Begin work with documented context
```

### Documentation Update Pattern
**When to use**: After completing any development work

**Successful Pattern**:
1. **Update session file**: Complete current session documentation
2. **Update domain docs**: Add new components to relevant `__domain__.md`
3. **Update current-work**: Reflect new state in active development tracking  
4. **Run validation**: `python3 tools/ai_docs_validator.py` to check consistency
5. **Log decisions**: Create decision log if architectural choices were made

**Anti-pattern to avoid**:
❌ Finishing development work without updating documentation
❌ Updating code without corresponding documentation updates

## Integration Patterns

### Cross-Domain Communication Pattern
**When to use**: When one domain needs data or functionality from another domain

**Successful Pattern**:
```python
# Use explicit service dependencies with clear interfaces
class AnalyticsCorrelationService:
    def __init__(self, market_data_service: MarketDataService, 
                 document_service: DocumentIntelligenceService):
        self.market_data = market_data_service
        self.documents = document_service
    
    def calculate_company_correlation(self, company_a: str, company_b: str) -> CorrelationResult:
        # Get financial data from market data domain
        financial_a = self.market_data.get_financial_metrics(company_a)
        financial_b = self.market_data.get_financial_metrics(company_b)
        
        # Get document embeddings from document intelligence domain  
        doc_embedding_a = self.documents.get_company_document_embedding(company_a)
        doc_embedding_b = self.documents.get_company_document_embedding(company_b)
        
        # Perform correlation analysis in analytics domain
        return self._calculate_multi_factor_correlation(
            financial_a, financial_b, doc_embedding_a, doc_embedding_b
        )
```

**Why it works**:
- Clear dependency injection
- Domain boundaries respected
- Easy to test and modify individual domains
- AI assistants can understand data flow

## Anti-Patterns to Avoid

### Documentation Anti-Patterns
❌ **Stale Documentation**: Writing code without updating relevant `__domain__.md` files
❌ **Vague Descriptions**: "Optimized the algorithm" instead of "Reduced correlation calculation from 45s to 18s using NumPy vectorization"  
❌ **Missing Context**: Not explaining why decisions were made, only what was done

### Performance Anti-Patterns
❌ **Premature Optimization**: Optimizing before profiling and identifying actual bottlenecks
❌ **Micro-optimization**: Spending time on 1% improvements while ignoring 50% improvements
❌ **Forgetting Caching**: Not using Redis for expensive computations that could be cached

### AI Development Anti-Patterns  
❌ **Generic Naming**: `process()`, `handle()`, `do_work()` instead of descriptive domain-specific names
❌ **Implicit Context**: Assuming AI assistant has context from previous sessions
❌ **No Session Tracking**: Not documenting development session progress and handoff context

## Pattern Evolution

This document should be updated by AI assistants when:
- New successful patterns are discovered through development work
- Existing patterns prove ineffective and need refinement
- Anti-patterns are identified through experience
- Performance optimizations reveal new effective approaches

**Update Template**:
```markdown
### [Pattern Name] Pattern
**When to use**: [Specific scenario]
**Successful Pattern**: [Detailed approach with code examples]
**Why it works**: [Benefits and reasoning]
**Anti-pattern to avoid**: [What not to do]
```

---

*This document captures institutional knowledge for YodaBuffett development. AI assistants should contribute patterns they discover and refine approaches based on experience.*