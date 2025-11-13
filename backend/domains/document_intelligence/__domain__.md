# Domain: Document Intelligence

## AI Quick Start (Cold Start Context)
Processes 47K+ Nordic financial documents to extract structured financial data.
Handles PDFs, annual reports, quarterly reports in Swedish, Norwegian, Danish, Finnish.

**Key AI Request Patterns**: "process documents", "extract financial data", "PDF analysis", "document classification"

**Start Files**: `services/pdf_processor.py`, `services/financial_data_extractor.py`, `services/document_classifier.py`

## When to Work Here
- User asks to process financial documents or extract data from PDFs
- Requests for document classification or text extraction
- Issues with the Nordic document corpus (47K+ documents)
- Adding support for new document types or languages

---

## Current Implementation (AI-Maintained)
*Last updated: 2025-11-13 by AI Assistant*

### Business Purpose
Processes 47,931 Nordic financial documents with robust pause/resume capabilities. Extracts text and metadata to enable future financial analytics and AI-driven insights.

### Key Capabilities (IMPLEMENTED)
- **Robust PDF Text Extraction**: pdfplumber + PyPDF2 fallback with content analysis (images, tables, scanned documents)
- **Document Discovery & Cataloging**: Independent state tracking for 47,931 PDFs without processing them
- **Pause/Resume Processing**: Stateful processing controller with batch control and interrupt handling
- **Document Metadata Extraction**: Company, year, document type, priority, region from file structure
- **Priority-Based Processing**: Annual reports (Priority 1), Quarterly (Priority 2), Press releases (Priority 7)
- **Text Chunking**: Prepares documents for future vector embedding with overlap and sentence boundary detection
- **Multi-language Detection**: Nordic language detection (Swedish, Norwegian, Danish, Finnish, English)
- **Multi-Market Architecture**: Regional partitioning ready for Europe, North America, Asia expansion
- **Structured Storage**: PostgreSQL with regional partitioning and comprehensive indexing

### Production Status (READY)
- **109,237 Documents Downloaded**: Complete Swedish market document collection with historical data
- **Integrated State Management**: Uses existing `nordic_documents` table for extraction tracking
- **Database-Driven Processing**: No filesystem scanning, direct database queries for efficient batching
- **Pause/Resume Capability**: Extraction state preserved in database with attempt tracking
- **Smart Prioritization**: Annual reports (Priority 1), Quarterly (Priority 2) processed first
- **Real-Time Monitoring**: Processing status, completion rates, performance metrics

### Planned Capabilities (NOT YET IMPLEMENTED)
- **Financial Data Extraction**: ML-based extraction of revenue, profits, ratios, and key metrics
- **Document Classification**: Automatic categorization beyond file structure
- **Semantic Search**: Vector embeddings for cross-document pattern detection
- **Advanced Analytics**: Cross-company analysis and pattern detection

### Architecture Overview
```
Nordic Documents DB → Extraction Service → PDF Text Extraction → Text Analysis → Extracted Documents DB
        ↓                      ↓                      ↓                  ↓              ↓
NordicExtractionService → DocumentProcessingService → PDFProcessor → TextChunk → PostgreSQLRepository
        ↓                      ↓                      ↓                  ↓              ↓
Extraction Tracking ← Priority Queuing ← Content Analysis ← Language Detection ← Regional Partitioning
```

### Services Implemented
- `NordicExtractionService`: Database-driven extraction management using `nordic_documents` table for state tracking
- `DocumentProcessingService`: Orchestrates PDF text extraction pipeline with dependency injection
- `PDFProcessor`: Handles PDF parsing, text extraction with pdfplumber/PyPDF2 fallback, content analysis
- `PostgreSQLRepository`: Storage for `extracted_documents` and `extracted_document_chunks` with proper indexing

### Services Planned (Not Yet Implemented)
- `TextExtractor`: Advanced text cleaning and structure extraction
- `DocumentClassifier`: ML-based classification into document types and priority levels
- `FinancialDataExtractor`: Extracts structured financial metrics using specialized ML models
- `SemanticIndexer`: Generates vector embeddings for document search and correlation

### Core Models
- `FinancialDocument`: Represents any financial document with metadata and content
- `ExtractionResult`: Structured financial data extracted from documents
- `DocumentMetadata`: Classification, company mapping, and processing status
- `DocumentEmbedding`: Vector representation for semantic search capabilities

### API Endpoints (Planned - Not Yet Implemented)
- `POST /documents/upload`: Upload new financial document for processing
- `GET /documents/{id}/extract`: Retrieve extracted financial data for specific document
- `GET /documents/search`: Semantic search across the document corpus
- `POST /documents/classify`: Classify document type and extract metadata
- `GET /documents/{id}/status`: Check processing status and any errors

### Performance Characteristics (Production Implementation)
- **Document Collection**: 109,237 Nordic PDFs downloaded with smart prioritization
- **Database Queries**: Batch selection via indexed queries (priority + year + type)
- **Single Document Processing**: 2-5 seconds for PDF text extraction with content analysis
- **Batch Processing**: 50-500 documents processed with database-driven pause/resume
- **Text Extraction**: pdfplumber + PyPDF2 fallback for comprehensive coverage
- **Language Detection**: Nordic language detection (Swedish, Norwegian, Danish, Finnish, English)
- **Memory Efficiency**: Database-driven batching prevents memory issues
- **Storage**: `extracted_documents` table with foreign key to `nordic_documents`
- **Priority Processing**: Annual reports (Priority 1), Quarterly (Priority 2) processed first
- **State Persistence**: Extraction state, attempts, duration, confidence tracked in database

### Planned Performance Targets
- **Financial Data Extraction**: <90 seconds per document for complex reports
- **Accuracy**: >95% for standard Nordic financial reports
- **Semantic Search**: <200ms response times

### Dependencies
- **Vector Database**: Stores document embeddings for semantic search capabilities
- **ML Models**: Financial data extraction models and document classification
- **File Storage**: Original PDF storage and processed text caching
- **Market Data Domain**: Company information for validation and enrichment
- **External APIs**: Azure OCR service for damaged document recovery

### Cross-Domain Integration
- **→ Analytics Domain**: Provides structured financial data for correlation analysis
- **→ Market Data Domain**: Validates extracted data against market information  
- **← User Management**: Document access control and processing quotas
- **← Shared Database**: Company mappings and document metadata storage

### Testing Coverage (Current)
- **Manual Testing**: Basic CLI testing with 3 Nordic documents (100% success rate)
- **Integration Testing**: End-to-end PDF processing pipeline validated
- **Unit Tests**: Not yet implemented (planned)
- **Performance Tests**: Not yet implemented (planned)
- **Quality Tests**: Not yet implemented (planned)

### Recent Changes (AI-Generated Log)
- **2025-11-13**: Eliminated redundant state tracking tables, integrated extraction tracking into `nordic_documents`
- **2025-11-13**: Renamed `filings` → `extracted_documents` and `filing_chunks` → `extracted_document_chunks`
- **2025-11-13**: Created `NordicExtractionService` for database-driven extraction management
- **2025-11-13**: Built `cli_nordic_extraction.py` CLI with status, extract, preview, reset commands
- **2025-11-13**: Added extraction tracking columns: status, attempts, duration, confidence, warnings
- **2025-11-13**: Established proper foreign key relationships: nordic_documents → extracted_documents
- **2025-11-13**: Updated repository classes to use new table names and column references
- **2025-11-13**: Migrated from filesystem-based to database-driven batch processing architecture

---

## Common Patterns and Examples

### Nordic Document Extraction Flow (PRODUCTION)
```bash
# Activate virtual environment (required)
source venv/bin/activate

# Navigate to the backend directory (important for relative paths)
cd backend

# Check extraction status and queue
PYTHONPATH=. python3 -m domains.document_intelligence.cli_nordic_extraction status

# Preview next documents to extract
PYTHONPATH=. python3 -m domains.document_intelligence.cli_nordic_extraction preview 20

# Extract priority documents (annual & quarterly reports first)
PYTHONPATH=. python3 -m domains.document_intelligence.cli_nordic_extraction extract 100 --priority=2

# Reset failed extractions for retry
PYTHONPATH=. python3 -m domains.document_intelligence.cli_nordic_extraction reset-failed 2

# Update extraction version and reprocess
PYTHONPATH=. python3 -m domains.document_intelligence.cli_nordic_extraction version v1.1 --reprocess --priority=2
```

### Standard Document Processing Flow (API - Planned)
```python
# Single document processing
document = PDFProcessor().extract_text(pdf_bytes)
classified = DocumentClassifier().classify(document)
extracted_data = FinancialDataExtractor().extract(document)
result = DocumentRepository().save_extraction(extracted_data)
```

### Database-Driven Extraction Pattern (PRODUCTION)
```python
# Database-driven extraction with pause/resume capabilities
service = NordicExtractionService()

# Get next batch based on database state - no filesystem scanning
documents = await service.get_next_extraction_batch(
    batch_size=100, 
    priority_filter=2
)

# Process with automatic state tracking
for doc in documents:
    try:
        result = await processing_service.process_single_document(doc['storage_path'])
        await service.mark_extraction_completed(doc['id'], result)
    except Exception as e:
        await service.mark_extraction_failed(doc['id'], str(e))
```

### Batch Processing Pattern (Planned)
```python
# Process multiple documents efficiently
batch_processor = BatchDocumentProcessor()
results = batch_processor.process_batch(document_paths, parallel_workers=5)
```

---

## AI Maintenance Instructions

### Auto-Update Triggers
Update this file immediately when:
- ✅ New service classes added to this domain
- ✅ API endpoints created, modified, or removed
- ✅ Performance characteristics change significantly (>20% improvement/degradation)
- ✅ Dependencies on other domains change
- ✅ New document types or languages supported
- ✅ Testing coverage changes substantially

### Update Templates

**New Service Added:**
```markdown
- `[ServiceName]`: [Brief description of what it does and key capabilities]
```

**Performance Change:**
```markdown
- [Service/Operation]: <[new_time] for [specific_scenario] (was [old_time])
```

**New API Endpoint:**
```markdown
- `[METHOD] [endpoint_path]`: [Description of what endpoint does]
```

**Recent Change Entry:**
```markdown
- **[DATE]**: [Brief description of change and impact]
```

### AI Update Checklist
Before finalizing work in this domain:
- [ ] Added any new services to "Services in Production" section
- [ ] Updated performance characteristics if they changed
- [ ] Added new API endpoints to the endpoint list
- [ ] Updated dependencies if new ones added
- [ ] Added entry to "Recent Changes" log with date and description
- [ ] Updated testing coverage statistics
- [ ] Verified cross-domain integration documentation is current

### Cross-Reference Maintenance
When modifying services in this domain, check if documentation needs updates in:
- `ARCHITECTURE_MAP.md` (if domain relationships change)
- Analytics domain documentation (if data structure changes)
- Market data domain documentation (if validation requirements change)