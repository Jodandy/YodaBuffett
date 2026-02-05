# Domain: Document Intelligence

## AI Quick Start (Cold Start Context)
**PROOF-OF-CONCEPT TEMPORAL ANOMALY DETECTION SYSTEM** 🧪
Processes 106K+ Nordic financial documents with AI-powered temporal anomaly detection.
Successfully validated concept by detecting real financial events: AAK balance sheet changes, AcadeMedia regulatory impacts, AddLife growth inflections.

**Key AI Request Patterns**: "temporal anomaly detection", "embedding generation", "financial signal detection", "document intelligence"

**Start Files**: `cli_multi_embeddings.py`, `services/section_chunking_service.py`, `test_temporal_patterns.py`

## When to Work Here
- **Temporal anomaly detection**: Detecting changes in company communication patterns
- **Embedding generation**: Local model embeddings for financial sections  
- **Financial signal detection**: Early warning systems for market events
- User asks to process financial documents or extract data from PDFs
- Issues with the Nordic document corpus (106K+ documents)
- Section-based intelligent document parsing
- Adding support for new document types or languages

---

## Current Implementation (AI-Maintained)
*Last updated: 2025-11-27 by AI Assistant*

### Business Purpose - PROOF-OF-CONCEPT VALIDATED
**BREAKTHROUGH**: Temporal anomaly detection concept successfully validated by identifying real financial events:
- AAK 2020-2021: Balance sheet anomaly → Major asset/debt spike
- AcadeMedia 2017-2018: Risk factor changes → Swedish schooling law changes  
- AddLife 2018-2019: Income statement anomaly → 40% revenue growth

Processes 106,683+ Nordic financial documents with intelligent section parsing and local embedding generation for company-specific temporal pattern analysis.

### Key Capabilities (WORK IN PROGRESS)
🧪 **TEMPORAL ANOMALY DETECTION (PROOF-OF-CONCEPT)** 
- **Concept Validated**: Real financial event detection with verified outcomes
- **Company-Specific Baselines**: Individual temporal patterns for each company  
- **Automated Anomaly Scoring**: Similarity thresholds with significance detection
- **Local Embeddings**: FREE sentence-transformers model (11,000+ embeddings)

📊 **INTELLIGENT SECTION PARSING**
- **Financial Section Chunking**: Smart parsing of balance sheets, income statements, risk factors
- **CID Artifact Filtering**: Automatic quality control for PDF extraction issues
- **Section Confidence Scoring**: Quality metrics for parsed content
- **183,884 Sections**: Across 19,425 documents with average 9.5 sections per document

🔧 **ROBUST EXTRACTION PIPELINE**
- **106,683 Extracted Documents**: Complete Nordic financial document corpus
- **Multi-Provider Embeddings**: OpenAI, Cohere, Local model support
- **Pause/Resume Processing**: Database-driven state management
- **Priority-Based Processing**: Annual reports (Priority 1), Quarterly (Priority 2)
- **Multi-language Support**: Nordic language detection (Swedish, Norwegian, Danish, Finnish)
- **Quality Validation**: Comprehensive embedding quality testing and debugging tools

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

### Services Implemented (PRODUCTION)
🚨 **TEMPORAL ANOMALY DETECTION**
- `test_temporal_patterns.py`: Core temporal anomaly detection with validated results
- `test_embedding_quality.py`: Embedding quality validation and similarity testing
- `test_embedding_search.py`: Semantic search capabilities across financial sections

📊 **SECTION PARSING & EMBEDDINGS** 
- `SectionChunkingService`: Intelligent financial section parsing with CID filtering
- `MultiProviderEmbeddingService`: Local/OpenAI/Cohere embedding generation  
- `FinancialSectionParser`: Rule-based Nordic financial document section detection
- `LocalEmbeddingProvider`: FREE sentence-transformers integration (production-tested)

🔧 **EXTRACTION PIPELINE**
- `NordicExtractionService`: Database-driven extraction management  
- `DocumentProcessingService`: PDF text extraction orchestration
- `PDFProcessor`: pdfplumber + PyPDF2 fallback with content analysis
- `PostgreSQLRepository`: Storage with pgvector support for embeddings

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
- **→ Market Data Domain**: Enriches documents with company metadata and market context

### Testing Coverage (Current)
- **Manual Testing**: Basic CLI testing with 3 Nordic documents (100% success rate)
- **Integration Testing**: End-to-end PDF processing pipeline validated
- **Unit Tests**: Not yet implemented (planned)
- **Performance Tests**: Not yet implemented (planned)
- **Quality Tests**: Not yet implemented (planned)

### Recent Changes (AI-Generated Log)
- **2025-11-27**: Validated temporal anomaly detection with real financial events
- **2025-11-27**: Detected AAK balance sheet changes, AcadeMedia regulatory impacts, AddLife growth inflections
- **2025-11-27**: Implemented production-ready local embedding system (FREE, sentence-transformers)
- **2025-11-27**: Generated 11,000+ section embeddings with quality validation tools
- **2025-11-27**: Created comprehensive temporal pattern analysis and anomaly detection system
- **2025-11-27**: Built section chunking service with CID artifact filtering and quality scoring
- **2025-11-27**: Established multi-provider embedding architecture (local/OpenAI/Cohere)
- **2025-11-27**: Processed 183,884 financial sections across 19,425 documents
- **2025-11-27**: Created debugging and monitoring tools for embedding quality control
- **2025-11-13**: Implemented vector embedding pipeline with OpenAI text-embedding-3-small integration
- **2025-11-13**: Generated 20 real embeddings with PostgreSQL + pgvector storage, validated production pipeline
- **2025-11-13**: Eliminated redundant state tracking tables, integrated extraction tracking into `nordic_documents`
- **2025-11-13**: Renamed `filings` → `extracted_documents` and `filing_chunks` → `extracted_document_chunks`
- **2025-11-13**: Created `NordicExtractionService` for database-driven extraction management
- **2025-11-13**: Built `cli_nordic_extraction.py` CLI with status, extract, preview, reset commands
- **2025-11-13**: Added extraction tracking columns: status, attempts, duration, confidence, warnings
- **2025-11-13**: Established proper foreign key relationships: nordic_documents → extracted_documents
- **2025-11-13**: Updated repository classes to use new table names and column references
- **2025-11-13**: Migrated from filesystem-based to database-driven batch processing architecture
- **[Future updates will be added here by AI assistants]**

---

## Common Patterns and Examples

### TEMPORAL ANOMALY DETECTION WORKFLOW (PROOF-OF-CONCEPT)
```bash
# Experimental pipeline - concept validation
cd /Users/jdandemar/Documents/YodaBuffett/backend

# 1. Check extraction status (106K+ documents ready)
python domains/document_intelligence/cli_nordic_extraction.py status

# 2. Generate intelligent financial sections
python domains/document_intelligence/cli_section_chunking.py process 1000

# 3. Create local embeddings (FREE - production tested)
python domains/document_intelligence/cli_multi_embeddings.py local setup
python domains/document_intelligence/cli_multi_embeddings.py local process 10000

# 4. Run temporal anomaly detection (CORE EDGE)
python test_temporal_patterns.py
python test_embedding_quality.py
python test_embedding_search.py
```

### EMBEDDING QUALITY CONTROL (PRODUCTION)
```bash
# Validate embedding quality and debug issues
python test_embedding_quality.py
python debug_embeddings.py
python count_dummy_embeddings.py
python clean_dummy_embeddings.py

# Investigate specific anomalies
python investigate_embeddings.py
```

### Nordic Document Extraction Flow (LEGACY REFERENCE)
```bash
# Legacy extraction commands (still functional)
cd backend
PYTHONPATH=. python3 -m domains.document_intelligence.cli_nordic_extraction status
PYTHONPATH=. python3 -m domains.document_intelligence.cli_nordic_extraction extract 100 --priority=2
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