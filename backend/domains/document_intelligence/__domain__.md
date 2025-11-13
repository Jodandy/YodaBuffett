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
Extracts text and metadata from Nordic financial documents to enable future analytics. Currently focused on reliable PDF processing and structured data storage.

### Key Capabilities (IMPLEMENTED)
- **PDF Text Extraction**: Robust extraction using pdfplumber + PyPDF2 fallback
- **Document Metadata Extraction**: Company, year, document type from file structure  
- **Text Chunking**: Prepares documents for future vector embedding
- **Multi-language Detection**: Basic Nordic language detection (Swedish, Norwegian, English)
- **Structured Storage**: PostgreSQL storage with proper indexing

### Planned Capabilities (NOT YET IMPLEMENTED)
- **Financial Data Extraction**: ML-based extraction of revenue, profits, ratios, and key metrics
- **Document Classification**: Automatic categorization beyond file structure
- **Semantic Search**: Vector embeddings for cross-document pattern detection
- **Advanced Analytics**: Cross-company analysis and pattern detection

### Architecture Overview
```
PDF Upload → Text Extraction → Classification → Financial Analysis → Structured Data → Storage
     ↓           ↓               ↓              ↓                 ↓              ↓
pdf_processor → text_extractor → classifier → financial_analyzer → models → repositories
```

### Services Implemented
- `PDFProcessor`: Handles PDF parsing, text extraction with pdfplumber/PyPDF2 fallback
- `DocumentProcessingService`: Orchestrates document processing pipeline with dependency injection

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

### Performance Characteristics (Current Implementation)
- **Single Document Processing**: ~1-2 seconds for PDF text extraction
- **Batch Processing**: 3 documents processed successfully in ~1.3 seconds (100% success rate)
- **Text Extraction**: Working with both pdfplumber and PyPDF2 fallback
- **Language Detection**: Basic Nordic language detection working
- **Memory Usage**: Minimal for basic text extraction
- **Supported File Sizes**: Tested with typical financial report PDFs

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
- **2025-11-13**: Restructured domain with hexagonal architecture following HARD principles
- **2025-11-13**: Implemented PDFProcessor and DocumentProcessingService with dependency injection
- **2025-11-13**: Added PostgreSQL repositories with proper ports/adapters pattern
- **2025-11-13**: Updated documentation to reflect actual implementation vs planned features
- **2025-01-12**: Initial domain structure created with comprehensive documentation

---

## Common Patterns and Examples

### Standard Document Processing Flow
```python
# Upload and process new document
document = PDFProcessor().extract_text(pdf_bytes)
classified = DocumentClassifier().classify(document)
extracted_data = FinancialDataExtractor().extract(document)
result = DocumentRepository().save_extraction(extracted_data)
```

### Semantic Search Pattern
```python
# Search for documents discussing specific topics
embeddings = SemanticIndexer().generate_embeddings(query_text)
similar_docs = VectorDatabase().similarity_search(embeddings, top_k=50)
```

### Batch Processing Pattern
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