# YodaBuffett Glossary

## Technical Terms

### Architecture
- **Modular Monolith**: Single deployable application organized into domain modules for better organization while maintaining operational simplicity
- **Domain**: Logical grouping of related business functionality (e.g., document_intelligence, analytics)
- **Hexagonal Architecture**: Design pattern isolating business logic from infrastructure dependencies

### Document Processing
- **Document Sections**: Intelligent parsing of financial documents into semantic sections (balance sheet, income statement, etc.)
- **Embeddings**: Vector representations of text that capture semantic meaning for similarity search
- **Section Chunking**: Process of parsing documents into meaningful financial sections vs mechanical text chunks
- **CID Artifacts**: Text corruption in PDFs that appears as random characters, filtered out during processing

### Embeddings & AI
- **Vector Embeddings**: Numerical representations of text in high-dimensional space that capture semantic meaning
- **Cosine Similarity**: Measure of similarity between two vectors, used to find semantically similar content
- **Temporal Anomaly Detection**: Finding unusual patterns in document communication over time
- **Multi-Label Classification**: Assigning multiple themes/categories to a single document section

### Financial Data
- **Nordic Markets**: Scandinavian financial markets (Sweden, Norway, Denmark, Finland)
- **MFN.se**: Swedish financial data source used for document collection
- **Document Types**: Annual reports, quarterly reports, press releases, governance documents
- **Section Types**: Balance sheet, income statement, cash flow, management discussion, risk factors

### Market Data
- **Temporal Patterns**: How company communications change over time
- **Anomaly Score**: Numerical measure of how unusual a document is compared to historical patterns
- **Winner Patterns**: Communication characteristics common among top-performing stocks
- **Performance Tiers**: Classifications like top_1pct, top_5pct based on stock performance

## Business Terms

### Investment Analysis
- **Alpha**: Excess return generated above market benchmark
- **Systematic Alpha**: Repeatable, data-driven investment edge vs intuitive stock picking
- **Fat Pitch**: High-confidence investment opportunity with asymmetric risk/reward
- **Multi-Dimensional Anomalies**: Pattern changes across multiple communication themes simultaneously

### Nordic Financial Reporting
- **IFRS**: International Financial Reporting Standards used in Nordic countries
- **Calendar Events**: Earnings releases, AGMs, dividend announcements that drive document publication
- **Event-Driven Collection**: Targeting document collection around scheduled financial events

### Performance Classification
- **Timeframe Labels**: Performance measurements over different periods (1mo, 6mo, 12mo, 24mo)
- **Quality Profiles**: Risk-adjusted performance characteristics (high_sharpe_winner, low_drawdown_winner)
- **Pattern Types**: Communication patterns that precede performance (transformation_signaler, confidence_builder)

## Operational Terms

### Data Pipeline
- **Historical Ingestion**: Batch collection of past financial documents
- **Daily Event Worker**: Automated system for collecting new documents based on calendar events
- **Retry System**: Intelligent system for handling failed document collection attempts
- **Document Discovery**: Process of cataloging available PDF files before text extraction

### System Components
- **Workers**: Specialized processes for specific tasks (document ingestion, calendar monitoring)
- **Schedulers**: Time-based automation for running workers at specific intervals
- **Health Monitoring**: System status checking and alerting
- **Batch Processing**: Processing large numbers of documents in controlled groups

### Development
- **CLI Tools**: Command-line interfaces for system administration and testing
- **Docker Containers**: Containerized deployment units for production operations
- **Domain Modules**: Self-contained business logic areas within the modular monolith
- **Vector Databases**: Specialized databases optimized for similarity search on embeddings

## Acronyms & Abbreviations

- **AI**: Artificial Intelligence
- **API**: Application Programming Interface
- **CID**: Character Identifier (PDF corruption artifacts)
- **CLI**: Command Line Interface  
- **CTA**: Call to Action
- **ESG**: Environmental, Social, Governance
- **FCF**: Free Cash Flow
- **IFRS**: International Financial Reporting Standards
- **LLM**: Large Language Model
- **MFN**: MFN.se (Swedish financial data source)
- **ML**: Machine Learning
- **MVP**: Minimum Viable Product
- **NLP**: Natural Language Processing
- **PDF**: Portable Document Format
- **RAG**: Retrieval-Augmented Generation
- **REST**: Representational State Transfer
- **RSS**: Really Simple Syndication
- **UUID**: Universally Unique Identifier

## Common File Extensions

- **`.md`**: Markdown documentation files
- **`.py`**: Python source code files
- **`.sql`**: Database query/schema files
- **`.json`**: JavaScript Object Notation data files
- **`.pdf`**: Portable Document Format files
- **`.yml`**: YAML configuration files

## Database Tables (Key)

- **`extracted_documents`**: Master table of processed PDF documents
- **`document_sections`**: Intelligent sections parsed from documents
- **`section_embeddings`**: Vector embeddings of document sections
- **`document_embeddings`**: Vector embeddings of entire documents
- **`company_performance_labels`**: Multi-dimensional performance classifications for ML
- **`nordic_companies`**: Master list of Nordic companies being tracked

This glossary is maintained alongside code changes to ensure accuracy.