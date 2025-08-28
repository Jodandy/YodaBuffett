# YodaBuffett MVP 1: AI-Powered Report Analysis

## Overview
Proof-of-concept for AI-powered analysis of SEC filings and financial documents.

## Quick Start

### 1. Setup Environment
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows

# Install dependencies
pip install -r requirements.txt

# Copy environment template
cp .env.example .env
# Edit .env with your OpenAI API key
```

### 2. Test Document Processing
```bash
# Run basic test (creates sample HTML file)
python test_processor.py

# Add your own PDF/HTML files to data/ directory and test again
```

### 3. Process Your First Document
1. Download a 10-K filing PDF from SEC EDGAR
2. Place it in the `data/` directory
3. Run `python test_processor.py`
4. Review the extracted sections and content

## Project Structure
```
mvp1-report-analysis/
├── src/
│   └── document_processor.py    # Core document processing logic
├── data/                        # Test documents (not committed)
├── tests/                       # Unit tests
├── docs/                        # Documentation
├── test_processor.py           # Quick test script
├── requirements.txt            # Python dependencies
└── .env.example               # Environment template
```

## Features

### Document Processing
- ✅ PDF parsing with PyPDF2
- ✅ HTML parsing with BeautifulSoup
- ✅ Section identification (Business, Risk Factors, MD&A, etc.)
- ✅ Metadata extraction (company name, filing type, date)
- ✅ Clean text extraction

### Coming Next (Week 2-5)
- [ ] LLM integration for analysis
- [ ] Web interface for file upload
- [ ] Multiple analysis types
- [ ] Confidence scoring
- [ ] Export capabilities

## Testing Strategy

### Unit Tests
```bash
python -m pytest tests/
```

### Manual Testing
1. Test with various SEC filing formats
2. Compare section identification accuracy
3. Validate text extraction quality

### Validation Documents
- Large company 10-K (e.g., Apple, Microsoft)
- Small company 10-Q
- Different industries (tech, healthcare, finance)

## Configuration

### Environment Variables (.env)
- `OPENAI_API_KEY`: Your OpenAI API key
- `DEBUG`: Enable debug logging
- `MAX_FILE_SIZE_MB`: Maximum upload size
- `UPLOAD_DIR`: Directory for uploaded files

## Known Limitations

### Current MVP Scope
- Manual file upload only
- Basic section identification
- No database storage
- Simple text extraction

### Future Improvements
- Better section detection with ML
- Table and figure extraction
- Multi-document analysis
- Real-time processing

## Success Metrics

### Week 1 Goals
- ✅ Basic PDF/HTML parsing works
- ✅ Section identification > 70% accuracy
- ✅ Clean text extraction
- ✅ Metadata extraction

### Next Week Targets
- LLM analysis integration
- Web interface completion
- Error handling and validation

## Getting Help

### Common Issues
- **PDF parsing fails**: Try with different PDF (some are scanned images)
- **No sections found**: Document may have unusual formatting
- **Import errors**: Check all dependencies installed

### Debug Tips
- Set `DEBUG=true` in .env
- Check logs for detailed error messages
- Test with sample HTML file first

## Next Steps
1. Validate document processing with your target documents
2. Move to Week 2: LLM integration
3. Build web interface in Week 4
4. Complete testing in Week 5