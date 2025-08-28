# YodaBuffett Storage Architecture

## Overview
Enterprise-grade scalable storage structure designed to handle thousands of Nordic companies across multiple years and document types.

## Directory Structure

### Companies Data
```
data/companies/{country}/{first_letter}/{company}/{year}/{document_type}/
```

### Real Examples
```
data/companies/SE/V/volvo/2025/Q2/q2-2025-quarterly-report.pdf
data/companies/SE/V/volvo/2025/press_release/2025-07-03-press-release-report.pdf
data/companies/SE/A/atlas-copco/2025/Q2/q2-2025-quarterly-report.pdf
data/companies/SE/E/ericsson/2025/Q3/q3-2025-quarterly-report.pdf
data/companies/SE/H/h-m/2025/annual/annual-2025-report.pdf
```

### Future Expansion
```
data/
├── companies/           # All company-specific data
│   ├── SE/             # Sweden
│   │   ├── A/atlas-copco/
│   │   │   ├── 2025/Q2/        # Financial reports
│   │   │   ├── logos/          # Company branding
│   │   │   └── filings/        # Legal documents
│   │   └── V/volvo/
│   ├── NO/             # Norway
│   ├── DK/             # Denmark
│   └── FI/             # Finland
├── markets/            # Market-wide data
│   ├── indices/
│   ├── sector-reports/
│   └── macro-news/
└── research/           # Analysis outputs
    └── company-analysis/
```

## Benefits

### Scalability
- **Alphabetical Bucketing**: ~26 folders per country instead of 1000s
- **Handles Growth**: Designed for thousands of companies
- **File System Optimized**: Prevents directory bloat

### Organization
- **Company-Centric**: All company data in one tree
- **Year-Based**: Easy historical analysis
- **Type-Separated**: Clean document type separation

### Performance
- **Fast Lookups**: Alphabetical navigation
- **Balanced Distribution**: Companies spread across A-Z
- **Future-Proof**: Ready for multi-company analysis

## Filename Standards

### Format
```
{report_period}-{document_type}-report.pdf
```

### Examples
- `q2-2025-quarterly-report.pdf`
- `2025-07-03-press-release-report.pdf`
- `annual-2024-report.pdf`

## Implementation

The storage path is generated in:
- `backend/nordic_ingestion/storage/document_downloader.py`
- Method: `_create_storage_path()`
- Filename: `_generate_filename()`

## Production Status

✅ **Live System**: Successfully storing real Swedish financial data  
✅ **Tested**: Volvo Group Q2 2025 reports downloaded and organized  
✅ **Scalable**: Ready for thousands of Nordic companies  
✅ **Enterprise-Ready**: Production deployment complete  

Last Updated: August 28, 2025