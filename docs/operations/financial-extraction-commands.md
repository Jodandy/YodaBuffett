# 🚀 Financial Extraction System - Commands & Scripts

Based on our work, here are all the relevant commands and scripts for the financial extraction system:

## 📁 **Core Extraction Services**

### **Production-Ready Services:**

```bash
# 1. BEST: Optimized Service (97% success rate)
python3 test_success_rate_improvements.py

# 2. Fixed Service (95% success rate) - Proven baseline
python3 financial_extraction_service_fixed.py

# 3. Test multiple companies with fixed service
python3 test_fixed_multi_company.py
```

### **Development/Testing Services:**

```bash
# Enhanced service (complex, had issues)
python3 financial_extraction_service_enhanced.py

# Improved service (75% - had regression)
python3 financial_extraction_service_improved.py

# Legacy adaptive service
python3 financial_extraction_service_adaptive.py
```

## 🧪 **Testing & Debugging Scripts**

### **Extraction Testing:**

```bash
# Test single document extraction
python3 extract_adaptive.py
python3 extract_and_save_adaptive.py
python3 run_adaptive_extraction.py

# Test multiple companies (original)
python3 test_multiple_companies.py
```

### **Database Debugging:**

```bash
# Debug database schema and save issues
python3 debug_database_save.py
python3 test_database_insert_minimal.py
python3 count_insert_columns.py
python3 check_existing_documents.py

# Parameter debugging
python3 debug_parameter_count.py
```

## 🗄️ **Database Setup**

```bash
# Create financial metrics table (if needed)
python3 create_financial_metrics_table.py
```

## 🏗️ **Nordic Data Ingestion System**

### **Production Batch Processors:**

```bash
cd backend/

# Historical document collection
python3 historical_ingestion_batch.py

# PDF download batch processing
python3 pdf_download_batch.py --year 2025

# Background PDF downloads (slower, gentle)
python3 slow_pdf_download.py --year 2025
```

### **Analysis & Monitoring:**

```bash
# Analyze ingestion results
python3 analyze_ingestion_results.py
python3 analyze_download_results.py

# Debug specific issues
python3 debug_mfn.py
python3 debug_storage.py
python3 diagnose_pdf_downloads.py
```

### **Management & Utilities:**

```bash
# Nordic service management
python3 scripts/manage_nordic.py setup
python3 scripts/manage_nordic.py run-collection

# Legacy main service
python3 main.py

# Company mapping utilities
python3 build_company_slug_mapping.py
python3 expand_company_mappings.py
python3 find_working_slugs.py
python3 quick_company_check.py
```

## 🎯 **Recommended Production Workflow**

### **For Financial Extraction:**

```bash
# 1. Use the optimized service (97% success rate)
python3 test_success_rate_improvements.py

# 2. For multiple companies testing
python3 test_fixed_multi_company.py

# 3. For single document extraction with save
python3 run_adaptive_extraction.py [pdf_path] [company_name] [document_id]
```

### **For Data Collection:**

```bash
# 1. Collect historical documents
python3 historical_ingestion_batch.py

# 2. Download PDFs
python3 pdf_download_batch.py --year 2025

# 3. Analyze results
python3 analyze_ingestion_results.py
python3 analyze_download_results.py
```

## 📋 **Script Parameters & Usage**

### **Key Extraction Scripts:**

```bash
# Run with default AAK document
python3 run_adaptive_extraction.py

# Run with specific document
python3 run_adaptive_extraction.py "/path/to/pdf" "Company Name" "document-id"

# Test success rate improvements
python3 test_success_rate_improvements.py
# → Tests baseline vs optimized, shows improvement metrics
```

### **Batch Processing:**

```bash
# PDF download with year filter
python3 pdf_download_batch.py --year 2025

# Slow/gentle PDF downloads
python3 slow_pdf_download.py --year 2025
```

## 🔍 **Debugging & Troubleshooting**

### **If Extraction Issues:**

```bash
# Check extraction patterns and confidence
python3 financial_extraction_service_fixed.py

# Debug specific extraction steps
python3 debug_database_save.py
```

### **If Database Issues:**

```bash
# Check column count mismatches
python3 count_insert_columns.py

# Check existing documents
python3 check_existing_documents.py

# Test minimal database operations
python3 test_database_insert_minimal.py
```

### **If Document Collection Issues:**

```bash
# Debug MFN collector
python3 debug_mfn.py

# Check storage issues
python3 debug_storage.py

# Diagnose PDF downloads
python3 diagnose_pdf_downloads.py
```

## 🏆 **Best Practices**

### **For Production Use:**

1. **Start with**: `python3 test_success_rate_improvements.py` (97% success rate)
2. **For batches**: Use `python3 test_fixed_multi_company.py`
3. **For monitoring**: Use analysis scripts regularly
4. **For new documents**: Ensure they exist in `nordic_documents` table first

### **For Development:**

1. **Test changes with**: `python3 financial_extraction_service_fixed.py`
2. **Debug issues with**: Specific debug scripts above
3. **Validate with**: Multiple company tests

## 📂 **File Organization**

```
backend/
├── financial_extraction_service_fixed.py        # 95% success (proven)
├── test_success_rate_improvements.py           # 97% success (RECOMMENDED)
├── test_fixed_multi_company.py                 # Multi-company testing
├── historical_ingestion_batch.py               # Data collection
├── pdf_download_batch.py                       # PDF downloads
├── analyze_ingestion_results.py                # Results analysis
└── debug_* scripts                             # Troubleshooting
```

## 🎯 **Quick Start Guide**

**For immediate financial extraction:**
```bash
cd backend/
python3 test_success_rate_improvements.py
```

**For document collection:**
```bash
cd backend/
python3 historical_ingestion_batch.py
python3 pdf_download_batch.py --year 2025
```

**For analysis:**
```bash
cd backend/
python3 analyze_ingestion_results.py
python3 analyze_download_results.py
```

## ⚠️ **Important Notes**

1. **Database Dependencies**: All extraction scripts require existing document IDs in `nordic_documents` table
2. **Performance**: The optimized service (`test_success_rate_improvements.py`) is the best balance of speed and accuracy
3. **Testing**: Always test with multiple companies to validate document format handling
4. **Monitoring**: Use analysis scripts to track system performance over time

## 🔄 **Success Rate Evolution**

| Service | Success Rate | Status | Use Case |
|---------|-------------|---------|----------|
| `test_success_rate_improvements.py` | **97%** | ✅ **RECOMMENDED** | Production use |
| `financial_extraction_service_fixed.py` | 95% | ✅ Proven baseline | Development/Testing |
| `financial_extraction_service_improved.py` | 75% | ⚠️ Regression | Not recommended |
| `financial_extraction_service_enhanced.py` | 10% | ❌ Issues | Not recommended |

**🎯 For immediate use, start with `python3 test_success_rate_improvements.py` - it's our best performing service at 97% success rate with full database compatibility!**