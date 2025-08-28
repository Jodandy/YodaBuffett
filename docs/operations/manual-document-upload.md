# Manual Document Upload Process

## When Manual Collection is Needed

**Triggers:**
1. **Expected report missing** - Calendar says Volvo Q1 due today, but automated collection failed
2. **Broken automation** - RSS/scraping stopped working, need backup until fixed
3. **One-off documents** - Special reports, merger documents, etc.

## Manual Upload Workflow

### Step 1: Human Downloads Document
```
1. Go to company IR page
2. Find the missing report
3. Download PDF to local machine
4. Note: filename, report type, period
```

### Step 2: Upload via API
```bash
# Upload via command line tool
./scripts/manual-upload.py \
  --company "Volvo Group" \
  --file "volvo-q1-2025.pdf" \
  --type "Q1" \
  --period "2025" \
  --source-url "https://volvogroup.com/..."

# OR via web interface
POST /api/v1/nordic/documents/manual
Content-Type: multipart/form-data

{
  "file": <PDF binary>,
  "company_id": "uuid",
  "document_type": "Q1", 
  "report_period": "2025",
  "source_url": "https://...",
  "uploaded_by": "human_operator",
  "notes": "Automated collection failed on 2025-01-28"
}
```

### Step 3: System Processing
```python
# Same pipeline as automated documents
def process_manual_upload(file, metadata):
    # 1. Store raw file
    s3_path = store_raw_document(file, metadata)
    
    # 2. Extract text & create embeddings  
    extracted = extract_pdf_content(s3_path)
    embeddings = create_embeddings(extracted)
    
    # 3. Update database
    db.documents.create({
        'company_id': metadata.company_id,
        'storage_path': s3_path,
        'source_method': 'manual_upload',
        'processing_status': 'completed'
    })
    
    # 4. Notify research service
    notify_research_service(document_id)
    
    # 5. Close related tickets
    close_manual_collection_ticket(company_id, report_period)
```

## Manual Upload Tools

### Command Line Interface
```bash
# Install CLI tool
pip install yodabuffett-cli

# Configure credentials
yb config set-endpoint https://api.yodabuffett.com
yb config set-token your-api-token

# Upload document
yb upload \
  --company-ticker VOLV-B \
  --type Q1 \
  --year 2025 \
  --file ~/Downloads/volvo-q1-2025.pdf \
  --url "https://source-url.com"

# List pending manual tasks
yb tasks list --status manual_needed

# Mark task as completed
yb tasks complete --task-id abc123
```

### Web Interface
```
Admin Dashboard → Manual Upload
1. Select company from dropdown
2. Choose report type (Q1/Q2/Q3/Annual)
3. Select period (2025)
4. Drag & drop PDF file
5. Enter source URL
6. Add notes about why manual was needed
7. Submit → Automatic processing starts
```

## Quality Assurance

### Automated Validation
```python
def validate_manual_upload(document):
    checks = []
    
    # File format check
    if not document.filename.endswith('.pdf'):
        checks.append('FAIL: Not a PDF file')
    
    # Company name verification
    extracted_text = extract_text(document)
    if company.name.lower() not in extracted_text.lower():
        checks.append('WARN: Company name not found in document')
    
    # Report period check
    if document.report_period not in extracted_text:
        checks.append('WARN: Report period not clearly identified')
    
    # Duplicate check
    existing = find_similar_documents(document)
    if existing:
        checks.append('FAIL: Possible duplicate document')
    
    return checks
```

### Human Review Queue
```
Admin Dashboard → Review Queue
- Documents flagged by validation
- Human confirms: legitimate vs duplicate vs wrong file
- Approve → Continue processing
- Reject → Delete and re-upload
```

## Ticket Integration

### GitHub Issues Integration
```python
# When manual collection is needed
def create_manual_collection_ticket(company, report_period, reason):
    issue = github.create_issue(
        title=f"Manual collection needed: {company} {report_period}",
        body=f"""
        **Company**: {company}
        **Report**: {report_period}  
        **Reason**: {reason}
        **Expected Date**: {expected_date}
        
        **Manual Steps**:
        1. Visit {company.ir_website}
        2. Download {report_period} report
        3. Upload via: `yb upload --company-ticker {company.ticker} --type {report_type} --year {year} --file report.pdf`
        4. Close this issue
        
        **Deadline**: {deadline}
        **Priority**: {priority}
        """,
        labels=['manual-collection', f'country-{company.country}', f'priority-{priority}']
    )
    
    # Send notification
    send_slack_notification(f"Manual collection needed for {company} {report_period}")

# When document is uploaded
def close_manual_collection_ticket(company, report_period):
    # Find related GitHub issue
    issue = find_github_issue(company, report_period)
    if issue:
        github.close_issue(
            issue.id,
            comment=f"Document uploaded successfully. Processing complete."
        )
```

## Monitoring & Metrics

### Manual Collection Metrics
```python
# Track manual intervention rate
manual_rate = manual_uploads_count / total_documents_count

# Company-specific automation success
company_automation_rate = {
    'Volvo': 95%,    # Very reliable  
    'H&M': 60%,      # Frequent manual needed
    'Ericsson': 80%  # Moderate reliability
}

# Alert if manual rate too high
if manual_rate > 0.2:  # >20% manual
    alert("High manual collection rate - check automation systems")
```

### Dashboard Views
```
Operator Dashboard:
├── Pending Manual Tasks (urgent: 3, normal: 7)
├── Recently Uploaded (last 24h: 5 documents)
├── Processing Status (extracting: 2, indexing: 1, complete: 15)
├── Automation Health (85% success rate this week)
└── Company Coverage (98% of expected reports collected)
```

## Security & Compliance

### Access Control
- Only authorized operators can upload documents
- All uploads logged with user ID and timestamp
- File integrity verified (checksums)
- Audit trail maintained

### Data Validation
- Virus scanning on all uploaded files
- Content validation (is this actually a financial report?)
- Metadata consistency checks
- Duplicate detection and prevention