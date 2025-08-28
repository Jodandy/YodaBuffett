---
name: Manual Collection Task
about: Document needs to be collected manually (automation failed)
title: 'Manual collection: [COMPANY] [REPORT]'
labels: ['manual-collection', 'operational']
assignees: ''

---

## Company Information
**Company**: 
**Ticker**: 
**Country**: 
**Report Type**: (Q1/Q2/Q3/Annual/Press Release)
**Period**: 

## Why Manual Collection Needed
- [ ] RSS feed failed
- [ ] Website blocking scraper
- [ ] Expected report not found
- [ ] Email parsing failed
- [ ] Other: ________________

## Manual Steps
1. Visit company IR website: 
2. Find and download the document
3. Upload via CLI: `yb upload --company [TICKER] --file [FILE]`
4. Close this issue

## Deadline
- [ ] Urgent (within 24 hours)
- [ ] Normal (within 1 week)

## Notes
Any additional context about why automation failed.