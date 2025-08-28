# MVP2: Swedish Company Reports Web Scraper

## Overview
Automated scraper for downloading Swedish company financial reports from various sources.

## Features
- Download quarterly and annual reports from Swedish companies
- Support for multiple sources (company websites, financial databases)
- Metadata extraction (company, period, report type)
- Integration with MVP1 analysis pipeline

## Target Sources
1. Company investor relations pages
2. Nasdaq Nordic (formerly OMX)
3. Financial supervisory authority databases

## Usage
```bash
python src/scraper.py --company "Volvo" --year 2024
```