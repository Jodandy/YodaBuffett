# Swedish Top 10 Companies - Data Collection Status

**Last Updated**: August 28, 2025

## Collection Status Overview

| Company | Market Cap | RSS Feed | Email Alerts | Status | Notes |
|---------|------------|----------|--------------|--------|-------|
| 1. **AstraZeneca** | Mega | ❌ No RSS | 🔍 TODO | ⚠️ Pending | No RSS available - need email signup |
| 2. **Investor AB** | Mega | 🔍 Check | 🔍 TODO | ⚠️ Pending | RSS URL needs verification |
| 3. **Nordea Bank** | Mega | 🔍 Check | 🔍 TODO | ⚠️ Pending | RSS URL needs verification |
| 4. **ABB** | Mega | ❌ No RSS | 🔍 TODO | ⚠️ Pending | Hidden subscribe page found |
| 5. **Atlas Copco** | Large | 🔍 Check | 🔍 TODO | ⚠️ Pending | RSS URL needs verification |
| 6. **Volvo Group** | Large | ✅ Working | ✅ Done | ✅ Active | **Fully operational!** |
| 7. **H&M** | Large | 🔍 Check | 🔍 TODO | ⚠️ Pending | RSS URL needs verification |
| 8. **Ericsson** | Large | 🔍 Check | 🔍 TODO | ⚠️ Pending | RSS URL needs verification |
| 9. **Sandvik** | Large | 🔍 Check | 🔍 TODO | ⚠️ Pending | RSS URL needs verification |
| 10. **Hexagon** | Large | ❓ Not added | 🔍 TODO | 🆕 New | Need to add to system |

## Data Collection Methods by Company

### ✅ Volvo Group (FULLY WORKING)
- **RSS**: `https://www.volvogroup.com/en/news-and-media/.../feed.xml` ✅
- **Email**: Subscribed ✅
- **Collecting**: Financial reports, M&A news, governance changes

### 🔍 AstraZeneca (REQUIRES SETUP)
- **RSS**: None available ❌
- **Email**: Visit https://www.astrazeneca.com/investor-relations/
- **Strategy**: Email primary, web scraping fallback

### 🔍 ABB (REQUIRES SETUP)
- **RSS**: None available ❌
- **Email**: Hidden subscribe page discovered
- **Strategy**: Find subscription URL, web scraping fallback

### 🔍 Others (NEED INVESTIGATION)
- Check each company's investor relations page
- Look for RSS icon, "Subscribe", or email alerts
- Test RSS URLs if found

## Next Steps for You

1. **Find RSS Feeds** (if they exist):
   - Check investor relations pages
   - Look for XML/RSS icons
   - Try common patterns: `/rss`, `/feed`, `/press-releases.xml`

2. **Sign Up for Email Alerts**:
   - Visit each company's IR page
   - Look for "Email Alerts", "Subscribe", "Investor Updates"
   - Use same email for easier management

3. **Document Findings**:
   ```python
   # When you find a working RSS:
   "rss_url": "https://example.com/feed.xml"  # Update in sample_companies.py
   
   # When you find email signup:
   "email_signup": "https://example.com/alerts"  # Note the URL
   ```

## Intelligent Collection Strategy (Future)

Once we have calendar aggregator:
- **Companies with RSS**: Check daily (cheap)
- **Companies without RSS**: Only scrape on earnings dates
- **Email subscribers**: Catch all breaking news
- **Result**: 95% coverage with 90% less effort

## Quick Test Commands

```bash
# Reload companies after updates
python scripts/manage_nordic.py load-companies

# Test specific RSS feed
python scripts/manage_nordic.py test-rss

# Check collection status
python scripts/manage_nordic.py status
```

## Collection Success Metrics

- **Volvo**: 4 documents collected (Q2 report, press release, M&A, governance)
- **Others**: Pending setup
- **Target**: All top 10 operational by end of week