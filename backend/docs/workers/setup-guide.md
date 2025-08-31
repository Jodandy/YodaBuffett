# YodaBuffett Worker Setup Guide

**Complete setup guide for the event-driven Swedish financial data monitoring system**

## Overview

This guide will walk you through setting up the production-ready event-driven workers that keep your Swedish financial data current by intelligently targeting companies based on calendar events.

## Prerequisites

### Required Software
- **Docker & Docker Compose** (latest stable versions)
- **Python 3.11+** (for local development)
- **Git** (for version control)
- **PostgreSQL 15+** (if running database separately)

### System Requirements
- **RAM**: 4GB minimum, 8GB recommended
- **CPU**: 2+ cores recommended  
- **Storage**: 10GB minimum for data and logs
- **Network**: Stable internet connection for web scraping

## Quick Start (Docker)

### 1. Clone and Navigate
```bash
cd /path/to/YodaBuffett/backend
```

### 2. Configure Environment
```bash
# Copy configuration template
cp docker/.env.example docker/.env

# Edit configuration
nano docker/.env
```

**Minimal required changes in `.env`:**
```bash
# Set a secure database password
DB_PASSWORD=your_secure_password_here

# Update user agent with your info
USER_AGENT=YodaBuffett-Worker/1.0 (+https://github.com/yourusername/yodabuffett)
```

### 3. Start Services
```bash
# Start PostgreSQL and workers
python scripts/manage_workers.py docker --action start

# Check status
python scripts/manage_workers.py docker --action status
```

### 4. Verify Setup
```bash
# Run health check
python scripts/manage_workers.py health-check

# Preview today's scheduled targets
python scripts/manage_workers.py daily-worker --dry-run
```

### 5. Test Run
```bash
# Run daily worker for today
python scripts/manage_workers.py daily-worker

# Check results
python scripts/manage_workers.py analyze --days 1
```

## Detailed Configuration

### Environment Variables (.env)

#### Database Configuration
```bash
# Database connection
DB_HOST=postgres                    # Use 'localhost' if running PostgreSQL separately
DB_PORT=5432
DB_NAME=yodabuffett
DB_USER=postgres
DB_PASSWORD=your_secure_password    # REQUIRED - set a strong password

# Database URL is auto-generated from above settings
```

#### Worker Behavior
```bash
# Worker execution mode
WORKER_MODE=production              # Options: production, development, test
LOG_LEVEL=INFO                      # Options: DEBUG, INFO, WARNING, ERROR

# Event scheduling
LOOK_AHEAD_DAYS=3                   # How far ahead to look for events
LOOK_BACK_DAYS=1                    # How far back to look for recent events
MAX_DAILY_TARGETS=100               # Maximum companies per daily run

# Weekly surprise scanner
WEEKLY_SAMPLE_SIZE=50               # Companies to sample weekly
```

#### Scraping Configuration
```bash
# Rate limiting (be respectful to MFN.se)
RATE_DELAY=2.0                      # Seconds between requests (minimum 2.0)
SCRAPING_TIMEOUT=30                 # Request timeout in seconds
SCRAPING_MAX_RETRIES=3              # Max retry attempts

# Identification
USER_AGENT=YodaBuffett-Worker/1.0 (+https://github.com/yourusername/yodabuffett)
```

#### Operational Settings
```bash
# Health monitoring
HEALTH_CHECK_PORT=8080              # Port for health check endpoint

# Data storage
DATA_VOLUME_PATH=/app/data          # Where worker results are stored
LOG_FILE_PATH=/app/logs/worker.log  # Log file location (optional)

# Scheduling
RUN_INTERVAL_HOURS=24               # How often daily worker runs
```

## Manual Setup (Without Docker)

### 1. Database Setup
```bash
# Install PostgreSQL
sudo apt install postgresql postgresql-contrib  # Ubuntu/Debian
brew install postgresql                         # macOS

# Create database
sudo -u postgres createdb yodabuffett
sudo -u postgres psql -c "CREATE USER yoda WITH PASSWORD 'your_password';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE yodabuffett TO yoda;"

# Run migrations
cd backend
python -m alembic upgrade head
```

### 2. Python Environment
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/macOS
# or
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
```

### 3. Configuration
```bash
# Set environment variables
export WORKER_MODE=production
export DB_HOST=localhost
export DB_PASSWORD=your_password
export LOG_LEVEL=INFO
# ... (other variables from .env.example)
```

### 4. Run Workers
```bash
# Daily worker
python scripts/manage_workers.py daily-worker

# Weekly scanner
python scripts/manage_workers.py weekly-scanner --sample-size 30
```

## Operations Guide

### Daily Operations

#### Morning Routine (Automated)
The daily worker should run automatically via Docker restart policies or cron jobs:

```bash
# Check if daily worker ran successfully
python scripts/manage_workers.py analyze --days 1

# View recent logs
python scripts/manage_workers.py docker --action logs --service daily-worker --tail 50
```

#### Manual Daily Run
```bash
# Preview targets for today
python scripts/manage_workers.py daily-worker --dry-run

# Run daily worker
python scripts/manage_workers.py daily-worker

# Check results
python scripts/manage_workers.py analyze --days 1
```

### Weekly Operations

#### Saturday: Surprise Scanner
```bash
# Preview surprise targets
python scripts/manage_workers.py weekly-scanner --dry-run --sample-size 50

# Run surprise scanner
python scripts/manage_workers.py weekly-scanner --sample-size 50

# Analyze surprise discoveries
python scripts/manage_workers.py analyze --days 7
```

#### Sunday: Schedule Preview  
```bash
# Preview next week's schedule
python scripts/manage_workers.py schedule --days 7

# Check system health
python scripts/manage_workers.py health-check
```

### Monitoring Commands

#### System Health
```bash
# Comprehensive health check
python scripts/manage_workers.py health-check

# Docker container status
python scripts/manage_workers.py docker --action status

# View service logs
python scripts/manage_workers.py docker --action logs --service daily-worker --follow
```

#### Performance Analysis
```bash
# Analyze last 7 days
python scripts/manage_workers.py analyze --days 7

# Analyze last 30 days
python scripts/manage_workers.py analyze --days 30

# Analyze specific results file
python scripts/manage_workers.py analyze --file /app/data/daily_worker_20250830_120000.json
```

## Scheduling & Automation

### Docker Automatic Restart
The docker-compose configuration includes automatic restart policies:

```yaml
restart: unless-stopped  # Daily worker restarts automatically
```

### Cron Job Setup (Alternative)
If you prefer cron-based scheduling:

```bash
# Edit crontab
crontab -e

# Add daily worker job (6 AM every day)
0 6 * * * cd /path/to/YodaBuffett/backend && python scripts/manage_workers.py daily-worker

# Add weekly scanner (Saturday 8 AM)
0 8 * * 6 cd /path/to/YodaBuffett/backend && python scripts/manage_workers.py weekly-scanner
```

### Systemd Service (Linux)
Create systemd services for production deployment:

```ini
# /etc/systemd/system/yodabuffett-daily.service
[Unit]
Description=YodaBuffett Daily Event Worker
After=postgresql.service

[Service]
Type=simple
User=yoda
WorkingDirectory=/opt/yodabuffett/backend
ExecStart=/opt/yodabuffett/backend/venv/bin/python scripts/manage_workers.py daily-worker
Restart=on-failure
RestartSec=300

[Install]
WantedBy=multi-user.target
```

## Troubleshooting

### Common Issues

#### "No daily targets found"
**Cause**: No calendar events scheduled for target date
**Solution**:
```bash
# Check calendar events in database
python scripts/manage_workers.py schedule --days 7

# Verify calendar data exists
python -c "
from workers.event_scheduler import EventScheduler
import asyncio
async def check():
    scheduler = EventScheduler()
    targets = await scheduler.get_daily_scrape_targets()
    print(f'Found {len(targets)} targets')
asyncio.run(check())
"
```

#### Database Connection Errors
**Cause**: PostgreSQL not running or wrong credentials
**Solution**:
```bash
# Check PostgreSQL status
python scripts/manage_workers.py health-check

# Test database connection
docker exec -it yodabuffett-postgres psql -U postgres -d yodabuffett -c "SELECT 1;"

# Check environment variables
docker exec -it yodabuffett-daily-worker env | grep DB_
```

#### Docker Containers Not Starting
**Cause**: Configuration errors or resource constraints
**Solution**:
```bash
# Check container logs
python scripts/manage_workers.py docker --action logs --service daily-worker

# Check system resources
docker stats

# Verify configuration
python scripts/manage_workers.py health-check
```

#### High Memory Usage
**Cause**: Large datasets or memory leaks
**Solution**:
```bash
# Monitor memory usage
docker stats yodabuffett-daily-worker

# Adjust resource limits in docker-compose.yml
deploy:
  resources:
    limits:
      memory: 512M  # Reduce if needed
```

#### Rate Limiting from MFN.se
**Cause**: Too aggressive scraping
**Solution**:
```bash
# Increase rate delay in .env
RATE_DELAY=3.0  # Increase from 2.0

# Reduce batch sizes
MAX_DAILY_TARGETS=50  # Reduce from 100
```

### Debug Commands

#### Interactive Debugging
```bash
# Access CLI container for debugging
docker exec -it yodabuffett-worker-cli bash

# Run commands interactively
python workers/event_scheduler.py
python workers/worker_config.py
```

#### Log Analysis
```bash
# View detailed logs
python scripts/manage_workers.py docker --action logs --service daily-worker --tail 500

# Follow logs in real-time
python scripts/manage_workers.py docker --action logs --service daily-worker --follow

# Export logs for analysis
docker logs yodabuffett-daily-worker > worker-logs.txt 2>&1
```

### Getting Help

1. **Check Health**: Always run `python scripts/manage_workers.py health-check` first
2. **Review Logs**: Check recent logs for error messages
3. **Verify Configuration**: Ensure .env file is properly configured
4. **Test Components**: Use dry-run modes to test without actual scraping
5. **Monitor Resources**: Check system resources (memory, CPU, disk space)

## Security Considerations

### Database Security
- Use strong passwords for database users
- Limit network access to PostgreSQL port (5432)
- Regularly update PostgreSQL to latest stable version

### Container Security  
- Workers run as non-root user inside containers
- No unnecessary ports exposed
- Resource limits prevent resource exhaustion
- Regular base image updates

### Web Scraping Ethics
- Respectful rate limiting (minimum 2 seconds between requests)
- Proper User-Agent identification
- Monitor for rate limiting responses
- Be prepared to back off if requested

## Performance Optimization

### Database Optimization
- Regular VACUUM and ANALYZE operations
- Monitor slow queries with pg_stat_statements
- Consider connection pooling for high-volume deployments

### Worker Optimization
- Adjust batch sizes based on available memory
- Monitor and tune rate limiting delays
- Use SSD storage for better I/O performance

### Resource Monitoring
```bash
# Monitor Docker resource usage
docker stats

# Monitor disk usage
df -h /var/lib/docker

# Monitor PostgreSQL performance
docker exec -it yodabuffett-postgres psql -U postgres -c "
SELECT query, mean_exec_time, calls 
FROM pg_stat_statements 
ORDER BY mean_exec_time DESC 
LIMIT 10;
"
```

This completes the comprehensive setup guide for the YodaBuffett worker system. The system is designed to be production-ready while remaining easy to operate and maintain.