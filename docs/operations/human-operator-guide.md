# YodaBuffett - Human Operator Guide

## Overview
Everything a human operator needs to run, manage, and maintain the YodaBuffett system.

## Quick Start Commands

### System Startup (Full Stack)
```bash
# Start everything
docker-compose up -d

# View all services status
docker-compose ps

# View logs for all services
docker-compose logs -f

# Stop everything
docker-compose down
```

### Development Mode
```bash
# Backend only (for frontend development)
docker-compose up -d database redis vector-db
cd backend && python -m uvicorn main:app --reload --port 8000

# Frontend only (for backend development)  
cd frontend && npm run dev

# Single service development
docker-compose up -d database redis
cd backend/research-service && python main.py
```

### MVP 1 Specific (Report Analysis)
```bash
# Start MVP 1 environment
cd mvp1-report-analysis
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows
pip install -r requirements.txt
python app.py

# Process a document
curl -X POST -F "file=@sample-10k.pdf" http://localhost:5000/analyze
```

## Environment Configuration

### Required Environment Variables

#### Core System (.env)
```bash
# Database
DATABASE_URL=postgresql://yoda:buffett123@localhost:5432/yodabuffett
REDIS_URL=redis://localhost:6379

# AI Services
OPENAI_API_KEY=sk-...  # Get from OpenAI dashboard
ANTHROPIC_API_KEY=sk-ant-...  # Get from Anthropic console

# Vector Database
PINECONE_API_KEY=...  # Get from Pinecone dashboard
PINECONE_ENVIRONMENT=us-east-1-aws

# Security
JWT_SECRET=your-super-secret-jwt-key-here
ENCRYPTION_KEY=32-byte-encryption-key-here

# External APIs
SEC_API_KEY=...  # For enhanced SEC data (optional)
ALPHA_VANTAGE_KEY=...  # For market data (future)
```

#### Development Specific (.env.development)
```bash
# Debugging
DEBUG=true
LOG_LEVEL=DEBUG

# Local services
POSTGRES_USER=yoda
POSTGRES_PASSWORD=buffett123
POSTGRES_DB=yodabuffett

# Redis
REDIS_PASSWORD=""  # No password for local

# File Storage (local)
STORAGE_TYPE=local
STORAGE_PATH=./data/uploads
```

#### Production Specific (.env.production)
```bash
# Security (DO NOT COMMIT THESE)
DATABASE_URL=postgresql://prod_user:STRONG_PASSWORD@prod-db:5432/yodabuffett
REDIS_URL=redis://:STRONG_PASSWORD@prod-redis:6379

# Cloud Storage
STORAGE_TYPE=s3
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
S3_BUCKET=yodabuffett-production
```

## Service Management

### Database Operations
```bash
# Connect to PostgreSQL
psql postgresql://yoda:buffett123@localhost:5432/yodabuffett

# Run migrations
cd backend && python -m alembic upgrade head

# Reset database (CAUTION: Deletes all data)
cd backend && python -m alembic downgrade base
docker-compose down -v  # Removes volumes too
docker-compose up -d database

# Backup database
pg_dump postgresql://yoda:buffett123@localhost:5432/yodabuffett > backup.sql

# Restore database
psql postgresql://yoda:buffett123@localhost:5432/yodabuffett < backup.sql
```

### Redis Operations
```bash
# Connect to Redis
redis-cli -h localhost -p 6379

# Clear all cache
redis-cli FLUSHALL

# Monitor Redis activity
redis-cli MONITOR

# Check memory usage
redis-cli INFO memory
```

### Vector Database (Pinecone)
```bash
# List indexes
curl -H "Api-Key: YOUR_API_KEY" https://api.pinecone.io/indexes

# Check index stats
curl -H "Api-Key: YOUR_API_KEY" https://api.pinecone.io/indexes/yodabuffett/stats
```

## API Keys & Credentials Management

### Development Phase (MVP/POC)
**Location**: Store in `.env` files (NOT committed to git)

#### OpenAI API Key
- **Where to get**: https://platform.openai.com/api-keys
- **Cost monitoring**: https://platform.openai.com/usage
- **Usage limits**: Set monthly spend limits in OpenAI dashboard

#### Anthropic API Key  
- **Where to get**: https://console.anthropic.com/
- **Cost monitoring**: Check console for usage
- **Rate limits**: Monitor in console

#### Pinecone API Key
- **Where to get**: https://app.pinecone.io/
- **Free tier limits**: 1 index, 5M vectors
- **Monitoring**: Dashboard shows usage

### Production Phase (Future)
**Location**: Use cloud secret management
- AWS Secrets Manager
- Azure Key Vault  
- Google Secret Manager
- HashiCorp Vault

## Helper Scripts & Tools

### Data Management Scripts
```bash
# Download sample SEC filings
./scripts/download-sample-filings.sh

# Parse and clean sample documents
python scripts/parse-sample-docs.py

# Generate test embeddings
python scripts/generate-embeddings.py
```

### Development Utilities
```bash
# Run all tests
./scripts/run-tests.sh

# Code formatting
./scripts/format-code.sh

# Lint checking
./scripts/lint-check.sh

# Generate API documentation
./scripts/generate-docs.sh
```

### Monitoring & Debugging
```bash
# Check system health
./scripts/health-check.sh

# View service logs
./scripts/view-logs.sh [service-name]

# Monitor API costs
python scripts/monitor-costs.py

# Performance profiling
python scripts/profile-performance.py
```

## File Locations & Data Storage

### Local Development Structure
```
YodaBuffett/
├── .env                    # Main environment config
├── .env.development       # Dev-specific config
├── data/
│   ├── uploads/           # User-uploaded files
│   ├── processed/         # Processed documents
│   ├── cache/            # File cache
│   └── backups/          # Database backups
├── logs/
│   ├── app.log           # Application logs
│   ├── error.log         # Error logs
│   └── access.log        # API access logs
└── scripts/              # Helper scripts
```

### Important Paths to Monitor
- **Log files**: `./logs/` (check for errors)
- **Upload directory**: `./data/uploads/` (monitor disk space)
- **Cache directory**: `./data/cache/` (can be cleared)
- **Database backups**: `./data/backups/` (ensure regular backups)

## Monitoring & Maintenance

### Daily Checks
- [ ] Check service status: `docker-compose ps`
- [ ] Review error logs: `tail -f logs/error.log`
- [ ] Monitor API costs: OpenAI/Anthropic dashboards
- [ ] Check disk space: `df -h`

### Weekly Tasks
- [ ] Backup database: `pg_dump > weekly_backup.sql`
- [ ] Clear old cache files: `find data/cache -mtime +7 -delete`
- [ ] Review system performance logs
- [ ] Update API usage tracking

### Monthly Tasks
- [ ] Review and rotate API keys (production)
- [ ] Analyze cost trends and optimize
- [ ] Update dependencies: `pip freeze > requirements.txt`
- [ ] Archive old log files

## Troubleshooting Common Issues

### Service Won't Start
```bash
# Check ports
netstat -tulpn | grep LISTEN

# Check docker logs
docker-compose logs [service-name]

# Rebuild containers
docker-compose down
docker-compose build --no-cache
docker-compose up
```

### Database Connection Issues
```bash
# Test connection
pg_isready -h localhost -p 5432

# Check database logs
docker-compose logs database

# Reset database connection
docker-compose restart database
```

### High API Costs
```bash
# Check recent usage
python scripts/analyze-api-usage.py

# Enable caching
redis-cli CONFIG SET maxmemory 256mb

# Review LLM prompt efficiency
python scripts/audit-prompts.py
```

### Out of Disk Space
```bash
# Check disk usage
du -sh data/

# Clear old files
find data/uploads -mtime +30 -delete
find logs -mtime +7 -delete

# Compress old backups
gzip data/backups/*.sql
```

## Security Checklist

### Development Security
- [ ] `.env` files in `.gitignore`
- [ ] No hardcoded passwords in code
- [ ] Local HTTPS certificates for testing
- [ ] Regular dependency updates

### API Security
- [ ] Rate limiting enabled
- [ ] API key rotation schedule
- [ ] Input validation on all endpoints
- [ ] Error messages don't leak sensitive info

### Data Security
- [ ] Database passwords are strong
- [ ] File upload validation
- [ ] Regular security scans
- [ ] Access logs monitored

## Emergency Procedures

### System Down
1. Check service status: `docker-compose ps`
2. Review logs: `docker-compose logs`
3. Restart services: `docker-compose restart`
4. If database issue, restore from backup
5. Update incident log

### Data Loss
1. Stop all services immediately
2. Restore from most recent backup
3. Check backup integrity
4. Restart services
5. Verify data consistency

### Security Incident
1. Rotate all API keys immediately
2. Review access logs for suspicious activity
3. Change all passwords
4. Scan for malware
5. Document incident

## Contact Information & Resources

### External Services Support
- **OpenAI Support**: https://help.openai.com/
- **Anthropic Support**: https://support.anthropic.com/
- **Pinecone Support**: https://support.pinecone.io/

### Internal Resources
- **Architecture docs**: `docs/architecture/`
- **API documentation**: `docs/api/` (when available)
- **Troubleshooting**: `docs/operations/`
- **Development guide**: `docs/development/`

### Emergency Contacts
- **System Administrator**: [Your contact info]
- **Database Admin**: [Contact info]
- **Security Team**: [Contact info]

## External Service Credentials

### Email Accounts

#### Investor Relations Email
- **Email**: yodabuffett.ir@gmail.com
- **Password**: !BuffayTime3214
- **Purpose**: Subscribe to company IR newsletters and receive automated financial reports
- **Setup Notes**:
  - Enable "Less secure app access" or use App Password
  - Check inbox regularly for report notifications
  - Configure email parsing pipeline to extract PDFs