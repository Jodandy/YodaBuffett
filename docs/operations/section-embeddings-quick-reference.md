# Section-Based Embeddings Quick Reference

## Overview
Two-phase system: **Rule-based chunking** → **Multi-provider embeddings**

**Status**: Section chunking is production ready with CID artifact filtering. Multi-provider embeddings ready for testing.

## Phase 1: Section Chunking (Free, Rule-Based) - PRODUCTION READY

### Setup
```bash
python domains/document_intelligence/cli_section_chunking.py setup
```

### Status & Monitoring  
```bash
python domains/document_intelligence/cli_section_chunking.py status
```

### Testing & Validation
```bash
# Test single document
python domains/document_intelligence/cli_section_chunking.py test Volvo

# Inspect section quality  
python domains/document_intelligence/cli_section_chunking.py inspect Volvo
```

### Processing
```bash
# Small batch (testing)
python domains/document_intelligence/cli_section_chunking.py process 5 Volvo

# Production batches
python domains/document_intelligence/cli_section_chunking.py process 50
python domains/document_intelligence/cli_section_chunking.py process 100
```

## Phase 2: Multi-Provider Embeddings (Paid, AI-Based) - READY FOR TESTING

### OpenAI Provider
```bash
# Setup database for OpenAI
python domains/document_intelligence/cli_multi_embeddings.py openai setup

# Check status
python domains/document_intelligence/cli_multi_embeddings.py openai status

# Process sections (small batch)
python domains/document_intelligence/cli_multi_embeddings.py openai process 20 Volvo

# Production batches
python domains/document_intelligence/cli_multi_embeddings.py openai process 100
python domains/document_intelligence/cli_multi_embeddings.py openai process 500
```

### Alternative Providers
```bash
# Cohere (lower cost alternative)
python domains/document_intelligence/cli_multi_embeddings.py cohere setup
python domains/document_intelligence/cli_multi_embeddings.py cohere process 20 Volvo

# Local models (no API cost)  
python domains/document_intelligence/cli_multi_embeddings.py local setup
python domains/document_intelligence/cli_multi_embeddings.py local process 20 Volvo
```

### Provider Comparison
```bash
# Compare all providers
python domains/document_intelligence/cli_multi_embeddings.py openai compare
```

## Environment Setup
```bash
# Required for OpenAI
export OPENAI_API_KEY=sk-proj-...

# Required for Cohere  
export COHERE_API_KEY=co-...
```

## Database Tables Created

### document_sections
- Stores parsed financial sections
- No API costs to generate
- Can validate quality before embedding

### section_embeddings  
- Stores vector embeddings per section per provider
- Supports multiple providers for same sections
- UNIQUE constraint prevents duplicate embeddings

## Cost Estimates

### Section Chunking
- **Cost**: $0 (rule-based parsing)
- **Speed**: ~1 second per document  
- **Output**: ~40-70 sections per document (varies by document complexity)
- **Quality Control**: Automatic CID artifact filtering (skips documents with >1% artifacts)

### OpenAI Embeddings
- **Cost**: ~$0.00003 per section (~$0.0003 per document)
- **Speed**: ~0.5 seconds per section
- **Model**: text-embedding-3-small (1536D)

### Full Collection Estimates
- **419,516 documents** → ~20,975,800 sections (estimated based on 50 sections/doc average)
- **OpenAI cost**: ~$719 for all section embeddings (based on updated section count)
- **Current Status**: 50 documents processed with 2,039 sections created
- **85% cheaper** than mechanical chunks due to intelligent section boundaries

## Workflow Examples

### Start Small (Recommended)
```bash
# 1. Setup databases
python domains/document_intelligence/cli_section_chunking.py setup
python domains/document_intelligence/cli_multi_embeddings.py openai setup

# 2. Test single company
python domains/document_intelligence/cli_section_chunking.py test Volvo
python domains/document_intelligence/cli_section_chunking.py inspect Volvo

# 3. Process small batch
python domains/document_intelligence/cli_section_chunking.py process 10 Volvo
python domains/document_intelligence/cli_multi_embeddings.py openai process 50

# 4. Check results
python domains/document_intelligence/cli_section_chunking.py status  
python domains/document_intelligence/cli_multi_embeddings.py openai status
```

### Production Scale
```bash
# 1. Bulk section chunking
python domains/document_intelligence/cli_section_chunking.py process 500

# 2. Bulk embedding generation  
python domains/document_intelligence/cli_multi_embeddings.py openai process 1000

# 3. Monitor progress
python domains/document_intelligence/cli_multi_embeddings.py openai compare
```

### Multi-Provider Comparison
```bash
# 1. Generate sections once
python domains/document_intelligence/cli_section_chunking.py process 50

# 2. Embed with multiple providers
python domains/document_intelligence/cli_multi_embeddings.py openai process 250
python domains/document_intelligence/cli_multi_embeddings.py cohere process 250  

# 3. Compare results
python domains/document_intelligence/cli_multi_embeddings.py openai compare
```

## Key Benefits

✅ **Cost Optimization**: 85% reduction vs mechanical chunks  
✅ **Quality Control**: Validate sections before spending on embeddings  
✅ **CID Artifact Filtering**: Automatically skips corrupted/scanned documents  
✅ **Provider Flexibility**: Try OpenAI, Cohere, local models on same sections  
✅ **Nordic Specialization**: Financial headers in Swedish, Norwegian, Danish, Finnish  
✅ **Complete Sections**: No more split balance sheets or income statements  
✅ **Independent Operation**: Section chunking and embedding completely separate  
✅ **Production Performance**: ~1 second per document processing speed

## Troubleshooting

### No sections found
```bash
# Check if document text extraction succeeded first
PYTHONPATH=backend python3 domains/document_intelligence/cli_stateful.py status
```

### Poor section quality
```bash
# Inspect sections and review confidence scores
python domains/document_intelligence/cli_section_chunking.py inspect CompanyName
```

### API key errors
```bash
# Verify environment variables
echo $OPENAI_API_KEY | head -c 20
echo $COHERE_API_KEY | head -c 10
```

### No sections to embed
```bash
# Run section chunking first
python domains/document_intelligence/cli_section_chunking.py process 10
```

## Next Steps
1. ✅ **Section chunking is production ready** (50 documents processed successfully)
2. **Start embedding generation** with OpenAI on processed sections
3. **Scale embedding generation** to 100+ documents once validated
4. **Try alternative providers** for cost comparison
5. **Build advanced analytics** on top of sectional embeddings
6. **Process remaining document collection** for complete section coverage