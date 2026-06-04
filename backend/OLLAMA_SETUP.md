# Local LLM Setup (Ollama) - FREE Alternative to OpenAI

Run AI analysis completely free on your Mac, no API keys needed!

## Quick Setup (5 minutes)

### 1. Install Ollama

**Option A: Download installer**
- Go to: https://ollama.com/download
- Download for macOS
- Install the app

**Option B: Homebrew**
```bash
brew install ollama
```

### 2. Start Ollama Server

```bash
ollama serve
```

Keep this terminal open!

### 3. Download a Model (one-time, ~4.7GB)

In a NEW terminal:

```bash
# Good balance of quality and speed
ollama pull llama3.1:8b

# Or if you want better quality (larger, slower):
# ollama pull llama3.1:70b
```

This downloads once and caches locally.

### 4. Test AI Analyst with Local LLM

```bash
cd /Users/jdandemar/Documents/YodaBuffett/backend
source venv/bin/activate
python test_ai_analyst.py --company "Volvo" --local
```

## Benefits of Local LLM

✅ **Free forever** - No API costs, no usage limits
✅ **Private** - Data never leaves your computer
✅ **Fast** - No network latency
✅ **Offline** - Works without internet
✅ **No rate limits** - Generate unlimited analyses

## Model Options

| Model | Size | Speed | Quality | RAM Needed |
|-------|------|-------|---------|------------|
| llama3.1:8b | 4.7GB | Fast | Good | 8GB |
| llama3.1:70b | 40GB | Slower | Excellent | 64GB |
| mistral:7b | 4.1GB | Very Fast | Good | 8GB |
| mixtral:8x7b | 26GB | Medium | Excellent | 32GB |

**Recommended:** `llama3.1:8b` for most users

## Troubleshooting

**"Connection refused" error:**
```bash
# Make sure Ollama is running
ollama serve
```

**Model not found:**
```bash
# Download it first
ollama pull llama3.1:8b
```

**Out of memory:**
```bash
# Use a smaller model
ollama pull mistral:7b
```

## Cost Comparison

**OpenAI GPT-4o-mini:**
- Cost: ~$0.08 per analysis
- 100 analyses = $8.00

**Local Ollama:**
- Cost: $0.00
- Unlimited analyses = $0.00

## Using Local LLM in Production

Once you verify it works, you can use local LLM for all analyses:

```python
# In your code
from domains.ai_analyst.models import AnalysisRequest, LLMProvider

request = AnalysisRequest(
    company_id="uuid-here",
    llm_provider=LLMProvider.LOCAL,  # Use local instead of OPENAI
    model="llama3.1:8b"
)
```

Perfect for:
- Development/testing
- High-volume batch processing
- Privacy-sensitive analysis
- Cost-conscious operations
