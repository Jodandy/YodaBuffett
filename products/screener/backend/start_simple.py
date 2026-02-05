#!/usr/bin/env python3
"""
Simple startup for YodaBuffett Screener API (no reload)
"""

import sys
import os
import uvicorn
from pathlib import Path

# Add current directory to Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

if __name__ == "__main__":
    print("🚀 Starting YodaBuffett Screener Pro...")
    print("📊 With 1,585 companies ready for screening")
    print("🌐 Server will be available at: http://localhost:8000")
    print("📖 API docs available at: http://localhost:8000/docs")
    print("🔄 Health check: http://localhost:8000/health/detailed")
    print("\nPress Ctrl+C to stop the server")
    
    # Start the server (no reload to avoid import issues)
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info"
    )