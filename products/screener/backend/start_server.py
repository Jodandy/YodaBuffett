#!/usr/bin/env python3
"""
Start the YodaBuffett Screener API server
"""

import sys
import os
import uvicorn
from pathlib import Path

# Add current directory to Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

# Import the FastAPI app
try:
    from app.main import app
    print("✅ Successfully imported screener app")
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure you're in the screener backend directory")
    sys.exit(1)

if __name__ == "__main__":
    print("🚀 Starting YodaBuffett Screener Pro...")
    print("📊 With 1,585 companies ready for screening")
    print("🌐 Server will be available at: http://localhost:8000")
    print("📖 API docs available at: http://localhost:8000/docs")
    print("\nPress Ctrl+C to stop the server")
    
    # Start the server
    uvicorn.run(
        "app.main:app",  # Import string format for reload
        host="0.0.0.0",
        port=8000,
        reload=True,  # Auto-reload on changes
        log_level="info"
    )