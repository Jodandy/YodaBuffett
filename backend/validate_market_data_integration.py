#!/usr/bin/env python3
"""
Market Data Integration Validation Script
Validates the Yahoo Finance provider implementation and provides setup instructions.
"""

import os
import sys
from pathlib import Path

def check_file_structure():
    """Check if all required files are in place"""
    print("📂 Checking Market Data Integration File Structure")
    print("=" * 60)
    
    required_files = [
        "domains/market_data/services/yahoo_provider.py",
        "docs/features/market-data-integration.md"
    ]
    
    missing_files = []
    for file_path in required_files:
        if os.path.exists(file_path):
            print(f"✅ {file_path}")
        else:
            print(f"❌ {file_path}")
            missing_files.append(file_path)
    
    return len(missing_files) == 0

def check_dependencies():
    """Check if required Python packages are available"""
    print("\n📦 Checking Dependencies")
    print("=" * 30)
    
    dependencies = ["yfinance", "numpy", "asyncio"]
    missing_deps = []
    
    for dep in dependencies:
        try:
            __import__(dep)
            print(f"✅ {dep}")
        except ImportError:
            print(f"❌ {dep}")
            missing_deps.append(dep)
    
    return missing_deps

def show_provider_features():
    """Show the key features of the Yahoo Finance provider"""
    print("\n🚀 Yahoo Finance Provider Features")
    print("=" * 40)
    print("✅ Nordic Symbol Mappings (AAK.ST, VOLV-B.ST, etc.)")
    print("✅ Historical Price Data Fetching")
    print("✅ Performance Metrics Calculation") 
    print("✅ Fuzzy Company Name Matching")
    print("✅ Provider-Agnostic Architecture")
    print("✅ Error Handling & Logging")

def show_next_steps(missing_deps):
    """Show next steps based on current state"""
    print("\n🔄 Next Steps")
    print("=" * 15)
    
    if missing_deps:
        print("1. Install missing dependencies:")
        for dep in missing_deps:
            if dep == "yfinance":
                print(f"   pip install {dep}")
            elif dep == "numpy":
                print(f"   pip install {dep}")
        print()
    
    print("2. Test the Yahoo Finance provider:")
    print("   python domains/market_data/services/yahoo_provider.py")
    print()
    
    print("3. Validate with real Nordic companies:")
    print("   - AAK (Swedish industrial company)")
    print("   - Volvo Group (Major Swedish automotive)")
    print("   - Atlas Copco (Swedish industrial)")
    print()
    
    print("4. Integrate with temporal anomaly detection:")
    print("   - Map company names from document database")
    print("   - Calculate performance for anomaly dates")
    print("   - Validate anomaly predictions vs actual returns")

def show_integration_architecture():
    """Show the integration architecture"""
    print("\n🏗️  Integration Architecture")
    print("=" * 30)
    print("📄 Document Database → 🏢 Company Names → 📊 Yahoo Symbols → 💰 Price Data")
    print()
    print("Example Flow:")
    print("1. 'Volvo_Group' from document → 'VOLV-B.ST' symbol → Yahoo Finance")
    print("2. Fetch 6-month price history → Calculate performance metrics")
    print("3. Correlate with temporal anomaly dates from embeddings")
    print("4. Validate: Did anomalies predict actual price movements?")

def main():
    """Main validation function"""
    print("🧪 YodaBuffett Market Data Integration Validation")
    print("=" * 55)
    
    # Check file structure
    files_ok = check_file_structure()
    
    # Check dependencies 
    missing_deps = check_dependencies()
    
    # Show features
    show_provider_features()
    
    # Show architecture
    show_integration_architecture()
    
    # Show next steps
    show_next_steps(missing_deps)
    
    # Summary
    print("\n📋 Summary")
    print("=" * 12)
    if files_ok and not missing_deps:
        print("✅ Market data integration ready for testing!")
        print("💡 Run: python domains/market_data/services/yahoo_provider.py")
    elif files_ok:
        print(f"⚠️  Files ready, but {len(missing_deps)} dependencies missing")
        print("💡 Install dependencies then test the provider")
    else:
        print("❌ Missing required files - check implementation")

if __name__ == "__main__":
    main()