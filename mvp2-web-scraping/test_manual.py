#!/usr/bin/env python3

# Simple test without external dependencies
import urllib.request
import os
from datetime import datetime

def test_volvo_download():
    """Test downloading the Volvo report directly"""
    url = "https://www.volvogroup.com/content/dam/volvo-group/markets/master/investors/reports-and-presentations/interim-reports/2025/volvo-group-q2-2025-sve.pdf"
    
    output_dir = "data/reports"
    os.makedirs(output_dir, exist_ok=True)
    
    filename = "Volvo_Group_Q2_2025.pdf"
    filepath = os.path.join(output_dir, filename)
    
    try:
        print(f"Downloading Volvo Q2 2025 report...")
        print(f"URL: {url}")
        
        # Download the file
        urllib.request.urlretrieve(url, filepath)
        
        # Check if downloaded successfully
        if os.path.exists(filepath):
            file_size = os.path.getsize(filepath) / 1024 / 1024  # MB
            print(f"✓ Success! Downloaded {filename} ({file_size:.1f} MB)")
            print(f"✓ Saved to: {filepath}")
            return True
        else:
            print("✗ Download failed - file not found")
            return False
            
    except Exception as e:
        print(f"✗ Download error: {e}")
        return False

if __name__ == "__main__":
    test_volvo_download()