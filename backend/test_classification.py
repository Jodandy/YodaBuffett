"""
Test the improved Swedish document classification
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from nordic_ingestion.collectors.aggregator.mfn_collector import MFNCollector

def test_swedish_classification():
    print("🧪 Testing Swedish document classification...")
    
    collector = MFNCollector()
    
    # Test cases with Swedish titles
    test_cases = [
        ("det andra kvartalet 2025", "", "quarterly_report"),
        ("Volvo Group första kvartalet 2024", "", "quarterly_report"), 
        ("Q2 delårsrapport", "", "quarterly_report"),
        ("Årsrapport 2024", "", "annual_report"),
        ("Helår resultat", "", "annual_report"),
        ("Volvo köper nytt företag", "", "corporate_action"),
        ("Förvärv av teknologibolag", "", "corporate_action"),
        ("Styrelse föreslår dividend", "", "governance"),
        ("Bolagsstämma 2025", "", "governance"),
        ("Pressmeddelande om ny VD", "", "press_release"),
    ]
    
    print(f"\n📊 Testing {len(test_cases)} classification cases:")
    
    correct = 0
    for title, content, expected in test_cases:
        result = collector._classify_news_type(title, content)
        status = "✅" if result == expected else "❌"
        if result == expected:
            correct += 1
        
        print(f"  {status} '{title}' → {result} (expected: {expected})")
    
    print(f"\n🎯 Accuracy: {correct}/{len(test_cases)} ({100*correct//len(test_cases)}%)")
    
    # Test the specific case you mentioned
    print(f"\n🔍 Your specific case:")
    swedish_title = "det andra kvartalet 2025"
    english_title = "second quarter 2025"
    
    swedish_result = collector._classify_news_type(swedish_title, "")
    english_result = collector._classify_news_type(english_title, "")
    
    print(f"  📄 Swedish: '{swedish_title}' → {swedish_result}")
    print(f"  📄 English: '{english_title}' → {english_result}")
    
    if swedish_result == "quarterly_report" and english_result == "quarterly_report":
        print(f"  ✅ Both correctly classified as quarterly reports!")
    else:
        print(f"  ❌ Classification failed")

if __name__ == "__main__":
    test_swedish_classification()