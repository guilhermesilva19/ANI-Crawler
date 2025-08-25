#!/usr/bin/env python3
"""
Test script that uses ONLY the existing BrowserService functionality.
This script tests screenshot capture using the built-in methods.
"""

import os
import sys
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.services.browser_service import BrowserService


def test_browser_service():
    """Test BrowserService screenshot functionality."""
    print("🚀 Testing BrowserService Screenshot Functionality")
    print("=" * 50)
    
    browser_service = None
    
    try:
        # Test 1: Initialize BrowserService
        print("📱 Test 1: Initializing BrowserService...")
        browser_service = BrowserService()
        print("✅ BrowserService initialized successfully")
        
        # Test 2: Load a simple, fast page
        test_url = "https://www.ato.gov.au/early-childhood/provider-obligations/child-care-enforcement-action-register/child-care-enforcement-action-register-20162017"
        print(f"\n📄 Test 2: Loading page {test_url}...")
        soup, status_code = browser_service.get_page(test_url)
        
        if status_code == 200:
            print("✅ Page loaded successfully")
            
            # Test 3: Take screenshot
            print(f"\n📸 Test 3: Taking screenshot...")
            try:
                screenshot_path, filename = browser_service.save_screenshot(test_url)
                
                if screenshot_path and os.path.exists(screenshot_path):
                    file_size = os.path.getsize(screenshot_path)
                    print("✅ Screenshot captured successfully!")
                    print(f"   📁 Filename: {filename}")
                    print(f"   📁 Path: {screenshot_path}")
                    print(f"   📏 Size: {file_size} bytes")
                    
                    # Test 4: Verify screenshot directory
                    print(f"\n📂 Test 4: Checking screenshot directory...")
                    screenshot_dir = os.path.dirname(screenshot_path)
                    if os.path.exists(screenshot_dir):
                        print(f"✅ Screenshot directory exists: {screenshot_dir}")
                        
                        # List all screenshots
                        screenshots = [f for f in os.listdir(screenshot_dir) if f.endswith('.png')]
                        print(f"📋 Found {len(screenshots)} screenshot(s) in directory:")
                        for screenshot in screenshots:
                            full_path = os.path.join(screenshot_dir, screenshot)
                            size = os.path.getsize(full_path)
                            print(f"   📸 {screenshot} ({size} bytes)")
                    else:
                        print("❌ Screenshot directory not found")
                    
                    # Test 5: Test with another simple URL
                    print(f"\n📄 Test 5: Testing with another URL...")
                    test_url2 = "https://example.com"
                    print(f"   Loading: {test_url2}")
                    
                    soup2, status_code2 = browser_service.get_page(test_url2)
                    if status_code2 == 200:
                        print("   ✅ Page loaded successfully")
                        
                        try:
                            screenshot_path2, filename2 = browser_service.save_screenshot(test_url2)
                            if screenshot_path2 and os.path.exists(screenshot_path2):
                                size2 = os.path.getsize(screenshot_path2)
                                print(f"   ✅ Second screenshot saved: {filename2} ({size2} bytes)")
                            else:
                                print("   ❌ Second screenshot failed")
                        except Exception as e:
                            print(f"   ❌ Second screenshot error: {e}")
                    else:
                        print(f"   ❌ Failed to load second page (status: {status_code2})")
                    
                else:
                    print("❌ Screenshot capture failed")
                    
            except Exception as screenshot_error:
                print(f"❌ Screenshot error: {screenshot_error}")
                print("💡 This might be due to page loading timeout or rendering issues")
                print("💡 Try using simpler pages or check network connectivity")
        else:
            print(f"❌ Failed to load page (status: {status_code})")
        
        print("\n🎉 All tests completed!")
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
    
    finally:
        # Clean up using existing quit method
        if browser_service:
            print("\n🧹 Cleaning up BrowserService...")
            browser_service.quit()
            print("✅ Cleanup completed")


if __name__ == "__main__":
    test_browser_service()
