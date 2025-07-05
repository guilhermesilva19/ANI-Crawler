#!/usr/bin/env python3
"""
Test script for Google Sheets logging functionality.

This script tests:
1. Sheets service initialization
2. Spreadsheet creation in Drive folder
3. Monthly tab creation
4. Alert logging (all 3 types)

Usage: python "Test Modules/test_sheets_logging.py"
"""

import os
import sys
from datetime import datetime

# Fix import path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.services.sheets_service import SheetsService

def test_sheets_service():
    """Test the Google Sheets logging service."""
    print("📊 Google Sheets Logging Test")
    print("=" * 50)
    
    # Check environment
    required_vars = ['PRIVATE_KEY', 'CLIENT_EMAIL', 'FOLDER_PARENT_ID']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        print("Please set these in your .env file before running the test.")
        return False
    
    try:
        print("🔧 Initializing SheetsService...")
        sheets_service = SheetsService()
        
        print(f"✅ Service initialized successfully!")
        print(f"📊 Spreadsheet URL: {sheets_service.get_spreadsheet_url()}")
        
        # Test monthly tab creation
        print("\n📅 Testing monthly tab creation...")
        current_month = datetime.now().strftime("%Y-%m")
        tab_name = sheets_service.get_or_create_monthly_tab()
        print(f"✅ Monthly tab ready: {tab_name}")
        
        # Test logging different alert types
        print("\n📝 Testing alert logging...")
        
        # Test 1: New Page Alert
        print("   → Testing New Page alert...")
        sheets_service.log_new_page_alert(
            "https://www.education.gov.au/test-new-page",
            "https://drive.google.com/screenshots/test",
            "https://drive.google.com/html/test"
        )
        
        # Test 2: Changed Page Alert
        print("   → Testing Changed Page alert...")
        sheets_service.log_changed_page_alert(
            "https://www.education.gov.au/test-changed-page",
            "Added 2 text sections; Removed 1 link; Added 1 PDF",
            "https://drive.google.com/screenshots/test2",
            "https://drive.google.com/html/test2"
        )
        
        # Test 3: Deleted Page Alert
        print("   → Testing Deleted Page alert...")
        last_success = datetime(2024, 12, 15, 10, 30, 0)
        sheets_service.log_deleted_page_alert(
            "https://www.education.gov.au/test-deleted-page",
            404,
            last_success
        )
        
        print("\n✅ All tests completed successfully!")
        print(f"📊 Check your spreadsheet: {sheets_service.get_spreadsheet_url()}")
        print(f"📁 Located in Drive folder: {os.getenv('FOLDER_PARENT_ID')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_monthly_rollover():
    """Test monthly tab creation for different months."""
    print("\n📅 Testing Monthly Rollover")
    print("=" * 40)
    
    try:
        sheets_service = SheetsService()
        
        # Test different months
        test_dates = [
            datetime(2024, 1, 15),
            datetime(2024, 2, 20), 
            datetime(2024, 12, 31)
        ]
        
        for test_date in test_dates:
            tab_name = sheets_service.get_or_create_monthly_tab(test_date)
            expected = test_date.strftime("%Y-%m")
            
            if tab_name == expected:
                print(f"   ✅ {test_date.strftime('%B %Y')} → {tab_name}")
            else:
                print(f"   ❌ {test_date.strftime('%B %Y')} → Expected {expected}, got {tab_name}")
                return False
        
        print("✅ Monthly rollover test passed!")
        return True
        
    except Exception as e:
        print(f"❌ Monthly rollover test failed: {e}")
        return False

if __name__ == "__main__":
    print("📊 Google Sheets Integration Test")
    print("=" * 60)
    print("This test will create/update a real spreadsheet in your Drive folder!")
    print("=" * 60)
    
    # Ask for confirmation
    response = input("\nDo you want to run the sheets logging test? (y/N): ")
    
    if response.lower() in ['y', 'yes']:
        test1_result = test_sheets_service()
        test2_result = test_monthly_rollover() if test1_result else False
        
        print("\n" + "=" * 60)
        print("📊 TEST RESULTS:")
        print(f"   📋 Sheets Service: {'✅ PASS' if test1_result else '❌ FAIL'}")
        print(f"   📅 Monthly Rollover: {'✅ PASS' if test2_result else '❌ FAIL'}")
        
        if test1_result and test2_result:
            print("\n🎉 All tests passed! Google Sheets logging is working correctly.")
        else:
            print("\n❌ Some tests failed. Check the implementation.")
            sys.exit(1)
    else:
        print("\n⏹️  Test cancelled.") 