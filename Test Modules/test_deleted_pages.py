#!/usr/bin/env python3
"""
Standalone test for deleted page detection.

This test simulates the real workflow:
1. First visit: URL returns 200 (success) - gets remembered
2. Later visit: Same URL returns 404 - detected as deleted

Usage: python "Test Modules/test_deleted_pages.py"
"""

import os
import sys
import time
from unittest.mock import patch

# Fix import path - add parent directory to sys.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.core.crawler import Crawler
from src.utils.state_manager import StateManager

def test_deleted_page_detection():
    """Test deleted page detection with proper session memory simulation."""
    print("🧪 Deleted Page Detection Test")
    print("=" * 50)
    
    # Test URL
    test_url = "https://www.education.gov.au/test-deleted-page"
    
    print(f"📝 Testing URL: {test_url}")
    print("\n🔄 PHASE 1: Simulating successful first visit...")
    
    # Initialize crawler (this creates the state_manager)
    crawler = Crawler()
    
    try:
        # PHASE 1: Mock successful response (200)
        print("   → Mocking HTTP 200 response...")
        with patch.object(crawler.browser_service, 'get_page') as mock_get_page:
            # Mock successful response with dummy content
            from bs4 import BeautifulSoup
            mock_soup = BeautifulSoup("<html><body><h1>Test Page</h1></body></html>", 'html.parser')
            mock_get_page.return_value = (mock_soup, 200)
            
            # Add URL to crawler's queue and process it
            crawler.state_manager.remaining_urls.add(test_url)
            crawler.state_manager.save_progress()
            
            print(f"   → Processing {test_url} (expecting 200)...")
            
            # Mock other methods to avoid actual file operations
            with patch.object(crawler.drive_service, 'get_or_create_folder') as mock_folder, \
                 patch.object(crawler.drive_service, 'find_file') as mock_find, \
                 patch.object(crawler.drive_service, 'upload_file') as mock_upload, \
                 patch.object(crawler.browser_service, 'save_screenshot') as mock_screenshot, \
                 patch('builtins.open'), \
                 patch('os.remove'):
                
                mock_folder.return_value = ("folder_id", True)
                mock_find.return_value = None
                mock_upload.return_value = "drive_url"
                mock_screenshot.return_value = ("screenshot.png", "filename")
                
                # Process the page - this should succeed and remember the URL
                crawler.process_page(test_url)
        
        print("   ✅ First visit completed - URL now in session memory")
        
        # Verify URL is remembered (use crawler's state_manager)
        url_status = crawler.state_manager.url_status.get(test_url)
        if url_status and url_status.get('last_success'):
            print(f"   ✅ URL status recorded: {url_status}")
        else:
            print("   ❌ URL was not properly recorded!")
            print(f"   🔍 Debug - url_status keys: {list(crawler.state_manager.url_status.keys())}")
            print(f"   🔍 Debug - url_status for test_url: {crawler.state_manager.url_status.get(test_url)}")
            return False
        
        print("\n🔄 PHASE 2: Simulating deleted page (404)...")
        
        # PHASE 2: Mock 404 response
        print("   → Mocking HTTP 404 response...")
        with patch.object(crawler.browser_service, 'get_page') as mock_get_page, \
             patch.object(crawler.slack_service, 'send_deleted_page_alert') as mock_slack:
            
            # Mock 404 response
            mock_get_page.return_value = (None, 404)
            
            # Add URL back to queue (simulate recrawl)
            crawler.state_manager.remaining_urls.add(test_url)
            crawler.state_manager.save_progress()
            
            print(f"   → Processing {test_url} (expecting 404)...")
            
            # Process the page - this should detect deletion
            crawler.process_page(test_url)
            
            # Check if deleted page alert was called
            if mock_slack.called:
                call_args = mock_slack.call_args
                print(f"   ✅ Deleted page alert triggered!")
                print(f"   📱 Alert details: URL={call_args[0][0]}, Status={call_args[0][1]}")
                return True
            else:
                print("   ❌ Deleted page alert was NOT triggered!")
                print(f"   🔍 Debug - Final url_status: {crawler.state_manager.url_status.get(test_url)}")
                return False
                
    except Exception as e:
        print(f"   ❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Cleanup
        if hasattr(crawler, 'browser_service'):
            crawler.browser_service.quit()

def test_multiple_failures():
    """Test detection after multiple failures (not just 404)."""
    
    test_url = "https://www.education.gov.au/test-multiple-failures"
    state_manager = StateManager()
    
    try:
        # First mark as successful
        state_manager.update_url_status(test_url, 200)
        print("   → Marked URL as initially successful")
        
        # First failure (500 error)
        is_deleted_1 = state_manager.update_url_status(test_url, 500)
        print(f"   → First failure (500): Deleted={is_deleted_1}")
        
        # Second failure (500 error) - should trigger deletion
        is_deleted_2 = state_manager.update_url_status(test_url, 500)
        print(f"   → Second failure (500): Deleted={is_deleted_2}")
        
        if is_deleted_2:
            print("   ✅ Multiple failures correctly detected as deleted page!")
            return True
        else:
            print("   ❌ Multiple failures not detected!")
            return False
            
    except Exception as e:
        print(f"   ❌ Multiple failures test failed: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Standalone Deleted Page Detection Test")
    print("=" * 60)
    print("This test simulates the real workflow without modifying crawler code.")
    print("=" * 60)
    
    # Check environment
    required_vars = ['SLACK_TOKEN', 'CHANNEL_ID']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"⚠️  Missing: {', '.join(missing_vars)}")
        print("Slack alerts won't work, but detection logic will be tested.")
    
    # Run tests
    test1_result = test_deleted_page_detection()
    test2_result = test_multiple_failures()
    
    print("\n" + "=" * 60)
    print("📊 TEST RESULTS:")
    print(f"   🗑️  Deleted Page Detection: {'✅ PASS' if test1_result else '❌ FAIL'}")
    print(f"   🔄 Multiple Failures: {'✅ PASS' if test2_result else '❌ FAIL'}")
    
    if test1_result and test2_result:
        print("\n🎉 All tests passed! Deleted page detection is working correctly.")
    else:
        print("\n❌ Some tests failed. Check the implementation.")
        sys.exit(1) 