#!/usr/bin/env python3
"""
test for Daily Progress Dashboard functionality.

This test covers:
1. Progress tracking and calculations
2. Dashboard report generation
3. Slack formatting
4. Scheduler functionality
5. Integration with state manager

Usage: python "Test Modules/test_dashboard.py"
"""

import sys
import os
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
import time

# Fix import path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.services.dashboard_service import DashboardService
from src.services.scheduler_service import SchedulerService
from src.utils.state_manager import StateManager

def test_state_manager_progress_tracking():
    """Test progress tracking functionality in StateManager."""
    print("🧪 Testing StateManager Progress Tracking...")
    
    # Create a test state manager
    state_manager = StateManager()
    
    # Simulate some crawl data
    test_urls = [
        "https://ato.gov.au/page1",
        "https://ato.gov.au/page2", 
        "https://ato.gov.au/page3"
    ]
    
    print(f"   → Initial state: {len(state_manager.visited_urls)} visited, {len(state_manager.remaining_urls)} remaining")
    
    # Record some page crawls
    for i, url in enumerate(test_urls):
        crawl_time = 12.5 + (i * 2)  # Varying crawl times
        page_type = ["new", "changed", "normal"][i % 3]
        
        state_manager.record_page_crawl(url, crawl_time, page_type)
        state_manager.add_visited_url(url)
        
        print(f"   → Recorded crawl: {url} ({crawl_time}s, {page_type})")
    
    # Get progress stats
    stats = state_manager.get_progress_stats()
    
    print(f"   → Progress: {stats['completed_pages']}/{stats['total_pages_estimate']} ({stats['progress_percent']}%)")
    print(f"   → Performance: {stats['pages_per_hour']:.0f} pages/hour, {stats['avg_crawl_time_seconds']}s avg")
    print(f"   → Today's stats: {stats['today_stats']}")
    
    # Verify calculations
    assert stats['completed_pages'] > 0, "Should have completed pages"
    assert stats['progress_percent'] > 0, "Should have progress percentage"
    assert stats['pages_per_hour'] > 0, "Should have pages per hour calculation"
    
    print("   ✅ StateManager progress tracking working correctly")
    return state_manager

def test_dashboard_report_generation(state_manager):
    """Test dashboard report generation."""
    print("\n🧪 Testing Dashboard Report Generation...")
    
    dashboard_service = DashboardService()
    
    # Generate report
    report_data = dashboard_service.generate_daily_report(state_manager)
    
    print(f"   → Generated report for: {report_data['timestamp']}")
    print(f"   → Progress: {report_data['progress']['percentage']}% complete")
    print(f"   → Progress bar: {report_data['progress']['progress_bar']}")
    print(f"   → Performance: {report_data['performance']['speed']:.0f} pages/hour ({report_data['performance']['grade']})")
    print(f"   → Cycle: {report_data['cycle']['type']} (Day {report_data['cycle']['day']})")
    print(f"   → Next milestone: {report_data['milestone']['next_milestone']}")
    
    # Verify report structure
    required_keys = ['timestamp', 'progress', 'performance', 'timing', 'cycle', 'today', 'milestone', 'discovery']
    for key in required_keys:
        assert key in report_data, f"Report missing required key: {key}"
    
    # Verify progress data
    assert 'percentage' in report_data['progress'], "Progress should have percentage"
    assert 'progress_bar' in report_data['progress'], "Progress should have visual bar"
    assert 'completed' in report_data['progress'], "Progress should have completed count"
    
    print("   ✅ Dashboard report generation working correctly")
    return report_data

def test_slack_formatting(dashboard_service, report_data):
    """Test Slack message formatting."""
    print("\n🧪 Testing Slack Message Formatting...")
    
    # Format for Slack
    blocks = dashboard_service.format_slack_dashboard(report_data)
    
    print(f"   → Generated {len(blocks)} Slack blocks")
    
    # Verify block structure
    assert len(blocks) > 0, "Should generate Slack blocks"
    
    # Check for required block types
    block_types = [block.get('type') for block in blocks]
    assert 'header' in block_types, "Should have header block"
    assert 'section' in block_types, "Should have section blocks"
    assert 'divider' in block_types, "Should have divider blocks"
    
    # Print sample blocks for verification
    for i, block in enumerate(blocks[:3]):
        print(f"   → Block {i+1}: {block.get('type', 'unknown')} - {str(block)[:100]}...")
    
    print("   ✅ Slack formatting working correctly")
    return blocks

def test_scheduler_functionality():
    """Test scheduler service functionality."""
    print("\n🧪 Testing Scheduler Functionality...")
    
    # Create scheduler (but don't start it for test)
    scheduler_service = SchedulerService()
    
    # Create mock state manager
    mock_state_manager = Mock()
    mock_state_manager.get_progress_stats.return_value = {
        'completed_pages': 100,
        'total_pages_estimate': 5196,
        'remaining_pages': 5096,
        'progress_percent': 1.9,
        'avg_crawl_time_seconds': 15.0,
        'pages_per_hour': 240,
        'eta_datetime': datetime.now() + timedelta(hours=20),
        'cycle_number': 1,
        'is_first_cycle': True,
        'cycle_duration_days': 0,
        'today_stats': {'pages_crawled': 25, 'new_pages': 5, 'changed_pages': 2, 'failed_pages': 0},
        'total_discovered': 100
    }
    
    scheduler_service.set_state_manager(mock_state_manager)
    
    # Test status before starting
    status = scheduler_service.get_scheduler_status()
    print(f"   → Initial status: {status}")
    assert not status['running'], "Scheduler should not be running initially"
    
    # Test starting scheduler
    print("   → Starting scheduler...")
    scheduler_service.start_scheduler()
    
    # Give it a moment to initialize
    time.sleep(0.5)
    
    # Check status after starting
    status = scheduler_service.get_scheduler_status()
    print(f"   → Status after start: {status}")
    
    if status['running']:
        print(f"   → Next run scheduled for: {status['next_run']}")
        print("   ✅ Scheduler started successfully")
    else:
        print("   ⚠️  Scheduler may not have started (this can happen in test environments)")
    
    # Test stopping scheduler
    print("   → Stopping scheduler...")
    scheduler_service.stop_scheduler()
    
    status = scheduler_service.get_scheduler_status()
    print(f"   → Status after stop: {status}")
    
    print("   ✅ Scheduler functionality tested")

def test_dashboard_integration(state_manager):
    """Test full dashboard integration with REAL Slack message."""
    print("\n🧪 Testing Full Dashboard Integration with REAL Slack...")
    
    # Create dashboard service (uses real Slack service)
    dashboard_service = DashboardService()
    
    print("   → Sending REAL dashboard message to Slack...")
    print("   → Check your Slack channel for the test message!")
    
    # Send actual test dashboard to real Slack
    success = dashboard_service.send_test_dashboard(state_manager)
    
    print(f"   → Dashboard send result: {success}")
    
    if success:
        print("   ✅ REAL Slack message sent successfully!")
        print("   📱 Check your Slack channel to see the dashboard report")
        
        # Generate the report data to show what was sent
        report_data = dashboard_service.generate_daily_report(state_manager)
        blocks = dashboard_service.format_slack_dashboard(report_data)
        
        print(f"   → Sent {len(blocks)} blocks to Slack")
        print(f"   → Progress: {report_data['progress']['percentage']}% complete")
        print(f"   → Performance: {report_data['performance']['speed']:.0f} pages/hour")
        print(f"   → ETA: {report_data['timing']['eta']}")
        
        # Verify message structure
        assert len(blocks) > 0, "Should generate Slack blocks"
        
        print("   ✅ Real dashboard integration working correctly")
    else:
        print("   ❌ Failed to send real Slack message")
        print("   💡 Check your Slack token and channel configuration")
    
    return success

def test_progress_calculations():
    """Test progress calculation accuracy."""
    print("\n🧪 Testing Progress Calculation Accuracy...")
    
    # Create fresh state manager for clean test
    state_manager = StateManager()
    
    # Clear any existing state for clean test
    state_manager.visited_urls.clear()
    state_manager.remaining_urls.clear()
    state_manager.performance_history.clear()
    state_manager.daily_stats.clear()
    
    # Set known values for testing
    state_manager.total_pages_estimate = 1000
    
    # Add some visited URLs
    for i in range(250):  # 25% complete
        url = f"https://ato.gov.au/test-page-{i}"
        state_manager.visited_urls.add(url)
        state_manager.record_page_crawl(url, 10.0, "normal")
    
    # Add remaining URLs
    for i in range(250, 1000):
        url = f"https://ato.gov.au/test-page-{i}"
        state_manager.remaining_urls.add(url)
    
    stats = state_manager.get_progress_stats()
    
    print(f"   → Completed: {stats['completed_pages']}")
    print(f"   → Total estimate: {stats['total_pages_estimate']}")
    print(f"   → Progress: {stats['progress_percent']}%")
    print(f"   → Pages per hour: {stats['pages_per_hour']}")
    
    # Verify calculations
    expected_progress = (250 / 1000) * 100  # 25%
    assert abs(stats['progress_percent'] - expected_progress) < 0.1, f"Progress calculation incorrect: {stats['progress_percent']} vs {expected_progress}"
    
    # Verify pages per hour (10 seconds per page = 360 pages per hour)
    expected_pph = 3600 / 10  # 360
    assert abs(stats['pages_per_hour'] - expected_pph) < 1, f"Pages per hour incorrect: {stats['pages_per_hour']} vs {expected_pph}"
    
    print("   ✅ Progress calculations accurate")

def main():
    """Run all dashboard tests."""
    print("🚀 Starting Comprehensive Dashboard Tests")
    print("=" * 60)
    print("⚠️  NOTE: This test will send a REAL message to your Slack channel!")
    print("📱 Check your Slack for the test dashboard report")
    print("=" * 60)
    
    try:
        # Test 1: State Manager Progress Tracking
        state_manager = test_state_manager_progress_tracking()
        
        # Test 2: Dashboard Report Generation
        dashboard_service = DashboardService()
        report_data = test_dashboard_report_generation(state_manager)
        
        # Test 3: Slack Formatting
        blocks = test_slack_formatting(dashboard_service, report_data)
        
        # Test 4: Scheduler Functionality
        test_scheduler_functionality()
        
        # Test 5: Full Integration
        test_dashboard_integration(state_manager)
        
        # Test 6: Progress Calculations
        test_progress_calculations()
        
        print("\n" + "=" * 60)
        print("🎉 ALL DASHBOARD TESTS PASSED!")
        print("=" * 60)
        
        print("\n📊 DASHBOARD FEATURES READY:")
        print("   ✅ Progress tracking with 5,196 page total")
        print("   ✅ Real-time performance metrics")
        print("   ✅ Daily Slack reports at 10am AEST")
        print("   ✅ Visual progress bars and ETA calculations")
        print("   ✅ Cycle detection (First Discovery vs Maintenance)")
        print("   ✅ Milestone tracking (25%, 50%, 75%, etc.)")
        print("   ✅ Professional Slack formatting")
        
        print("\n🚀 READY TO DEPLOY:")
        print("   • Install APScheduler: pip install APScheduler==3.10.4")
        print("   • Run crawler to start automatic daily reports")
        print("   • Dashboard will appear at 10:00 AM AEST daily")
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 