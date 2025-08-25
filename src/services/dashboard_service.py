"""Dashboard service for generating daily progress reports."""

import os
from datetime import datetime, timedelta
from typing import Dict, Optional, List
import pytz
from src.services.slack_service import SlackService
from src.utils.state_manager import StateManager

__all__ = ['DashboardService']


class DashboardService:
    """Service for generating and sending daily progress dashboards."""
    
    def __init__(self):
        self.slack_service = SlackService()
        self.aest_tz = pytz.timezone('Australia/Sydney')
        
    def generate_progress_bar(self, percentage: float, width: int = 10) -> str:
        """Generate a visual progress bar."""
        filled = round((percentage / 100) * width)
        empty = width - filled
        return "▓" * filled + "░" * empty
    
    def format_time_duration(self, hours: float) -> str:
        """Format time duration in a human-readable way."""
        if hours < 1:
            minutes = int(hours * 60)
            return f"{minutes} minutes"
        elif hours < 24:
            return f"{hours:.1f} hours"
        else:
            days = int(hours // 24)
            remaining_hours = hours % 24
            if remaining_hours < 1:
                return f"{days} days"
            else:
                return f"{days} days, {remaining_hours:.1f} hours"
    
    def format_eta(self, eta_datetime: Optional[datetime]) -> str:
        """Format ETA in a user-friendly way."""
        if not eta_datetime:
            return "Unknown"
        
        now = datetime.now()
        if eta_datetime.date() == now.date():
            return f"{eta_datetime.strftime('%I:%M %p')} today"
        elif eta_datetime.date() == (now + timedelta(days=1)).date():
            return f"{eta_datetime.strftime('%I:%M %p')} tomorrow"
        else:
            return eta_datetime.strftime('%b %d at %I:%M %p')
    
    def get_milestone_info(self, current_percent: float) -> Dict[str, str]:
        """Get information about the next milestone."""
        milestones = [25, 50, 75, 90, 95, 100]
        
        for milestone in milestones:
            if current_percent < milestone:
                return {
                    'next_milestone': f"{milestone}%",
                    'progress_to_milestone': f"{milestone - current_percent:.1f}% away"
                }
        
        return {
            'next_milestone': "Complete! 🎉",
            'progress_to_milestone': "Finished"
        }
    
    def generate_daily_report(self, state_manager: StateManager) -> Dict:
        """Generate comprehensive daily progress report."""
        stats = state_manager.get_progress_stats()
        
        # Generate progress bar
        progress_bar = self.generate_progress_bar(stats['progress_percent'])
        
        # Calculate time estimates
        eta_mode = stats.get('eta_mode', 'cycle_completion')
        if stats['eta_datetime']:
            eta_text = self.format_eta(stats['eta_datetime'])
            time_remaining_hours = (stats['eta_datetime'] - datetime.now()).total_seconds() / 3600
            time_remaining_text = self.format_time_duration(max(time_remaining_hours, 0))
        else:
            eta_text = "Calculating..."
            time_remaining_text = "Unknown"

        # Labels depend on whether we're estimating completion or next cycle start
        if eta_mode == 'next_cycle_start':
            eta_label = "Next crawl start"
            time_label = "Until next crawl"
        else:
            eta_label = "ETA"
            time_label = "Remaining"
        
        # Get milestone information
        milestone_info = self.get_milestone_info(stats['progress_percent'])
        
        # Calculate cycle information
        cycle_type = "First Discovery" if stats['is_first_cycle'] else "Maintenance"
        cycle_day = stats['cycle_duration_days'] + 1
        
        # Performance metrics
        performance_grade = self._get_performance_grade(stats['pages_per_hour'])
        
        return {
            'timestamp': datetime.now(self.aest_tz).strftime('%B %d, %Y - %I:%M %p AEST'),
            'progress': {
                'completed': stats['completed_pages'],
                'total': stats['total_known_pages'],
                'percentage': stats['progress_percent'],
                'progress_bar': progress_bar,
                'remaining': stats['remaining_pages']
            },
            'performance': {
                'speed': stats['pages_per_hour'],
                'avg_time': stats['avg_crawl_time_seconds'],
                'grade': performance_grade
            },
            'timing': {
                'eta': eta_text,
                'eta_label': eta_label,
                'time_remaining': time_remaining_text,
                'time_label': time_label
            },
            'cycle': {
                'type': cycle_type,
                'number': stats['cycle_number'],
                'day': cycle_day
            },
            'today': stats['today_stats'],
            'milestone': milestone_info,
            'discovery': {
                'total_found': stats['total_discovered']
            }
        }
    
    def _get_performance_grade(self, pages_per_hour: float) -> str:
        """Get performance grade based on pages per hour."""
        if pages_per_hour >= 300:
            return "🚀 Excellent"
        elif pages_per_hour >= 200:
            return "⚡ Good"
        elif pages_per_hour >= 120:
            return "✅ Normal"
        elif pages_per_hour >= 60:
            return "🐌 Slow"
        else:
            return "⚠️ Very Slow"
    
    def format_slack_dashboard(self, report_data: Dict) -> List[Dict]:
        """Format the report data into Slack blocks."""
        blocks = []
        
        # Header
        blocks.append({
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "📊 Daily ANI-Crawler Progress Report"
            }
        })
        
        # Timestamp
        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"📅 {report_data['timestamp']}"
                }
            ]
        })
        
        blocks.append({"type": "divider"})
        
        # Progress section
        progress = report_data['progress']
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*📊 CRAWL PROGRESS*\n{progress['progress_bar']} {progress['percentage']}% ({progress['completed']:,}/{progress['total']:,} pages discovered)"
            }
        })
        
        # Performance section
        perf = report_data['performance']
        timing = report_data['timing']
        blocks.append({
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*⏱️ PERFORMANCE*\n• Speed: {perf['speed']:.0f} pages/hour {perf['grade']}\n• Avg time: {perf['avg_time']} sec/page"
                },
                {
                    "type": "mrkdwn", 
                    "text": f"*🎯 TIMING*\n• {timing['eta_label']}: {timing['eta']}\n• {timing['time_label']}: {timing['time_remaining']}"
                }
            ]
        })
        
        # Today's activity
        today = report_data['today']
        if today['pages_crawled'] > 0:
            activity_text = f"*🔄 TODAY'S ACTIVITY*\n"
            activity_text += f"• Pages crawled: {today['pages_crawled']}\n"
            if today['new_pages'] > 0:
                activity_text += f"• New pages: {today['new_pages']}\n"
            if today['changed_pages'] > 0:
                activity_text += f"• Changed pages: {today['changed_pages']}\n"
            if today['failed_pages'] > 0:
                activity_text += f"• Failed pages: {today['failed_pages']}"
        else:
            activity_text = "*🔄 TODAY'S ACTIVITY*\n• No pages crawled yet today"
        
        # Cycle and milestone info
        cycle = report_data['cycle']
        milestone = report_data['milestone']
        cycle_text = f"*📈 CYCLE STATUS*\n"
        cycle_text += f"• Cycle: {cycle['type']} (Day {cycle['day']})\n"
        
        # Show queue-based info consistently
        cycle_text += f"• Pages completed: {progress['completed']:,}\n"
        cycle_text += f"• Pages in queue: {progress['remaining']:,}\n"
        if cycle['type'] == "First Discovery":
            cycle_text += f"• Total discovered: {progress['total']:,}\n"
        else:
            cycle_text += f"• Est. time until next cycle: {timing['time_remaining']}\n"
            
        cycle_text += f"• Next milestone: {milestone['next_milestone']} ({milestone['progress_to_milestone']})"
        
        blocks.append({
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": activity_text
                },
                {
                    "type": "mrkdwn",
                    "text": cycle_text
                }
            ]
        })
        
        # Note: Discovery insights removed - using queue-based approach
        
        blocks.append({"type": "divider"})
        
        # Footer
        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "🤖 Automated daily crawl progress • Next report tomorrow at 10:00 AM AEST • Monitoring ato.gov.au for changes"
                }
            ]
        })
        
        return blocks
    
    def send_daily_dashboard(self, state_manager: StateManager) -> bool:
        """Generate and send daily dashboard to Slack."""
        try:
            # Generate report data
            report_data = self.generate_daily_report(state_manager)
            
            # Format for Slack
            blocks = self.format_slack_dashboard(report_data)
            
            # Send to Slack
            self.slack_service.send_message(blocks)
            
            print(f"📊 Daily dashboard sent at {datetime.now().strftime('%I:%M %p')}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to send daily dashboard: {e}")
            return False
    
    def send_test_dashboard(self, state_manager: StateManager) -> bool:
        """Send a test dashboard immediately (for testing purposes)."""
        print("🧪 Sending test dashboard...")
        return self.send_daily_dashboard(state_manager) 