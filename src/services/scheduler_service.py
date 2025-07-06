"""Scheduler service for automated daily dashboard reports."""

import os
import threading
from datetime import datetime
from typing import Optional
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

from src.services.dashboard_service import DashboardService
from src.utils.state_manager import StateManager

__all__ = ['SchedulerService']


class SchedulerService:
    """Service for scheduling automated daily dashboard reports."""
    
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.dashboard_service = DashboardService()
        self.state_manager: Optional[StateManager] = None
        self.is_running = False
        
        # Australian Eastern Time zone
        self.aest_tz = pytz.timezone('Australia/Sydney')
        
    def set_state_manager(self, state_manager: StateManager) -> None:
        """Set the state manager reference."""
        self.state_manager = state_manager
    
    def _send_daily_dashboard(self) -> None:
        """Internal method to send daily dashboard."""
        if not self.state_manager:
            print("❌ Cannot send dashboard: State manager not set")
            return
        
        try:
            print(f"📊 Sending scheduled daily dashboard at {datetime.now().strftime('%I:%M %p AEST')}")
            success = self.dashboard_service.send_daily_dashboard(self.state_manager)
            
            if success:
                print("✅ Daily dashboard sent successfully")
            else:
                print("❌ Failed to send daily dashboard")
                
        except Exception as e:
            print(f"❌ Error sending daily dashboard: {e}")
    
    def start_scheduler(self) -> None:
        """Start the background scheduler for daily reports."""
        if self.is_running:
            print("⚠️  Scheduler already running")
            return
        
        try:
            # Schedule daily dashboard at 10:00 AM AEST
            self.scheduler.add_job(
                func=self._send_daily_dashboard,
                trigger=CronTrigger(
                    hour=10,
                    minute=0,
                    timezone=self.aest_tz
                ),
                id='daily_dashboard',
                name='Daily Progress Dashboard',
                replace_existing=True
            )
            
            self.scheduler.start()
            self.is_running = True
            
            # Calculate next run time for user feedback
            next_run = self.scheduler.get_job('daily_dashboard').next_run_time
            next_run_aest = next_run.astimezone(self.aest_tz)
            
            print(f"⏰ Daily dashboard scheduler started")
            print(f"📅 Next report: {next_run_aest.strftime('%A, %B %d at %I:%M %p AEST')}")
            
        except Exception as e:
            print(f"❌ Failed to start scheduler: {e}")
            self.is_running = False
    
    def stop_scheduler(self) -> None:
        """Stop the background scheduler."""
        if not self.is_running:
            print("⚠️  Scheduler not running")
            return
        
        try:
            self.scheduler.shutdown()
            self.is_running = False
            print("⏹️  Daily dashboard scheduler stopped")
            
        except Exception as e:
            print(f"❌ Error stopping scheduler: {e}")
    
    def send_test_dashboard(self) -> bool:
        """Send a test dashboard immediately."""
        if not self.state_manager:
            print("❌ Cannot send test dashboard: State manager not set")
            return False
        
        print("🧪 Sending test dashboard...")
        return self.dashboard_service.send_test_dashboard(self.state_manager)
    
    def get_scheduler_status(self) -> dict:
        """Get current scheduler status and next run time."""
        if not self.is_running:
            return {
                'running': False,
                'next_run': None,
                'jobs': []
            }
        
        try:
            job = self.scheduler.get_job('daily_dashboard')
            next_run = job.next_run_time.astimezone(self.aest_tz) if job.next_run_time else None
            
            return {
                'running': True,
                'next_run': next_run.strftime('%A, %B %d at %I:%M %p AEST') if next_run else None,
                'jobs': [
                    {
                        'id': job.id,
                        'name': job.name,
                        'next_run': next_run.strftime('%Y-%m-%d %H:%M:%S AEST') if next_run else None
                    }
                ]
            }
            
        except Exception as e:
            print(f"❌ Error getting scheduler status: {e}")
            return {
                'running': False,
                'error': str(e),
                'next_run': None,
                'jobs': []
            }
    
    def reschedule_dashboard(self, hour: int = 10, minute: int = 0) -> bool:
        """Reschedule the daily dashboard to a different time."""
        if not self.is_running:
            print("❌ Cannot reschedule: Scheduler not running")
            return False
        
        try:
            # Remove existing job
            self.scheduler.remove_job('daily_dashboard')
            
            # Add new job with updated time
            self.scheduler.add_job(
                func=self._send_daily_dashboard,
                trigger=CronTrigger(
                    hour=hour,
                    minute=minute,
                    timezone=self.aest_tz
                ),
                id='daily_dashboard',
                name='Daily Progress Dashboard',
                replace_existing=True
            )
            
            # Get next run time
            next_run = self.scheduler.get_job('daily_dashboard').next_run_time
            next_run_aest = next_run.astimezone(self.aest_tz)
            
            print(f"✅ Dashboard rescheduled to {hour:02d}:{minute:02d} AEST")
            print(f"📅 Next report: {next_run_aest.strftime('%A, %B %d at %I:%M %p AEST')}")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to reschedule dashboard: {e}")
            return False
    
    def __del__(self):
        """Cleanup when object is destroyed."""
        if self.is_running:
            try:
                self.scheduler.shutdown(wait=False)
            except:
                pass 