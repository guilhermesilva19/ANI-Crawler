"""Real-time performance monitor for 3-minute interval status reporting."""

import threading
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, List
import pytz
from dataclasses import dataclass

__all__ = ['RealTimePerformanceMonitor', 'PerformanceSnapshot']


@dataclass
class PerformanceSnapshot:
    """Snapshot of performance metrics at a specific time."""
    timestamp: datetime
    pages_processed_last_3min: int
    current_speed_per_hour: float
    avg_processing_time: float
    memory_usage_mb: float
    error_count_last_3min: int
    grade: str
    grade_emoji: str
    total_pages_processed: int
    active_workers: int = 0
    browser_pool_stats: Dict = None


class RealTimePerformanceMonitor:
    """Real-time performance monitoring with 3-minute status updates."""
    
    def __init__(self, 
                 state_manager=None, 
                 slack_service=None,
                 browser_pool=None,
                 interval_minutes: int = 3):
        """Initialize real-time performance monitor."""
        self.state_manager = state_manager
        self.slack_service = slack_service
        self.browser_pool = browser_pool
        self.interval_seconds = interval_minutes * 60
        self.aest_tz = pytz.timezone('Australia/Sydney')
        
        # Performance tracking
        self._performance_history: List[PerformanceSnapshot] = []
        self._page_count_tracker = []  # Track pages processed with timestamps
        self._error_tracker = []  # Track errors with timestamps
        self._processing_times = []  # Track processing times
        self._last_report_time = None
        
        # Threading
        self._monitoring = False
        self._monitor_thread = None
        self._lock = threading.RLock()
        
        # Performance thresholds (same as existing system but adapted for real-time)
        self._performance_thresholds = {
            'excellent': 300,  # 300+ pages/hour
            'good': 200,       # 200+ pages/hour  
            'normal': 120,     # 120+ pages/hour
            'slow': 60,        # 60+ pages/hour
            'very_slow': 0     # Below 60 pages/hour
        }
        
        print(f"🕒 Real-time performance monitor initialized (every {interval_minutes} minutes)")
    
    def start_monitoring(self) -> None:
        """Start the real-time performance monitoring."""
        if self._monitoring:
            print("⚠️  Performance monitor already running")
            return
        
        self._monitoring = True
        self._monitor_thread = threading.Thread(target=self._monitor_worker, daemon=True)
        self._monitor_thread.start()
        self._last_report_time = datetime.now()
        
        print(f"🚀 Real-time performance monitoring started (every {self.interval_seconds/60:.1f} minutes)")
    
    def stop_monitoring(self) -> None:
        """Stop the real-time performance monitoring."""
        self._monitoring = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5.0)
        print("🛑 Real-time performance monitoring stopped")
    
    def record_page_processed(self, url: str, processing_time: float, page_type: str = 'normal') -> None:
        """Record that a page was processed."""
        with self._lock:
            now = datetime.now()
            self._page_count_tracker.append({
                'timestamp': now,
                'url': url,
                'processing_time': processing_time,
                'page_type': page_type
            })
            self._processing_times.append(processing_time)
            
            # Keep only last 24 hours of data
            cutoff = now - timedelta(hours=24)
            self._page_count_tracker = [p for p in self._page_count_tracker if p['timestamp'] >= cutoff]
            self._processing_times = self._processing_times[-1000:]  # Keep last 1000 processing times
    
    def record_error(self, error_type: str, url: str = None) -> None:
        """Record that an error occurred."""
        with self._lock:
            now = datetime.now()
            self._error_tracker.append({
                'timestamp': now,
                'error_type': error_type,
                'url': url
            })
            
            # Keep only last 24 hours of errors
            cutoff = now - timedelta(hours=24)
            self._error_tracker = [e for e in self._error_tracker if e['timestamp'] >= cutoff]
    
    def _monitor_worker(self) -> None:
        """Main monitoring worker that runs every 3 minutes."""
        while self._monitoring:
            try:
                # Wait for the interval
                time.sleep(self.interval_seconds)
                
                if not self._monitoring:
                    break
                
                # Generate performance snapshot
                snapshot = self._generate_performance_snapshot()
                
                # Store snapshot
                with self._lock:
                    self._performance_history.append(snapshot)
                    
                    # Keep only last 24 hours of snapshots
                    cutoff = datetime.now() - timedelta(hours=24)
                    self._performance_history = [
                        s for s in self._performance_history 
                        if s.timestamp >= cutoff
                    ]
                
                # Report performance
                self._report_performance(snapshot)
                self._last_report_time = datetime.now()
                
            except Exception as e:
                print(f"❌ Performance monitoring error: {e}")
                time.sleep(30)  # Wait 30 seconds before retrying
    
    def _generate_performance_snapshot(self) -> PerformanceSnapshot:
        """Generate a performance snapshot for the current moment."""
        now = datetime.now()
        three_minutes_ago = now - timedelta(minutes=3)
        
        with self._lock:
            # Count pages processed in last 3 minutes
            recent_pages = [
                p for p in self._page_count_tracker 
                if p['timestamp'] >= three_minutes_ago
            ]
            pages_last_3min = len(recent_pages)
            
            # Calculate current speed (pages per hour based on last 3 minutes)
            current_speed_per_hour = (pages_last_3min / 3) * 60  # Convert 3-min rate to hourly
            
            # Calculate average processing time from recent pages
            if recent_pages:
                avg_processing_time = sum(p['processing_time'] for p in recent_pages) / len(recent_pages)
            elif self._processing_times:
                avg_processing_time = sum(self._processing_times[-20:]) / len(self._processing_times[-20:])
            else:
                avg_processing_time = 0.0
            
            # Count errors in last 3 minutes
            recent_errors = [
                e for e in self._error_tracker 
                if e['timestamp'] >= three_minutes_ago
            ]
            error_count_last_3min = len(recent_errors)
            
            # Get memory usage
            memory_usage_mb = self._get_current_memory_usage()
            
            # Get performance grade
            grade, grade_emoji = self._get_performance_grade(current_speed_per_hour)
            
            # Get total pages processed
            total_pages = len(self._page_count_tracker)
            
            # Get browser pool stats if available
            browser_stats = None
            if self.browser_pool:
                try:
                    browser_stats = self.browser_pool.get_stats()
                except:
                    pass
        
        return PerformanceSnapshot(
            timestamp=now,
            pages_processed_last_3min=pages_last_3min,
            current_speed_per_hour=current_speed_per_hour,
            avg_processing_time=avg_processing_time,
            memory_usage_mb=memory_usage_mb,
            error_count_last_3min=error_count_last_3min,
            grade=grade,
            grade_emoji=grade_emoji,
            total_pages_processed=total_pages,
            browser_pool_stats=browser_stats
        )
    
    def _get_current_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        try:
            import psutil
            import os
            process = psutil.Process(os.getpid())
            return process.memory_info().rss / (1024 * 1024)
        except ImportError:
            return 0.0
        except Exception:
            return 0.0
    
    def _get_performance_grade(self, pages_per_hour: float) -> tuple:
        """Get performance grade and emoji based on pages per hour."""
        if pages_per_hour >= self._performance_thresholds['excellent']:
            return "Excellent", "🚀"
        elif pages_per_hour >= self._performance_thresholds['good']:
            return "Good", "⚡"
        elif pages_per_hour >= self._performance_thresholds['normal']:
            return "Normal", "✅"
        elif pages_per_hour >= self._performance_thresholds['slow']:
            return "Slow", "🐌"
        else:
            return "Very Slow", "⚠️"
    
    def _report_performance(self, snapshot: PerformanceSnapshot) -> None:
        """Report performance snapshot to console and optionally Slack."""
        # Console report
        self._print_console_report(snapshot)
        
        # Slack report (only for significant changes or poor performance)
        if self._should_send_slack_alert(snapshot):
            self._send_slack_alert(snapshot)
    
    def _print_console_report(self, snapshot: PerformanceSnapshot) -> None:
        """Print performance report to console."""
        time_str = snapshot.timestamp.strftime('%H:%M:%S')
        
        print(f"\n{'='*60}")
        print(f"🕒 REAL-TIME PERFORMANCE REPORT - {time_str}")
        print(f"{'='*60}")
        print(f"📊 Last 3 minutes: {snapshot.pages_processed_last_3min} pages processed")
        print(f"🚀 Current speed: {snapshot.current_speed_per_hour:.1f} pages/hour {snapshot.grade_emoji} {snapshot.grade}")
        print(f"⏱️  Avg processing: {snapshot.avg_processing_time:.1f} seconds per page")
        print(f"🧠 Memory usage: {snapshot.memory_usage_mb:.1f} MB")
        
        if snapshot.error_count_last_3min > 0:
            print(f"❌ Errors (3min): {snapshot.error_count_last_3min}")
        else:
            print(f"✅ No errors in last 3 minutes")
        
        print(f"📈 Total processed: {snapshot.total_pages_processed} pages")
        
        # Browser pool stats
        if snapshot.browser_pool_stats:
            stats = snapshot.browser_pool_stats
            print(f"🏊 Browser pool: {stats.get('active_browsers', 0)} active, {stats.get('reuse_ratio', 0):.1%} reuse rate")
        
        # Performance trend
        trend = self._get_performance_trend()
        if trend:
            print(f"📈 Trend: {trend}")
        
        print(f"{'='*60}\n")
    
    def _should_send_slack_alert(self, snapshot: PerformanceSnapshot) -> bool:
        """Determine if we should send a Slack alert."""
        # Send alert for very slow performance
        if snapshot.grade in ['Very Slow', 'Slow']:
            return True
        
        # Send alert for high error rate
        if snapshot.error_count_last_3min >= 3:
            return True
        
        # Send alert for high memory usage
        if snapshot.memory_usage_mb > 450:  # Close to 512MB limit
            return True
        
        # Send alert for significant performance improvement
        if len(self._performance_history) >= 2:
            prev_snapshot = self._performance_history[-2]
            if (prev_snapshot.grade in ['Slow', 'Very Slow'] and 
                snapshot.grade in ['Normal', 'Good', 'Excellent']):
                return True
        
        return False
    
    def _send_slack_alert(self, snapshot: PerformanceSnapshot) -> None:
        """Send performance alert to Slack."""
        if not self.slack_service:
            return
        
        try:
            time_str = snapshot.timestamp.strftime('%H:%M:%S AEST')
            
            # Determine alert type
            if snapshot.grade in ['Very Slow', 'Slow']:
                alert_type = "⚠️ PERFORMANCE ALERT"
                color = "#ff6b6b"
            elif snapshot.error_count_last_3min >= 3:
                alert_type = "❌ ERROR ALERT"
                color = "#ff6b6b"
            elif snapshot.memory_usage_mb > 450:
                alert_type = "🧠 MEMORY ALERT"
                color = "#ffa500"
            else:
                alert_type = "📈 PERFORMANCE UPDATE"
                color = "#51cf66"
            
            blocks = [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{alert_type} - {time_str}"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Current Performance Status:*\n"
                               f"• Speed: {snapshot.current_speed_per_hour:.1f} pages/hour {snapshot.grade_emoji} *{snapshot.grade}*\n"
                               f"• Last 3 minutes: {snapshot.pages_processed_last_3min} pages processed\n"
                               f"• Avg processing time: {snapshot.avg_processing_time:.1f}s per page\n"
                               f"• Memory usage: {snapshot.memory_usage_mb:.1f} MB\n"
                               f"• Errors (3min): {snapshot.error_count_last_3min}"
                    }
                }
            ]
            
            self.slack_service.send_message(blocks)
            
        except Exception as e:
            print(f"❌ Failed to send Slack alert: {e}")
    
    def _get_performance_trend(self) -> Optional[str]:
        """Get performance trend over recent snapshots."""
        if len(self._performance_history) < 3:
            return None
        
        recent_speeds = [s.current_speed_per_hour for s in self._performance_history[-3:]]
        
        if recent_speeds[-1] > recent_speeds[0] * 1.2:
            return "📈 Improving"
        elif recent_speeds[-1] < recent_speeds[0] * 0.8:
            return "📉 Declining"
        else:
            return "➡️ Stable"
    
    def get_current_performance(self) -> Optional[PerformanceSnapshot]:
        """Get the most recent performance snapshot."""
        with self._lock:
            if self._performance_history:
                return self._performance_history[-1]
            return None
    
    def get_performance_summary(self, hours: int = 1) -> Dict:
        """Get performance summary for the specified time period."""
        if not self._performance_history:
            return {'error': 'No performance data available'}
        
        cutoff = datetime.now() - timedelta(hours=hours)
        recent_snapshots = [
            s for s in self._performance_history 
            if s.timestamp >= cutoff
        ]
        
        if not recent_snapshots:
            return {'error': f'No data for last {hours} hour(s)'}
        
        avg_speed = sum(s.current_speed_per_hour for s in recent_snapshots) / len(recent_snapshots)
        avg_memory = sum(s.memory_usage_mb for s in recent_snapshots) / len(recent_snapshots)
        total_errors = sum(s.error_count_last_3min for s in recent_snapshots)
        
        grade, emoji = self._get_performance_grade(avg_speed)
        
        return {
            'time_period_hours': hours,
            'snapshots_count': len(recent_snapshots),
            'avg_speed_per_hour': avg_speed,
            'avg_memory_mb': avg_memory,
            'total_errors': total_errors,
            'performance_grade': grade,
            'grade_emoji': emoji,
            'trend': self._get_performance_trend()
        }
