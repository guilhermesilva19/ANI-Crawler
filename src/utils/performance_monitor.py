"""Performance monitoring and reporting utilities."""

import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from collections import deque

__all__ = ['PerformanceMonitor', 'PerformanceReport']


@dataclass
class PerformanceMetric:
    """Single performance metric data point."""
    timestamp: datetime
    metric_name: str
    value: float
    metadata: Dict = field(default_factory=dict)


class PerformanceMonitor:
    """Comprehensive performance monitoring system."""
    
    def __init__(self, max_history_size: int = 1000):
        """Initialize performance monitor."""
        self.max_history_size = max_history_size
        self._metrics: Dict[str, deque] = {}
        self._lock = threading.RLock()
        self._start_time = datetime.now()
        
        # System metrics
        self._page_processing_times = deque(maxlen=max_history_size)
        self._memory_usage_history = deque(maxlen=max_history_size)
        self._error_counts = {}
        self._throughput_history = deque(maxlen=100)  # Pages per minute
        
        # Performance counters
        self._counters = {
            'pages_processed': 0,
            'errors_occurred': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'browser_reuses': 0,
            'browser_creations': 0,
            'db_operations': 0,
            'upload_operations': 0
        }
        
    def record_page_processing_time(self, url: str, processing_time: float, page_type: str = 'normal') -> None:
        """Record page processing performance."""
        with self._lock:
            metric = PerformanceMetric(
                timestamp=datetime.now(),
                metric_name='page_processing_time',
                value=processing_time,
                metadata={'url': url, 'page_type': page_type}
            )
            
            self._page_processing_times.append(metric)
            self._counters['pages_processed'] += 1
            
            # Calculate throughput (pages per minute)
            self._calculate_throughput()
    
    def record_memory_usage(self, memory_mb: float, status: str = 'OK') -> None:
        """Record memory usage metrics."""
        with self._lock:
            metric = PerformanceMetric(
                timestamp=datetime.now(),
                metric_name='memory_usage',
                value=memory_mb,
                metadata={'status': status}
            )
            
            self._memory_usage_history.append(metric)
    
    def record_error(self, error_type: str, error_message: str, url: Optional[str] = None) -> None:
        """Record error occurrence."""
        with self._lock:
            self._counters['errors_occurred'] += 1
            
            if error_type not in self._error_counts:
                self._error_counts[error_type] = 0
            self._error_counts[error_type] += 1
            
            # Store detailed error info
            if 'errors' not in self._metrics:
                self._metrics['errors'] = deque(maxlen=self.max_history_size)
            
            self._metrics['errors'].append({
                'timestamp': datetime.now(),
                'type': error_type,
                'message': error_message,
                'url': url
            })
    
    def increment_counter(self, counter_name: str, amount: int = 1) -> None:
        """Increment a performance counter."""
        with self._lock:
            if counter_name in self._counters:
                self._counters[counter_name] += amount
    
    def _calculate_throughput(self) -> None:
        """Calculate current throughput (pages per minute)."""
        now = datetime.now()
        one_minute_ago = now - timedelta(minutes=1)
        
        # Count pages processed in the last minute
        recent_pages = sum(
            1 for metric in self._page_processing_times
            if metric.timestamp >= one_minute_ago
        )
        
        self._throughput_history.append(recent_pages)
    
    def get_current_stats(self) -> Dict:
        """Get current performance statistics."""
        with self._lock:
            uptime = datetime.now() - self._start_time
            
            # Calculate averages
            avg_processing_time = 0.0
            if self._page_processing_times:
                avg_processing_time = sum(
                    metric.value for metric in self._page_processing_times
                ) / len(self._page_processing_times)
            
            # Calculate current throughput
            current_throughput = 0
            if self._throughput_history:
                current_throughput = self._throughput_history[-1]
            
            # Calculate memory stats
            current_memory = 0.0
            avg_memory = 0.0
            max_memory = 0.0
            if self._memory_usage_history:
                current_memory = self._memory_usage_history[-1].value
                avg_memory = sum(m.value for m in self._memory_usage_history) / len(self._memory_usage_history)
                max_memory = max(m.value for m in self._memory_usage_history)
            
            # Calculate error rate
            error_rate = 0.0
            if self._counters['pages_processed'] > 0:
                error_rate = self._counters['errors_occurred'] / self._counters['pages_processed']
            
            # Calculate cache hit rate
            cache_hit_rate = 0.0
            total_cache_operations = self._counters['cache_hits'] + self._counters['cache_misses']
            if total_cache_operations > 0:
                cache_hit_rate = self._counters['cache_hits'] / total_cache_operations
            
            # Calculate browser reuse rate
            browser_reuse_rate = 0.0
            total_browser_operations = self._counters['browser_reuses'] + self._counters['browser_creations']
            if total_browser_operations > 0:
                browser_reuse_rate = self._counters['browser_reuses'] / total_browser_operations
            
            return {
                'uptime_seconds': uptime.total_seconds(),
                'pages_processed': self._counters['pages_processed'],
                'current_throughput_ppm': current_throughput,  # Pages per minute
                'avg_processing_time_seconds': avg_processing_time,
                'current_memory_mb': current_memory,
                'avg_memory_mb': avg_memory,
                'max_memory_mb': max_memory,
                'error_rate': error_rate,
                'total_errors': self._counters['errors_occurred'],
                'cache_hit_rate': cache_hit_rate,
                'browser_reuse_rate': browser_reuse_rate,
                'db_operations': self._counters['db_operations'],
                'upload_operations': self._counters['upload_operations'],
                'error_breakdown': dict(self._error_counts)
            }
    
    def get_performance_trend(self, minutes: int = 10) -> Dict:
        """Get performance trend over specified time period."""
        with self._lock:
            cutoff_time = datetime.now() - timedelta(minutes=minutes)
            
            # Filter recent metrics
            recent_processing_times = [
                metric.value for metric in self._page_processing_times
                if metric.timestamp >= cutoff_time
            ]
            
            recent_memory_usage = [
                metric.value for metric in self._memory_usage_history
                if metric.timestamp >= cutoff_time
            ]
            
            recent_throughput = list(self._throughput_history)[-minutes:] if len(self._throughput_history) >= minutes else list(self._throughput_history)
            
            return {
                'time_period_minutes': minutes,
                'pages_in_period': len(recent_processing_times),
                'avg_processing_time': sum(recent_processing_times) / max(1, len(recent_processing_times)),
                'min_processing_time': min(recent_processing_times) if recent_processing_times else 0,
                'max_processing_time': max(recent_processing_times) if recent_processing_times else 0,
                'avg_throughput': sum(recent_throughput) / max(1, len(recent_throughput)),
                'avg_memory_usage': sum(recent_memory_usage) / max(1, len(recent_memory_usage)),
                'memory_trend': 'increasing' if len(recent_memory_usage) > 1 and recent_memory_usage[-1] > recent_memory_usage[0] else 'stable'
            }
    
    def generate_report(self) -> 'PerformanceReport':
        """Generate comprehensive performance report."""
        current_stats = self.get_current_stats()
        trend_10min = self.get_performance_trend(10)
        trend_60min = self.get_performance_trend(60)
        
        return PerformanceReport(
            timestamp=datetime.now(),
            uptime_hours=current_stats['uptime_seconds'] / 3600,
            total_pages=current_stats['pages_processed'],
            avg_processing_time=current_stats['avg_processing_time_seconds'],
            current_throughput=current_stats['current_throughput_ppm'],
            memory_usage_mb=current_stats['current_memory_mb'],
            error_rate=current_stats['error_rate'],
            cache_hit_rate=current_stats['cache_hit_rate'],
            browser_reuse_rate=current_stats['browser_reuse_rate'],
            trend_10min=trend_10min,
            trend_60min=trend_60min,
            error_breakdown=current_stats['error_breakdown']
        )


@dataclass
class PerformanceReport:
    """Structured performance report."""
    timestamp: datetime
    uptime_hours: float
    total_pages: int
    avg_processing_time: float
    current_throughput: float
    memory_usage_mb: float
    error_rate: float
    cache_hit_rate: float
    browser_reuse_rate: float
    trend_10min: Dict
    trend_60min: Dict
    error_breakdown: Dict
    
    def to_formatted_string(self) -> str:
        """Generate formatted string representation of the report."""
        return f"""
🚀 ANI-Crawler Performance Report
Generated: {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}

📊 Overall Performance:
  • Uptime: {self.uptime_hours:.1f} hours
  • Total Pages Processed: {self.total_pages:,}
  • Average Processing Time: {self.avg_processing_time:.2f}s
  • Current Throughput: {self.current_throughput:.1f} pages/minute
  • Memory Usage: {self.memory_usage_mb:.1f} MB

📈 Efficiency Metrics:
  • Cache Hit Rate: {self.cache_hit_rate:.1%}
  • Browser Reuse Rate: {self.browser_reuse_rate:.1%}
  • Error Rate: {self.error_rate:.2%}

⏱️ Recent Trends (10 minutes):
  • Pages Processed: {self.trend_10min['pages_in_period']}
  • Avg Processing Time: {self.trend_10min['avg_processing_time']:.2f}s
  • Avg Throughput: {self.trend_10min['avg_throughput']:.1f} pages/min
  • Memory Trend: {self.trend_10min['memory_trend']}

🔍 Hourly Trends (60 minutes):
  • Pages Processed: {self.trend_60min['pages_in_period']}
  • Avg Processing Time: {self.trend_60min['avg_processing_time']:.2f}s
  • Avg Throughput: {self.trend_60min['avg_throughput']:.1f} pages/min

❌ Error Breakdown:
{self._format_error_breakdown()}
        """.strip()
    
    def _format_error_breakdown(self) -> str:
        """Format error breakdown for display."""
        if not self.error_breakdown:
            return "  • No errors recorded"
        
        lines = []
        for error_type, count in sorted(self.error_breakdown.items(), key=lambda x: x[1], reverse=True):
            lines.append(f"  • {error_type}: {count}")
        
        return "\n".join(lines)
    
    def get_performance_score(self) -> float:
        """Calculate overall performance score (0-100)."""
        score = 100.0
        
        # Deduct points for high error rate
        score -= min(self.error_rate * 100, 30)
        
        # Deduct points for slow processing
        if self.avg_processing_time > 30:  # More than 30 seconds per page
            score -= min((self.avg_processing_time - 30) * 2, 20)
        
        # Deduct points for low cache hit rate
        score -= (1.0 - self.cache_hit_rate) * 15
        
        # Deduct points for low browser reuse rate
        score -= (1.0 - self.browser_reuse_rate) * 10
        
        # Deduct points for high memory usage
        if self.memory_usage_mb > 400:
            score -= min((self.memory_usage_mb - 400) / 10, 15)
        
        return max(0.0, score)
