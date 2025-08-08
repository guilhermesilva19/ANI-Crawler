"""Memory monitoring and URL logging utilities for ANI-Crawler."""

import os
import psutil
import gc
import time
from datetime import datetime
from typing import Optional, Dict, Any
import logging

__all__ = ['MemoryMonitor']

class MemoryMonitor:
    """Memory monitoring and URL logging utility."""
    
    def __init__(self, log_file: str = "scraped_urls.log"):
        """Initialize memory monitor with URL logging."""
        self.log_file = log_file
        self.process = psutil.Process()
        self.memory_threshold_mb = 1800  # Alert at 1.8GB (leaving 200MB buffer)
        self.critical_threshold_mb = 1900  # Critical at 1.9GB
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Track memory usage history
        self.memory_history = []
        self.max_memory_usage = 0
        
    def get_memory_usage(self) -> Dict[str, Any]:
        """Get current memory usage statistics."""
        try:
            memory_info = self.process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024  # Convert to MB
            
            # Get system memory info
            system_memory = psutil.virtual_memory()
            
            return {
                'process_memory_mb': round(memory_mb, 2),
                'system_memory_mb': round(system_memory.used / 1024 / 1024, 2),
                'system_memory_percent': round(system_memory.percent, 2),
                'available_memory_mb': round(system_memory.available / 1024 / 1024, 2),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Error getting memory usage: {e}")
            return {
                'process_memory_mb': 0,
                'system_memory_mb': 0,
                'system_memory_percent': 0,
                'available_memory_mb': 0,
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def check_memory_status(self) -> Dict[str, Any]:
        """Check memory status and return warnings if needed."""
        memory_info = self.get_memory_usage()
        process_memory = memory_info['process_memory_mb']
        
        # Update max memory usage
        if process_memory > self.max_memory_usage:
            self.max_memory_usage = process_memory
        
        # Store in history (keep last 100 entries)
        self.memory_history.append(memory_info)
        if len(self.memory_history) > 100:
            self.memory_history.pop(0)
        
        status = {
            'memory_info': memory_info,
            'max_memory_usage': self.max_memory_usage,
            'warning': None,
            'critical': False
        }
        
        # Check thresholds
        if process_memory > self.critical_threshold_mb:
            status['warning'] = f"CRITICAL: Memory usage {process_memory}MB exceeds {self.critical_threshold_mb}MB threshold!"
            status['critical'] = True
            self.logger.critical(status['warning'])
        elif process_memory > self.memory_threshold_mb:
            status['warning'] = f"WARNING: Memory usage {process_memory}MB approaching {self.memory_threshold_mb}MB threshold"
            self.logger.warning(status['warning'])
        
        return status
    
    def log_url_scraped(self, url: str, status: str = "success", error: Optional[str] = None, 
                        memory_usage: Optional[Dict[str, Any]] = None) -> None:
        """Log a scraped URL with timestamp and memory usage."""
        timestamp = datetime.now().isoformat()
        
        if memory_usage is None:
            memory_usage = self.get_memory_usage()
        
        log_entry = {
            'timestamp': timestamp,
            'url': url,
            'status': status,
            'memory_mb': memory_usage['process_memory_mb'],
            'system_memory_percent': memory_usage['system_memory_percent']
        }
        
        if error:
            log_entry['error'] = error
        
        # Log to file
        log_line = f"{timestamp} | {url} | {status} | {memory_usage['process_memory_mb']:.2f}MB | {memory_usage['system_memory_percent']:.1f}%"
        if error:
            log_line += f" | ERROR: {error}"
        
        self.logger.info(log_line)
        
        # Also write to dedicated scraped URLs file
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_line + '\n')
    
    def force_memory_cleanup(self) -> Dict[str, Any]:
        """Force memory cleanup and return cleanup results."""
        initial_memory = self.get_memory_usage()
        initial_mb = initial_memory['process_memory_mb']
        
        # Force garbage collection
        collected = gc.collect()
        
        # Force another collection cycle
        collected += gc.collect()
        
        # Get memory after cleanup
        final_memory = self.get_memory_usage()
        final_mb = final_memory['process_memory_mb']
        
        memory_freed = initial_mb - final_mb
        
        cleanup_result = {
            'initial_memory_mb': initial_mb,
            'final_memory_mb': final_mb,
            'memory_freed_mb': memory_freed,
            'garbage_collected': collected,
            'timestamp': datetime.now().isoformat()
        }
        
        if memory_freed > 0:
            self.logger.info(f"Memory cleanup: Freed {memory_freed:.2f}MB, collected {collected} objects")
        else:
            self.logger.info(f"Memory cleanup: No memory freed, collected {collected} objects")
        
        return cleanup_result
    
    def get_memory_summary(self) -> Dict[str, Any]:
        """Get a summary of memory usage patterns."""
        if not self.memory_history:
            return {'error': 'No memory history available'}
        
        memory_values = [entry['process_memory_mb'] for entry in self.memory_history]
        
        return {
            'current_memory_mb': self.get_memory_usage()['process_memory_mb'],
            'max_memory_mb': max(memory_values),
            'min_memory_mb': min(memory_values),
            'avg_memory_mb': sum(memory_values) / len(memory_values),
            'memory_history_count': len(self.memory_history),
            'threshold_warnings': len([entry for entry in self.memory_history 
                                    if entry['process_memory_mb'] > self.memory_threshold_mb])
        }
    
    def log_memory_summary(self) -> None:
        """Log a summary of current memory status."""
        summary = self.get_memory_summary()
        self.logger.info(f"Memory Summary: Current={summary['current_memory_mb']:.2f}MB, "
                        f"Max={summary['max_memory_mb']:.2f}MB, "
                        f"Avg={summary['avg_memory_mb']:.2f}MB, "
                        f"Warnings={summary['threshold_warnings']}")
