"""High-performance browser connection pool for web crawling optimization."""

import threading
import time
import queue
from typing import Optional, List
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta

from .browser_service import BrowserService

__all__ = ['BrowserPool', 'PooledBrowserService']


@dataclass
class BrowserInstance:
    """Represents a browser instance in the pool."""
    browser: BrowserService
    created_at: datetime
    last_used: datetime
    usage_count: int
    is_healthy: bool = True


class BrowserPool:
    """High-performance browser connection pool with automatic lifecycle management."""
    
    def __init__(self, 
                 min_size: int = 2, 
                 max_size: int = 5, 
                 max_age_minutes: int = 30,
                 max_usage_count: int = 100,
                 proxy_options=None):
        """
        Initialize browser pool with intelligent resource management.
        
        Args:
            min_size: Minimum number of browsers to maintain
            max_size: Maximum number of browsers allowed
            max_age_minutes: Maximum age before browser is recycled
            max_usage_count: Maximum uses before browser is recycled
            proxy_options: Proxy configuration for browsers
        """
        self.min_size = min_size
        self.max_size = max_size
        self.max_age = timedelta(minutes=max_age_minutes)
        self.max_usage_count = max_usage_count
        self.proxy_options = proxy_options
        
        # Thread-safe pool management
        self._pool = queue.Queue(maxsize=max_size)
        self._active_browsers: List[BrowserInstance] = []
        self._lock = threading.RLock()
        self._total_created = 0
        self._total_reused = 0
        
        # Performance metrics
        self._stats = {
            'created': 0,
            'reused': 0,
            'recycled': 0,
            'failed': 0,
            'avg_creation_time': 0.0
        }
        
        # Initialize minimum pool size
        self._initialize_pool()
        
        # Start cleanup thread
        self._cleanup_thread = threading.Thread(target=self._cleanup_worker, daemon=True)
        self._cleanup_thread.start()
        
        print(f"🏊 Browser pool initialized: {min_size}-{max_size} instances")
    
    def _initialize_pool(self) -> None:
        """Initialize the pool with minimum number of browsers."""
        successful_browsers = 0
        print(f"🔄 Initializing browser pool with {self.min_size} minimum browsers...")
        
        for i in range(self.min_size):
            print(f"🌐 Creating browser {i+1}/{self.min_size}...")
            browser_instance = self._create_browser_instance()
            if browser_instance:
                self._pool.put(browser_instance)
                successful_browsers += 1
                print(f"✅ Browser {i+1} created successfully")
            else:
                print(f"❌ Failed to create browser {i+1}")
        
        if successful_browsers == 0:
            print("💥 CRITICAL: No browsers could be created! Pool is empty!")
            raise Exception(f"Browser pool initialization failed - 0/{self.min_size} browsers created")
        elif successful_browsers < self.min_size:
            print(f"⚠️  WARNING: Only {successful_browsers}/{self.min_size} browsers created")
        else:
            print(f"🎉 Browser pool ready! {successful_browsers}/{self.min_size} browsers active")
    
    def _create_browser_instance(self) -> Optional[BrowserInstance]:
        """Create a new browser instance with performance tracking."""
        start_time = time.time()
        try:
            print("🔄 Creating new browser service...")
            browser = BrowserService(self.proxy_options)
            print("✅ Browser service created, getting browser instance...")
            
            # Test browser creation
            test_browser = browser.get_browser()
            if not test_browser:
                raise Exception("Browser service returned None")
            
            print("✅ Browser instance obtained successfully")
            creation_time = time.time() - start_time
            
            # Update performance stats
            self._stats['created'] += 1
            self._stats['avg_creation_time'] = (
                (self._stats['avg_creation_time'] * (self._stats['created'] - 1) + creation_time) 
                / self._stats['created']
            )
            
            instance = BrowserInstance(
                browser=test_browser,  # Use the actual driver, not the service
                created_at=datetime.now(),
                last_used=datetime.now(),
                usage_count=0,
                is_healthy=True
            )
            
            with self._lock:
                self._active_browsers.append(instance)
            
            print(f"🌐 New browser created in {creation_time:.2f}s (total: {self._stats['created']})")
            return instance
            
        except Exception as e:
            self._stats['failed'] += 1
            print(f"❌ Failed to create browser: {e}")
            print(f"❌ Browser creation error details: {type(e).__name__}")
            import traceback
            traceback.print_exc()
            return None
    
    def _is_browser_expired(self, instance: BrowserInstance) -> bool:
        """Check if browser instance should be recycled."""
        age = datetime.now() - instance.created_at
        return (
            not instance.is_healthy or
            age > self.max_age or
            instance.usage_count >= self.max_usage_count
        )
    
    def _cleanup_worker(self) -> None:
        """Background worker to clean up expired browsers."""
        while True:
            try:
                time.sleep(60)  # Check every minute
                self._cleanup_expired_browsers()
            except Exception as e:
                print(f"⚠️  Browser cleanup error: {e}")
    
    def _cleanup_expired_browsers(self) -> None:
        """Remove expired browsers from the pool."""
        with self._lock:
            active_count = len(self._active_browsers)
            expired_browsers = []
            
            # Identify expired browsers
            for instance in self._active_browsers[:]:
                if self._is_browser_expired(instance) and active_count > self.min_size:
                    expired_browsers.append(instance)
                    self._active_browsers.remove(instance)
                    active_count -= 1
            
            # Clean up expired browsers
            for instance in expired_browsers:
                try:
                    instance.browser.quit()
                    self._stats['recycled'] += 1
                    print(f"♻️  Browser recycled (age: {datetime.now() - instance.created_at}, uses: {instance.usage_count})")
                except Exception as e:
                    print(f"⚠️  Error cleaning up browser: {e}")
            
            # Ensure minimum pool size
            while len(self._active_browsers) < self.min_size:
                new_instance = self._create_browser_instance()
                if not new_instance:
                    break
    
    @contextmanager
    def get_browser(self):
        """
        Get a browser from the pool with automatic return.
        
        Usage:
            with browser_pool.get_browser() as browser:
                browser.get_page(url)
        """
        browser_instance = None
        try:
            # Try to get from pool first
            try:
                browser_instance = self._pool.get_nowait()
                self._stats['reused'] += 1
                print(f"🔄 Browser reused (pool size: {self._pool.qsize()})")
            except queue.Empty:
                # Pool empty, create new browser if under limit
                with self._lock:
                    if len(self._active_browsers) < self.max_size:
                        browser_instance = self._create_browser_instance()
                    else:
                        # Wait for available browser
                        browser_instance = self._pool.get(timeout=30)
                        self._stats['reused'] += 1
            
            if not browser_instance:
                raise Exception("No browser available")
            
            # Update usage stats
            browser_instance.last_used = datetime.now()
            browser_instance.usage_count += 1
            
            yield browser_instance.browser
            
        except Exception as e:
            print(f"❌ Browser pool error: {e}")
            # Mark browser as unhealthy if there was an error
            if browser_instance:
                browser_instance.is_healthy = False
            raise
        finally:
            # Return browser to pool if still healthy
            if browser_instance and browser_instance.is_healthy and not self._is_browser_expired(browser_instance):
                try:
                    self._pool.put_nowait(browser_instance)
                except queue.Full:
                    # Pool is full, terminate this browser
                    try:
                        browser_instance.browser.quit()
                        with self._lock:
                            self._active_browsers.remove(browser_instance)
                    except Exception:
                        pass
    
    def get_browser_direct(self):
        """
        Get a browser directly (non-context manager) for compatibility.
        Must call return_browser() when done.
        """
        browser_instance = None
        try:
            # Try to get from pool first
            try:
                browser_instance = self._pool.get_nowait()
                self._stats['reused'] += 1
                print(f"🔄 Browser reused (pool size: {self._pool.qsize()})")
            except queue.Empty:
                # Pool empty, create new browser if under limit
                with self._lock:
                    if len(self._active_browsers) < self.max_size:
                        browser_instance = self._create_browser_instance()
                    else:
                        # Wait for available browser
                        browser_instance = self._pool.get(timeout=30)
                        self._stats['reused'] += 1
            
            if not browser_instance:
                raise Exception("No browser available")
            
            # Update usage stats
            browser_instance.last_used = datetime.now()
            browser_instance.usage_count += 1
            
            # Store reference for return_browser
            browser_instance.browser._pool_instance = browser_instance
            
            return browser_instance.browser
            
        except Exception as e:
            print(f"❌ Browser pool error: {e}")
            # Mark browser as unhealthy if there was an error
            if browser_instance:
                browser_instance.is_healthy = False
            raise
    
    def return_browser(self, browser):
        """Return a browser to the pool."""
        try:
            if hasattr(browser, '_pool_instance'):
                browser_instance = browser._pool_instance
                
                # Return browser to pool if still healthy
                if browser_instance.is_healthy and not self._is_browser_expired(browser_instance):
                    try:
                        self._pool.put_nowait(browser_instance)
                        print(f"🔄 Browser returned to pool (size: {self._pool.qsize() + 1})")
                    except queue.Full:
                        # Pool is full, terminate this browser
                        try:
                            browser_instance.browser.quit()
                            with self._lock:
                                if browser_instance in self._active_browsers:
                                    self._active_browsers.remove(browser_instance)
                        except Exception:
                            pass
                else:
                    # Remove unhealthy/expired browser
                    try:
                        browser_instance.browser.quit()
                        with self._lock:
                            if browser_instance in self._active_browsers:
                                self._active_browsers.remove(browser_instance)
                    except Exception:
                        pass
            else:
                # Browser not from pool, just quit it
                try:
                    browser.quit()
                except Exception:
                    pass
                    
        except Exception as e:
            print(f"⚠️  Error returning browser to pool: {e}")
            try:
                browser.quit()
            except Exception:
                pass
    
    def get_stats(self) -> dict:
        """Get pool performance statistics."""
        with self._lock:
            pool_size = self._pool.qsize()
            active_count = len(self._active_browsers)
            
        return {
            'pool_size': pool_size,
            'active_browsers': active_count,
            'total_created': self._stats['created'],
            'total_reused': self._stats['reused'],
            'total_recycled': self._stats['recycled'],
            'failed_creations': self._stats['failed'],
            'avg_creation_time': self._stats['avg_creation_time'],
            'reuse_ratio': self._stats['reused'] / max(1, self._stats['created'] + self._stats['reused'])
        }
    
    def shutdown(self) -> None:
        """Shutdown all browsers in the pool."""
        print("🛑 Shutting down browser pool...")
        
        with self._lock:
            # Close all browsers in pool
            while not self._pool.empty():
                try:
                    instance = self._pool.get_nowait()
                    instance.browser.quit()
                except Exception:
                    pass
            
            # Close all active browsers
            for instance in self._active_browsers:
                try:
                    instance.browser.quit()
                except Exception:
                    pass
            
            self._active_browsers.clear()
        
        print("✅ Browser pool shutdown complete")
    
    def cleanup(self) -> None:
        """Clean up all browser pool resources (alias for shutdown)."""
        self.shutdown()


class PooledBrowserService:
    """Wrapper to make browser pool usage seamless with existing code."""
    
    def __init__(self, browser_pool: BrowserPool):
        self.browser_pool = browser_pool
    
    def get_page_with_pool(self, url: str):
        """Get page using pooled browser."""
        with self.browser_pool.get_browser() as browser:
            return browser.get_page(url)
    
    def save_screenshot_with_pool(self, url: str):
        """Save screenshot using pooled browser."""
        with self.browser_pool.get_browser() as browser:
            return browser.save_screenshot(url)
