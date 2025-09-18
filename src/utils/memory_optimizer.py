"""Memory-efficient content processing utilities for high-performance crawling."""

import gc
import os
import tempfile
import mmap
from typing import Iterator, Optional, Tuple
from contextlib import contextmanager
from bs4 import BeautifulSoup
from io import StringIO
import weakref

__all__ = ['MemoryEfficientProcessor', 'StreamingContentHandler']


class MemoryEfficientProcessor:
    """Memory-efficient content processing with lazy evaluation and streaming."""
    
    def __init__(self, max_memory_mb: int = 100):
        """Initialize processor with memory limits."""
        self.max_memory_mb = max_memory_mb
        self.temp_files = weakref.WeakSet()  # Track temp files for cleanup
        
    @contextmanager
    def process_large_content(self, content: str):
        """Context manager for processing large content efficiently."""
        temp_file = None
        try:
            # For very large content, use temporary file with memory mapping
            if len(content) > self.max_memory_mb * 1024 * 1024:
                temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, encoding='utf-8')
                temp_file.write(content)
                temp_file.flush()
                self.temp_files.add(temp_file)
                
                with open(temp_file.name, 'r+b') as f:
                    with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mmapped_file:
                        yield self._create_streaming_parser(mmapped_file)
            else:
                # For smaller content, process in memory with streaming parser
                yield self._create_streaming_parser(StringIO(content))
                
        finally:
            # Cleanup temporary file
            if temp_file:
                try:
                    os.unlink(temp_file.name)
                except OSError:
                    pass
            
            # Force garbage collection
            gc.collect()
    
    def _create_streaming_parser(self, content_stream):
        """Create streaming BeautifulSoup parser."""
        # Use lxml parser for better memory efficiency
        try:
            return BeautifulSoup(content_stream, 'lxml')
        except:
            # Fallback to html.parser
            return BeautifulSoup(content_stream, 'html.parser')
    
    def extract_text_chunks(self, soup: BeautifulSoup, chunk_size: int = 1000) -> Iterator[str]:
        """Extract text in chunks to reduce memory usage."""
        current_chunk = []
        current_size = 0
        
        # Extract text element by element
        for element in soup.find_all(text=True):
            text = element.strip()
            if not text:
                continue
                
            current_chunk.append(text)
            current_size += len(text)
            
            if current_size >= chunk_size:
                yield ' '.join(current_chunk)
                current_chunk = []
                current_size = 0
        
        # Yield remaining chunk
        if current_chunk:
            yield ' '.join(current_chunk)
    
    def extract_links_streaming(self, soup: BeautifulSoup, base_url: str) -> Iterator[str]:
        """Extract links using streaming approach to reduce memory usage."""
        for link in soup.find_all('a', href=True):
            href = link.get('href', '').strip()
            if href:
                # Process link immediately instead of storing
                full_url = self._resolve_url(base_url, href)
                if full_url:
                    yield full_url
                
                # Clear processed element to free memory
                link.decompose()
    
    def _resolve_url(self, base_url: str, href: str) -> Optional[str]:
        """Resolve relative URLs efficiently."""
        from urllib.parse import urljoin, urlparse
        
        try:
            full_url = urljoin(base_url, href)
            parsed = urlparse(full_url)
            
            # Basic validation
            if parsed.scheme in ('http', 'https') and parsed.netloc:
                return full_url.split('#')[0]  # Remove fragment
        except Exception:
            pass
        
        return None


class StreamingContentHandler:
    """High-performance streaming content handler for file operations."""
    
    def __init__(self, buffer_size: int = 8192):
        """Initialize with optimal buffer size."""
        self.buffer_size = buffer_size
    
    def stream_to_file(self, content: str, file_path: str) -> bool:
        """Stream content to file efficiently."""
        try:
            with open(file_path, 'w', encoding='utf-8', buffering=self.buffer_size) as f:
                # Write in chunks to reduce memory usage
                for i in range(0, len(content), self.buffer_size):
                    chunk = content[i:i + self.buffer_size]
                    f.write(chunk)
                    
                    # Periodic garbage collection for large files
                    if i % (self.buffer_size * 10) == 0:
                        gc.collect()
            
            return True
        except Exception as e:
            print(f"❌ Error streaming to file {file_path}: {e}")
            return False
    
    def compare_files_streaming(self, file1: str, file2: str) -> Tuple[bool, float]:
        """Compare files using streaming approach for memory efficiency."""
        try:
            total_chunks = 0
            different_chunks = 0
            
            with open(file1, 'r', encoding='utf-8') as f1, \
                 open(file2, 'r', encoding='utf-8') as f2:
                
                while True:
                    chunk1 = f1.read(self.buffer_size)
                    chunk2 = f2.read(self.buffer_size)
                    
                    if not chunk1 and not chunk2:
                        break  # Both files ended
                    
                    total_chunks += 1
                    if chunk1 != chunk2:
                        different_chunks += 1
            
            if total_chunks == 0:
                return True, 1.0  # Both files are empty
            
            similarity = 1.0 - (different_chunks / total_chunks)
            are_similar = similarity > 0.9  # 90% similarity threshold
            
            return are_similar, similarity
            
        except Exception as e:
            print(f"❌ Error comparing files: {e}")
            return False, 0.0
    
    def get_file_hash_streaming(self, file_path: str) -> Optional[str]:
        """Calculate file hash using streaming approach."""
        import hashlib
        
        try:
            hasher = hashlib.md5()
            with open(file_path, 'rb') as f:
                while chunk := f.read(self.buffer_size):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except Exception as e:
            print(f"❌ Error calculating hash for {file_path}: {e}")
            return None


class MemoryMonitor:
    """Memory usage monitoring and optimization."""
    
    def __init__(self, warning_threshold_mb: int = 400, critical_threshold_mb: int = 500):
        """Initialize memory monitor with thresholds."""
        self.warning_threshold = warning_threshold_mb * 1024 * 1024  # Convert to bytes
        self.critical_threshold = critical_threshold_mb * 1024 * 1024
        self.gc_count = 0
        
    def check_memory_usage(self) -> Tuple[bool, float, str]:
        """Check current memory usage and return status."""
        try:
            import psutil
            process = psutil.Process(os.getpid())
            memory_bytes = process.memory_info().rss
            memory_mb = memory_bytes / (1024 * 1024)
            
            if memory_bytes > self.critical_threshold:
                return False, memory_mb, "CRITICAL"
            elif memory_bytes > self.warning_threshold:
                return True, memory_mb, "WARNING"
            else:
                return True, memory_mb, "OK"
                
        except ImportError:
            return True, 0.0, "UNKNOWN"
        except Exception as e:
            print(f"⚠️  Memory check error: {e}")
            return True, 0.0, "ERROR"
    
    def optimize_memory(self, force: bool = False) -> bool:
        """Optimize memory usage with garbage collection."""
        is_ok, memory_mb, status = self.check_memory_usage()
        
        if not is_ok or force:
            print(f"🧠 Memory optimization triggered: {memory_mb:.1f}MB ({status})")
            
            # Force garbage collection
            collected = gc.collect()
            self.gc_count += 1
            
            # Get memory after cleanup
            _, new_memory_mb, _ = self.check_memory_usage()
            freed_mb = memory_mb - new_memory_mb
            
            print(f"♻️  GC completed: freed {freed_mb:.1f}MB, collected {collected} objects")
            return True
        
        return False
    
    def get_memory_stats(self) -> dict:
        """Get comprehensive memory statistics."""
        is_ok, memory_mb, status = self.check_memory_usage()
        
        return {
            'current_memory_mb': memory_mb,
            'status': status,
            'is_healthy': is_ok,
            'gc_count': self.gc_count,
            'warning_threshold_mb': self.warning_threshold / (1024 * 1024),
            'critical_threshold_mb': self.critical_threshold / (1024 * 1024)
        }
