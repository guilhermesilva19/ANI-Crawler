"""
Robust HTML page detection and filtering utility.
Super clean, accurate, and modular approach to filter out non-HTML content.
"""

import re
from typing import Set, List, Tuple
from urllib.parse import urlparse

__all__ = ['HTMLPageFilter']


class HTMLPageFilter:
    """
    Powerful and accurate HTML page detection filter.
    Filters out PDFs, documents, media files, and other non-HTML content.
    """
    
    # File extensions that are definitely not HTML pages
    DOCUMENT_EXTENSIONS = {
        # Documents
        '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', 
        '.txt', '.rtf', '.csv', '.odt', '.ods', '.odp',
        # Archives
        '.zip', '.rar', '.7z', '.tar', '.gz', '.bz2',
        # Media
        '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp',
        '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.mp3', 
        '.wav', '.flac', '.aac', '.ogg',
        # Code/Data
        '.xml', '.json', '.js', '.css', '.rss', '.atom'
    }
    
    # Path patterns that typically contain documents/files
    DOCUMENT_PATH_PATTERNS = {
        '/download/', '/downloads/', '/files/', '/attachments/', '/assets/',
        '/documents/', '/media/', '/uploads/', '/file/', '/attachment/'
    }
    
    # URL patterns that indicate non-HTML content (without file extensions)
    NON_HTML_URL_PATTERNS = {
        '/pdf', '/doc', '/docx', '/xls', '/xlsx', '/ppt', '/pptx',
        '/download', '/file', '/attachment', '/media', '/asset'
    }
    
    def __init__(self):
        """Initialize the HTML page filter."""
        # Compile regex patterns for efficiency
        self._extension_pattern = self._compile_extension_pattern()
        self._path_pattern = self._compile_path_pattern()
        self._url_pattern = self._compile_url_pattern()
    
    def _compile_extension_pattern(self) -> re.Pattern:
        """Compile regex pattern for file extensions."""
        # Match extensions at end of URL or followed by query params
        extensions = '|'.join(re.escape(ext[1:]) for ext in self.DOCUMENT_EXTENSIONS)
        return re.compile(f'\.({extensions})(?:\?.*)?(?:#.*)?$', re.IGNORECASE)
    
    def _compile_path_pattern(self) -> re.Pattern:
        """Compile regex pattern for document paths."""
        patterns = '|'.join(re.escape(pattern) for pattern in self.DOCUMENT_PATH_PATTERNS)
        return re.compile(f'({patterns})', re.IGNORECASE)
    
    def _compile_url_pattern(self) -> re.Pattern:
        """Compile regex pattern for non-HTML URL endings."""
        patterns = '|'.join(re.escape(pattern) for pattern in self.NON_HTML_URL_PATTERNS)
        return re.compile(f'({patterns})(?:/.*)?(?:\?.*)?(?:#.*)?$', re.IGNORECASE)
    
    def is_html_page(self, url: str) -> bool:
        """
        Determine if a URL points to an HTML page.
        
        Args:
            url: The URL to check
            
        Returns:
            True if the URL appears to be an HTML page, False otherwise
        """
        if not url or not isinstance(url, str):
            return False
        
        # Clean the URL
        url = url.strip().rstrip('/')
        if not url:
            return False
        
        # Parse URL for more thorough analysis
        try:
            parsed = urlparse(url)
            path = parsed.path.lower()
            
            # Check for file extensions
            if self._extension_pattern.search(url):
                return False
            
            # Check for document path patterns
            if self._path_pattern.search(path):
                return False
            
            # Check for non-HTML URL patterns
            if self._url_pattern.search(path):
                return False
            
            # Additional specific checks for edge cases
            if self._has_suspicious_patterns(url, path):
                return False
            
            # If none of the non-HTML patterns match, assume it's an HTML page
            return True
            
        except Exception:
            # If URL parsing fails, err on the side of caution
            return False
    
    def _has_suspicious_patterns(self, url: str, path: str) -> bool:
        """Check for additional suspicious patterns that indicate non-HTML content."""
        url_lower = url.lower()
        
        # Check for inline file extensions (e.g., "document.pdf/view")
        if re.search(r'\.(?:pdf|doc|docx|xls|xlsx|ppt|pptx)/', url_lower):
            return True
        
        # Check for query parameters that suggest file downloads
        if re.search(r'[?&](?:download|file|attachment)=', url_lower):
            return True
        
        # Check for common file serving patterns
        if re.search(r'/(?:serve|get|fetch)(?:file|document|attachment)', path):
            return True
        
        # Check for version/revision patterns that often contain documents
        if re.search(r'/(?:v\d+|version\d+|rev\d+)/.*\.', url_lower):
            return True
        
        return False
    
    def filter_html_urls(self, urls: Set[str]) -> Tuple[Set[str], Set[str]]:
        """
        Filter a set of URLs into HTML pages and non-HTML files.
        
        Args:
            urls: Set of URLs to filter
            
        Returns:
            Tuple of (html_urls, non_html_urls)
        """
        html_urls = set()
        non_html_urls = set()
        
        for url in urls:
            if self.is_html_page(url):
                html_urls.add(url)
            else:
                non_html_urls.add(url)
        
        return html_urls, non_html_urls
    
    def get_non_html_reason(self, url: str) -> str:
        """
        Get a human-readable reason why a URL was classified as non-HTML.
        Useful for debugging and logging.
        
        Args:
            url: The URL to analyze
            
        Returns:
            String describing why the URL is not considered an HTML page
        """
        if not url or not isinstance(url, str):
            return "Invalid URL"
        
        url = url.strip().rstrip('/')
        if not url:
            return "Empty URL"
        
        try:
            parsed = urlparse(url)
            path = parsed.path.lower()
            url_lower = url.lower()
            
            # Check file extensions
            ext_match = self._extension_pattern.search(url)
            if ext_match:
                return f"File extension: .{ext_match.group(1)}"
            
            # Check path patterns
            path_match = self._path_pattern.search(path)
            if path_match:
                return f"Document path: {path_match.group(1)}"
            
            # Check URL patterns
            url_match = self._url_pattern.search(path)
            if url_match:
                return f"Non-HTML URL pattern: {url_match.group(1)}"
            
            # Check additional patterns
            if re.search(r'\.(?:pdf|doc|docx|xls|xlsx|ppt|pptx)/', url_lower):
                return "Inline file extension in path"
            
            if re.search(r'[?&](?:download|file|attachment)=', url_lower):
                return "Download parameter in query string"
            
            if re.search(r'/(?:serve|get|fetch)(?:file|document|attachment)', path):
                return "File serving endpoint"
            
            return "Appears to be HTML page"
            
        except Exception as e:
            return f"URL parsing error: {e}"
    
    def get_filter_stats(self, urls: Set[str]) -> dict:
        """
        Get statistics about URL filtering.
        
        Args:
            urls: Set of URLs to analyze
            
        Returns:
            Dictionary with filtering statistics
        """
        html_urls, non_html_urls = self.filter_html_urls(urls)
        
        # Categorize non-HTML URLs by reason
        reasons = {}
        for url in non_html_urls:
            reason = self.get_non_html_reason(url)
            reasons[reason] = reasons.get(reason, 0) + 1
        
        return {
            'total_urls': len(urls),
            'html_pages': len(html_urls),
            'non_html_files': len(non_html_urls),
            'html_percentage': round(len(html_urls) / len(urls) * 100, 1) if urls else 0,
            'non_html_reasons': reasons
        } 