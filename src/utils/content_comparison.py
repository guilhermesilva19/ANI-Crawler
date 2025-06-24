from bs4 import BeautifulSoup
from difflib import ndiff, SequenceMatcher
import re
from typing import List, Tuple, Set, Optional, Dict, Any
from urllib.parse import urlparse, urljoin

__all__ = ['compare_content','extract_links']

def filter_dynamic_content(text: str) -> str:
    """Remove or normalize dynamic content that shouldn't trigger change detection."""
    # Normalize dates and times
    text = re.sub(r'\b\d{1,2}/\d{1,2}/\d{2,4}\b', 'DATE', text)
    text = re.sub(r'\b\d{1,2}:\d{2}(?::\d{2})?\s*(?:AM|PM|am|pm)?\b', 'TIME', text)
    
    # Remove session IDs and other common dynamic parameters
    text = re.sub(r'(?i)sessionid=[a-z0-9-]+', 'sessionid=REMOVED', text)
    text = re.sub(r'(?i)token=[a-z0-9-]+', 'token=REMOVED', text)
    
    # Remove timestamp patterns
    text = re.sub(r'\b\d{13}\b', 'TIMESTAMP', text)  # Unix timestamp
    
    # Remove dynamic classes that often change
    text = re.sub(r'class="[^"]*"', 'class="NORMALIZED"', text)
    
    return text

def normalize_html_whitespace(html_content: str) -> str:
    """Normalize HTML whitespace and formatting to reduce false positives."""
    # Remove extra whitespace
    html_content = re.sub(r'\s+', ' ', html_content)
    
    # Normalize line endings
    html_content = html_content.replace('\r\n', '\n').replace('\r', '\n')
    
    # Remove whitespace between tags
    html_content = re.sub(r'>\s+<', '><', html_content)
    
    return html_content.strip()

def extract_visible_text(html_content: str) -> List[str]:
    """Extract visible text content from HTML."""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script and style elements
        for element in soup(['script', 'style', 'head', 'title', 'meta', '[document]']):
            element.decompose()

        # Get text and normalize whitespace
        text = soup.get_text(separator='\n')
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        
        return lines
    except Exception as e:
        print(f"\nError extracting visible text: {e}")
        return []

def is_meaningful_change(old_text: str, new_text: str, threshold: float = 0.3) -> bool:
    """
    Determine if a change is meaningful based on content similarity.
    Returns True if the change is significant enough.
    """
    # If either text is empty, consider it meaningful only if the other has content
    if not old_text or not new_text:
        return bool(old_text) != bool(new_text)
    
    # Calculate similarity ratio
    similarity = SequenceMatcher(None, old_text, new_text).ratio()
    return similarity < (1 - threshold)  # If similarity is less than 70%, consider it meaningful

def extract_links(page_url: str, soup: BeautifulSoup, check_prefix: Optional[str] = None) -> Set[str]:
    """Extract all internal links from a page."""
    domain = urlparse(page_url).netloc
    links = set()
    
    for link in soup.find_all("a", href=True):
        href = link["href"].strip()
        full_url = urljoin(page_url, href)
        parsed_url = urlparse(full_url)
        
        # Skip fragment identifiers
        if '#' in full_url:
            continue
            
        # Skip external links
        if parsed_url.netloc != domain:
            continue
            
        # Skip specific prefixes if provided
        if check_prefix and full_url.startswith(check_prefix):
            continue
            
        links.add(full_url)
        
    return links

def compare_content(old_content: str, new_content: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Compare two versions of content and return meaningful changes."""
    added = []
    deleted = []
    changed = []

    try:
        # Normalize content
        old_content = normalize_html_whitespace(old_content)
        new_content = normalize_html_whitespace(new_content)

        # Extract and normalize text
        old_lines = extract_visible_text(old_content)
        new_lines = extract_visible_text(new_content)

        # Filter dynamic content
        old_lines = [filter_dynamic_content(line) for line in old_lines]
        new_lines = [filter_dynamic_content(line) for line in new_lines]

        # Compare lines
        diff = list(ndiff(old_lines, new_lines))
        
        i = 0
        while i < len(diff):
            line = diff[i]
            if line.startswith('+ '):
                new_text = line[2:]
                if any(is_meaningful_change(old, new_text) for old in old_lines):
                    added.append({'new_text': new_text})
            elif line.startswith('- '):
                old_text = line[2:]
                if i + 1 < len(diff) and diff[i + 1].startswith('+ '):
                    new_text = diff[i + 1][2:]
                    if is_meaningful_change(old_text, new_text):
                        changed.append({'new_text': f"Changed from '{old_text}' to '{new_text}'"})
                    i += 1
                else:
                    if any(is_meaningful_change(old_text, new) for new in new_lines):
                        deleted.append({'new_text': f"Deleted: {old_text}"})
            i += 1

    except Exception as e:
        print(f"\nError comparing content: {e}")

    return added, deleted, changed 