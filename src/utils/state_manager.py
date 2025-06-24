import pickle
import os
from datetime import datetime
from typing import Set, Dict, Optional, Tuple
from src.config import DATA_FILE, NEXT_CRAWL_FILE, SCANNED_PAGES_FILE, TARGET_URLS

__all__ = ['StateManager']

class StateManager:
    def __init__(self):
        self.visited_urls: Set[str] = set()
        self.remaining_urls: Set[str] = set(TARGET_URLS)
        self.next_crawl: Dict[str, datetime] = {}
        self.load_progress()

    def load_progress(self) -> None:
        """Load saved crawl progress from files."""
        try:
            if os.path.exists(DATA_FILE):
                with open(DATA_FILE, "rb") as f:
                    self.visited_urls, self.remaining_urls = pickle.load(f)
                if not self.remaining_urls:
                    self.remaining_urls.update(TARGET_URLS)
                    print("\nNo remaining URLs found. Resetting crawl.")
                else:
                    print("\nResuming from previous session...")

            if os.path.exists(NEXT_CRAWL_FILE):
                with open(NEXT_CRAWL_FILE, "rb") as f:
                    self.next_crawl = pickle.load(f)
        except Exception as e:
            print(f"\nError loading progress: {e}")
            # Reset state if loading fails
            self.visited_urls = set()
            self.remaining_urls = set(TARGET_URLS)
            self.next_crawl = {}

    def save_progress(self) -> None:
        """Save current crawl progress to files."""
        try:
            with open(DATA_FILE, "wb") as f:
                pickle.dump((self.visited_urls, self.remaining_urls), f)
            with open(NEXT_CRAWL_FILE, "wb") as f:
                pickle.dump(self.next_crawl, f)
            print("\nProgress saved.")
        except Exception as e:
            print(f"\nError saving progress: {e}")

    def log_scanned_page(self, page_url: str) -> None:
        """Log scanned pages to a file with timestamp."""
        try:
            with open(SCANNED_PAGES_FILE, "a", encoding="utf-8") as f:
                f.write(f"{datetime.now()} - {page_url}\n")
        except Exception as e:
            print(f"\nError logging scanned page: {e}")

    def was_visited(self, url: str) -> bool:
        """Check if a URL has been visited before."""
        return url in self.visited_urls

    def add_visited_url(self, url: str) -> None:
        """Add URL to visited set and update next crawl time."""
        self.visited_urls.add(url)
        self.next_crawl[url] = datetime.now()
        self.save_progress()

    def add_new_urls(self, urls: Set[str]) -> None:
        """Add new URLs to remaining set if not visited."""
        new_urls = urls - self.visited_urls
        self.remaining_urls.update(new_urls)
        self.save_progress()

    def get_next_url(self) -> Optional[str]:
        """Get the next URL to crawl."""
        if not self.remaining_urls:
            # Check for URLs that need recrawling
            now = datetime.now()
            for url, last_crawl in self.next_crawl.items():
                if self.should_recrawl(url):
                    self.remaining_urls.add(url)
            
            if not self.remaining_urls:
                return None

        return self.remaining_urls.pop()

    def should_recrawl(self, url: str, recrawl_days: int = 3) -> bool:
        """Check if a URL should be recrawled based on last crawl time."""
        if url not in self.next_crawl:
            return True
        last_crawl = self.next_crawl[url]
        return (datetime.now() - last_crawl).days >= recrawl_days

    def get_crawl_stats(self) -> Dict[str, int]:
        """Get current crawl statistics."""
        return {
            "visited_urls": len(self.visited_urls),
            "remaining_urls": len(self.remaining_urls),
            "total_known_urls": len(self.visited_urls) + len(self.remaining_urls)
        } 