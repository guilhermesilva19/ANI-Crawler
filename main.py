"""Main entry point for the ANI crawler system."""
import os
import sys

# Add the project root to Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src.core.crawler import Crawler

def main():
    """Main function to run the ANI system."""
    crawler = Crawler()
    crawler.run()

if __name__ == "__main__":
    main()
