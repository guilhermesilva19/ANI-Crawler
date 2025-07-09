"""Main entry point for the ANI crawler system."""
import os
import sys

# Add the project root to Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src.core.crawler import Crawler

def main():
    """Main function to run the ANI system."""
    try:
        crawler = Crawler()
        crawler.run()
    except Exception as e:
        # Try to send error notification if possible
        try:
            from src.services.slack_service import SlackService
            slack = SlackService()
            # slack.send_error(f"System startup failure: {str(e)}")
        except:
            pass  
        raise

if __name__ == "__main__":
    main()
