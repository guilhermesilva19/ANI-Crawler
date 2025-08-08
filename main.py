"""Main entry point for the ANI crawler system."""
import os
import sys

# Add the project root to Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src.core.crawler import Crawler
from datetime import datetime

def main():
    """Main function to run the ANI system."""
    try:
        # Append a startup marker without truncating the log
        try:
            # Try to capture RAM usage
            try:
                import os as _os
                import psutil as _psutil  # type: ignore
                _rss_mb = int(_psutil.Process(_os.getpid()).memory_info().rss / (1024 * 1024))
            except Exception:
                _rss_mb = None

            with open("target_urls.log", "a", encoding="utf-8") as log_file:
                if _rss_mb is not None:
                    log_file.write("Server started--------------> " + datetime.utcnow().isoformat() + f"Z RAM={_rss_mb}MB\n")
                else:
                    log_file.write("Server started--------------> " + datetime.utcnow().isoformat() + "Z\n")
        except Exception:
            # Do not crash if logging fails
            pass

        crawler = Crawler()
        crawler.run()
    except Exception as e:
        # Try to send error notification if possible
        try:
            from src.services.slack_service import SlackService
            slack = SlackService()
            slack.send_error(f"System startup failure: {str(e)}")
        except:
            pass  
        raise

if __name__ == "__main__":
    main()
