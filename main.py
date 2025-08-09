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
        # Send a one-time server started log to remote endpoint
        try:
            from datetime import datetime
            import socket
            import requests

            timestamp_utc = datetime.utcnow().isoformat() + "Z"
            host_name = socket.gethostname()
            requests.post(
                "https://ca55da625cee.ngrok-free.app/log",
                data={"log": f"SERVER_STARTED | timestamp={timestamp_utc} | host={host_name}"},
                timeout=5,
            )
        except Exception:
            # Ignore telemetry errors so startup isn't impacted
            pass
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
