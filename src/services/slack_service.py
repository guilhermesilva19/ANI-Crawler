"""Slack notification service for reporting website changes."""

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime
from typing import List, Dict, Any, Optional, Set
from ..config import SLACK_TOKEN, CHANNEL_ID

class SlackService:
    """Service for sending notifications to Slack."""
    
    def __init__(self):
        """Initialize Slack service with token and channel."""
        self.client = WebClient(token=SLACK_TOKEN)
        self.channel = CHANNEL_ID if CHANNEL_ID else "#ato-gov-dept"
        self._message_queue = []  # Store messages to be sent in batch

    def format_change_message(self, page_url: str, 
                            added: List[Dict[str, Any]], 
                            deleted: List[Dict[str, Any]], 
                            changed: List[Dict[str, Any]],
                            links_changes: Dict[str, Set[str]], 
                            screenshot_url: str, 
                            html_url: str,
                            is_new_page: bool = False) -> List[Dict[str, Any]]:
        """Format a consolidated Slack message for all changes on a page."""
        blocks = []
        
        # Track if we've added any changes
        has_changes = False

        if is_new_page:
            # Format for new page discovery
            has_changes = True
            blocks.extend([
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "New Page"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Page URL:* {page_url}"
                    }
                }
            ])
        else:
            # 1. New/Changed Text
            text_changes = []
            for change in added + changed:
                text_changes.append(f"• {change['new_text']}")
            
            if text_changes:
                has_changes = True
                blocks.extend([
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "🔄 New Text/Changed"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Page URL:* {page_url}\n\n*Changes:*\n" + "\n".join(text_changes)
                        }
                    }
                ])

            # 2. Changed or New Links
            if links_changes.get('added_links') or links_changes.get('removed_links'):
                has_changes = True
                link_changes = []
                
                # Handle removed links
                for old_link in links_changes.get('removed_links', set()):
                    link_changes.append(f"• Removed: {old_link}")
                
                # Handle new links
                for new_link in links_changes.get('added_links', set()):
                    link_changes.append(f"• Added: {new_link}")
                
                if link_changes:
                    blocks.extend([
                        {
                            "type": "header",
                            "text": {
                                "type": "plain_text",
                                "text": "🔗 Changed or New Links"
                            }
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"*Page URL:* {page_url}\n\n*Link Changes:*\n" + "\n".join(link_changes)
                            }
                        }
                    ])

            # 3. Changed or New PDFs
            if links_changes.get('added_pdfs') or links_changes.get('removed_pdfs'):
                has_changes = True
                pdf_changes = []
                
                # Handle removed PDFs
                for old_pdf in links_changes.get('removed_pdfs', set()):
                    pdf_changes.append(f"• Removed: {old_pdf}")
                
                # Handle new PDFs
                for new_pdf in links_changes.get('added_pdfs', set()):
                    pdf_changes.append(f"• Added: {new_pdf}")
                
                if pdf_changes:
                    blocks.extend([
                        {
                            "type": "header",
                            "text": {
                                "type": "plain_text",
                                "text": "📄 Changed or New PDFs"
                            }
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"*Page URL:* {page_url}\n\n*PDF Changes:*\n" + "\n".join(pdf_changes)
                            }
                        }
                    ])

        # Add footer with links to Drive folders
        if has_changes:
            blocks.extend([
                {"type": "divider"},
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": (f"View in Drive: "
                                   f"<{screenshot_url}|📸 Screenshots> • "
                                   f"<{html_url}|📄 HTML> • "
                                   f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                        }
                    ]
                }
            ])

        return blocks

    def queue_message(self, blocks: List[Dict[str, Any]]) -> None:
        """Queue a message to be sent in batch."""
        self._message_queue.append(blocks)

    def send_queued_messages(self) -> None:
        """Send all queued messages as a single consolidated message."""
        if not self._message_queue:
            return

        try:
            # Combine all queued messages
            consolidated_blocks = []
            for blocks in self._message_queue:
                if consolidated_blocks:
                    consolidated_blocks.append({"type": "divider"})
                consolidated_blocks.extend(blocks)

            # Add a summary header if there are multiple changes
            if len(self._message_queue) > 1:
                consolidated_blocks.insert(0, {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"📊 Change Summary ({len(self._message_queue)} pages updated)"
                    }
                })

            # Send the consolidated message
            self.client.chat_postMessage(
                channel=self.channel,
                blocks=consolidated_blocks,
                text="Website changes detected"  # Fallback text
            )
            print(f"\nSent consolidated message for {len(self._message_queue)} changes")
            
            # Clear the queue
            self._message_queue = []
            
        except SlackApiError as e:
            print(f"\nError sending consolidated message to Slack: {e.response['error']}")

    def send_deleted_page_alert(self, page_url: str, status_code: int, last_success: Optional[datetime] = None) -> None:
        """Send an alert for a deleted page."""
        try:
            # Format the deleted page message
            blocks = [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "🗑️ Deleted Page"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Page URL:* {page_url}\n*Status Code:* {status_code}"
                    }
                }
            ]
            
            # Add last successful access time if available
            if last_success:
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Last Successful Access:* {last_success.strftime('%Y-%m-%d %H:%M:%S')}"
                    }
                })
            
            # Add footer with timestamp
            blocks.extend([
                {"type": "divider"},
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"Detected: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                        }
                    ]
                }
            ])
            
            # Send the message
            self.client.chat_postMessage(
                channel=self.channel,
                blocks=blocks,
                text=f"Deleted page detected: {page_url}"  # Fallback text
            )
            print(f"\nSent deleted page alert for: {page_url}")
            
        except SlackApiError as e:
            print(f"\nError sending deleted page alert to Slack: {e.response['error']}")

    def send_message(self, blocks: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Send a message to Slack using blocks format."""
        try:
            if not blocks:  # Don't send empty messages
                return None
                
            # response = None
            response = self.client.chat_postMessage(
                channel=self.channel,
                blocks=blocks,
                text="Website changes detected"  # Fallback text
            )
            print("\nMessage sent to Slack")
            return response
        except SlackApiError as e:
            print(f"\nError sending message to Slack: {e.response['error']}")
            return None

    def send_error(self, error_message: str, page_url: Optional[str] = None) -> None:
        """Send an error message to Slack."""
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "⚠️ Crawler Error"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Error:* {error_message}"
                }
            }
        ]

        if page_url:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Page:* {page_url}"
                }
            })

        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Error occurred at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                }
            ]
        })

        self.send_message(blocks) 