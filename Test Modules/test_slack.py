#!/usr/bin/env python3
"""Test script for Slack connectivity."""

from src.services.slack_service import SlackService
from slack_sdk.errors import SlackApiError
import os

def main():
    # Check if token exists
    token = os.getenv('SLACK_TOKEN')
    if not token:
        print("❌ SLACK_TOKEN not found in environment variables")
        print("Please add your bot token to the .env file")
        return
        
    print(f"Token prefix: {token[:10]}...")  # Show token prefix for verification
    
    slack = SlackService()
    print(f"Target channel: {slack.channel}")
    
    # Test message
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "🔄 Test Message"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "This is a test message to verify Slack connectivity."
            }
        }
    ]
    
    try:
        response = slack.send_message(blocks)
        if response:
            print("\n✅ Successfully sent message to Slack!")
            print(f"Message sent to channel: {slack.channel}")
        else:
            print("\n❌ Failed to send message")
            print("Please check your token and channel permissions")
    except SlackApiError as e:
        print("\n❌ Slack API Error:")
        print(f"Error: {e.response['error']}")
        print("\nTroubleshooting steps:")
        print("1. Verify your bot token starts with 'xoxb-'")
        print("2. Check that these scopes are added to your bot:")
        print("   - chat:write")
        print("   - chat:write.public")
        print("   - channels:read")
        print("3. Make sure your bot is invited to the channel")
        print(f"   Type: /invite @YourBotName in #{slack.channel}")
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")

if __name__ == '__main__':
    main() 