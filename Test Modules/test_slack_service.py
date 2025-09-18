"""Tests for the enhanced Slack notification system."""

import unittest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
from src.services.slack_service import SlackService

class TestSlackService(unittest.TestCase):
    """Test cases for the enhanced Slack notification system."""

    def setUp(self):
        """Set up test environment."""
        self.slack_service = SlackService()
        self.slack_service.client = Mock()  # Mock the Slack client
        self.test_url = "https://www.education.gov.au/test-page"
        self.test_screenshot_url = "https://drive.google.com/test-screenshot"
        self.test_html_url = "https://drive.google.com/test-html"

    def test_new_page_notification(self):
        """Test notification formatting for new page discovery."""
        blocks = self.slack_service.format_change_message(
            self.test_url, [], [], [], 
            {'added_links': set(), 'removed_links': set(), 'added_pdfs': set(), 'removed_pdfs': set()},
            self.test_screenshot_url,
            self.test_html_url,
            is_new_page=True
        )

        # Verify header
        self.assertEqual(blocks[0]['text']['text'], "🆕 New Page")
        
        # Verify URL formatting
        self.assertIn(self.test_url, blocks[1]['text']['text'])
        
        # Verify batch statistics
        self.assertEqual(self.slack_service._batch_stats['new_pages'], 1)

    def test_text_changes_notification(self):
        """Test notification formatting for text changes."""
        added = [{'new_text': 'New content added'}]
        changed = [{'new_text': 'Content was changed'}]
        deleted = []

        blocks = self.slack_service.format_change_message(
            self.test_url,
            added, deleted, changed,
            {'added_links': set(), 'removed_links': set(), 'added_pdfs': set(), 'removed_pdfs': set()},
            self.test_screenshot_url,
            self.test_html_url,
            is_new_page=False
        )

        # Verify text change sections
        content_texts = [block['text']['text'] for block in blocks if block['type'] == 'section']
        self.assertTrue(any('New content added' in text for text in content_texts))
        self.assertTrue(any('Content was changed' in text for text in content_texts))
        
        # Verify batch statistics
        self.assertEqual(self.slack_service._batch_stats['text_changes'], 2)
        self.assertEqual(self.slack_service._batch_stats['modified_pages'], 1)

    def test_link_changes_notification(self):
        """Test notification formatting for link changes."""
        links_changes = {
            'added_links': {'https://new-link.com'},
            'removed_links': {'https://old-link.com'},
            'added_pdfs': set(),
            'removed_pdfs': set()
        }

        blocks = self.slack_service.format_change_message(
            self.test_url, [], [], [],
            links_changes,
            self.test_screenshot_url,
            self.test_html_url,
            is_new_page=False
        )

        # Verify link sections
        link_texts = [block['text']['text'] for block in blocks if block['type'] == 'section']
        self.assertTrue(any('new-link.com' in text for text in link_texts))
        self.assertTrue(any('old-link.com' in text for text in link_texts))
        
        # Verify batch statistics
        self.assertEqual(self.slack_service._batch_stats['link_changes'], 2)

    def test_pdf_changes_notification(self):
        """Test notification formatting for PDF changes."""
        links_changes = {
            'added_links': set(),
            'removed_links': set(),
            'added_pdfs': {'https://new-pdf.com/doc.pdf'},
            'removed_pdfs': {'https://old-pdf.com/doc.pdf'}
        }

        blocks = self.slack_service.format_change_message(
            self.test_url, [], [], [],
            links_changes,
            self.test_screenshot_url,
            self.test_html_url,
            is_new_page=False
        )

        # Verify PDF sections
        pdf_texts = [block['text']['text'] for block in blocks if block['type'] == 'section']
        self.assertTrue(any('new-pdf.com' in text for text in pdf_texts))
        self.assertTrue(any('old-pdf.com' in text for text in pdf_texts))
        
        # Verify batch statistics
        self.assertEqual(self.slack_service._batch_stats['pdf_changes'], 2)

    def test_message_batching(self):
        """Test message batching functionality."""
        # Queue multiple messages
        for i in range(3):
            blocks = self.slack_service.format_change_message(
                f"{self.test_url}/{i}", [], [], [],
                {'added_links': set(), 'removed_links': set(), 'added_pdfs': set(), 'removed_pdfs': set()},
                self.test_screenshot_url,
                self.test_html_url,
                is_new_page=True
            )
            self.slack_service.queue_message(blocks)

        # Verify messages are queued
        self.assertEqual(len(self.slack_service._message_queue), 3)
        
        # Force send queued messages
        self.slack_service.send_queued_messages()

        # Verify queue is cleared
        self.assertEqual(len(self.slack_service._message_queue), 0)
        
        # Verify batch statistics are reset
        self.assertEqual(self.slack_service._batch_stats['new_pages'], 0)

    def test_batch_timing(self):
        """Test batch timing functionality."""
        with patch('src.services.slack_service.datetime') as mock_datetime:
            # Set initial time
            initial_time = datetime(2024, 1, 1, 12, 0)
            mock_datetime.now.return_value = initial_time
            
            # Queue first message
            self.slack_service.queue_message([{"type": "section", "text": {"type": "mrkdwn", "text": "Test"}}])
            
            # Verify no message sent yet
            self.slack_service.client.chat_postMessage.assert_not_called()
            
            # Move time forward 4 minutes (before batch interval)
            mock_datetime.now.return_value = initial_time + timedelta(minutes=4)
            self.slack_service.queue_message([{"type": "section", "text": {"type": "mrkdwn", "text": "Test"}}])
            
            # Verify still no message sent
            self.slack_service.client.chat_postMessage.assert_not_called()
            
            # Move time forward 6 minutes (after batch interval)
            mock_datetime.now.return_value = initial_time + timedelta(minutes=6)
            self.slack_service.queue_message([{"type": "section", "text": {"type": "mrkdwn", "text": "Test"}}])
            
            # Verify batch was sent
            self.slack_service.client.chat_postMessage.assert_called_once()

    def test_error_notification(self):
        """Test error notification formatting."""
        error_message = "Test error message"
        self.slack_service.send_error(error_message, self.test_url)

        # Verify error message was sent
        self.slack_service.client.chat_postMessage.assert_called_once()
        call_args = self.slack_service.client.chat_postMessage.call_args[1]
        
        # Verify error formatting
        blocks = call_args['blocks']
        self.assertEqual(blocks[0]['text']['text'], "⚠️ Error Alert")
        self.assertIn(error_message, blocks[1]['text']['text'])
        self.assertIn(self.test_url, blocks[2]['text']['text'])

    def test_text_formatting(self):
        """Test text formatting utilities."""
        # Test text truncation
        long_text = "x" * 300
        truncated = self.slack_service._format_text(long_text)
        self.assertTrue(len(truncated) <= 200)
        self.assertTrue(truncated.endswith("..."))
        
        # Test HTML escaping
        html_text = "<div>Test & example</div>"
        escaped = self.slack_service._format_text(html_text)
        self.assertEqual(escaped, "&lt;div&gt;Test &amp; example&lt;/div&gt;")

    def test_url_formatting(self):
        """Test URL formatting utilities."""
        # Test URL truncation
        long_url = "https://www.education.gov.au/" + "x" * 100
        truncated = self.slack_service._truncate_url(long_url)
        self.assertTrue(len(truncated) <= 50)
        self.assertTrue(truncated.endswith("..."))
        
        # Test short URL
        short_url = "https://example.com"
        unchanged = self.slack_service._truncate_url(short_url)
        self.assertEqual(unchanged, short_url)

    def test_summary_creation(self):
        """Test summary block creation."""
        # Queue multiple messages with different types of changes
        self.slack_service._batch_stats.update({
            'modified_pages': 5,
            'new_pages': 2,
            'text_changes': 10,
            'link_changes': 3,
            'pdf_changes': 4
        })
        
        summary_blocks = self.slack_service._create_summary_blocks()
        
        # Verify summary formatting
        summary_text = summary_blocks[1]['text']['text']
        self.assertIn('Pages Modified: 5', summary_text)
        self.assertIn('New Pages: 2', summary_text)
        self.assertIn('Text Changes: 10', summary_text)
        self.assertIn('Link Changes: 3', summary_text)
        self.assertIn('PDF Changes: 4', summary_text)

if __name__ == '__main__':
    unittest.main() 