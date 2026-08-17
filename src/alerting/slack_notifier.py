"""
Slack Notifier for pipeline alerts.

Handles all Slack integration for alerting pipeline failures and successes.
"""

import os
import json
import logging
from typing import Optional, Dict
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

logger = logging.getLogger(__name__)


class SlackNotifier:
    """
    Send notifications to Slack for pipeline events.
    """
    
    def __init__(self):
        """Initialize Slack client with bot token."""
        self.token = os.getenv('SLACK_TOKEN')
        self.webhook_url = os.getenv('SLACK_WEBHOOK_URL')
        self.default_channel = os.getenv('SLACK_CHANNEL', '#airflow-alerts')
        
        if not self.token and not self.webhook_url:
            logger.warning("Slack credentials not configured. Slack notifications will be disabled.")
            self.client = None
        else:
            try:
                self.client = WebClient(token=self.token) if self.token else None
            except Exception as e:
                logger.error(f"Failed to initialize Slack client: {e}")
                self.client = None
    
    def send_alert(self, message: str, channel: Optional[str] = None, 
                   exception: Optional[Exception] = None) -> bool:
        """
        Send an alert message to Slack.
        
        Args:
            message: Alert message text
            channel: Slack channel (defaults to configured channel)
            exception: Optional exception object to include in alert
            
        Returns:
            True if message was sent successfully
        """
        if not self.client and not self.webhook_url:
            logger.warning("Slack notifier not configured")
            return False
        
        channel = channel or self.default_channel
        
        try:
            # Build message blocks
            blocks = [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "⚠️ Pipeline Alert",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": message
                    }
                }
            ]
            
            if exception:
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"```\n{str(exception)}\n```"
                    }
                })
            
            if self.client:
                self.client.chat_postMessage(
                    channel=channel,
                    blocks=blocks,
                    text=message  # Fallback text for clients that don't support blocks
                )
            
            logger.info(f"Alert sent to Slack channel {channel}")
            return True
            
        except SlackApiError as e:
            logger.error(f"Slack API error: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")
            return False
    
    def send_message(self, message: str, channel: Optional[str] = None) -> bool:
        """
        Send a regular message to Slack.
        
        Args:
            message: Message text
            channel: Slack channel
            
        Returns:
            True if message was sent successfully
        """
        if not self.client and not self.webhook_url:
            logger.warning("Slack notifier not configured")
            return False
        
        channel = channel or self.default_channel
        
        try:
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": message
                    }
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"_Sent at {self._get_timestamp()}_"
                        }
                    ]
                }
            ]
            
            if self.client:
                self.client.chat_postMessage(
                    channel=channel,
                    blocks=blocks,
                    text=message
                )
            
            logger.info(f"Message sent to Slack channel {channel}")
            return True
            
        except SlackApiError as e:
            logger.error(f"Slack API error: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to send Slack message: {e}")
            return False
    
    def send_thread_message(self, channel: str, thread_ts: str, message: str) -> bool:
        """
        Send a message to a thread in Slack.
        
        Args:
            channel: Slack channel containing the thread
            thread_ts: Thread timestamp (unique identifier)
            message: Message text
            
        Returns:
            True if message was sent successfully
        """
        if not self.client:
            logger.warning("Slack client not configured")
            return False
        
        try:
            self.client.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                text=message
            )
            logger.info(f"Thread message sent to {channel}")
            return True
            
        except SlackApiError as e:
            logger.error(f"Slack API error: {e}")
            return False
    
    def upload_file(self, channel: str, file_content: str, 
                   filename: str, file_type: str = "text") -> bool:
        """
        Upload a file to Slack channel.
        
        Args:
            channel: Slack channel
            file_content: File content
            filename: Name of the file
            file_type: File type (text, json, etc.)
            
        Returns:
            True if file was uploaded successfully
        """
        if not self.client:
            logger.warning("Slack client not configured")
            return False
        
        try:
            self.client.files_upload(
                channels=channel,
                file=file_content.encode(),
                filename=filename,
                filetype=file_type
            )
            logger.info(f"File {filename} uploaded to {channel}")
            return True
            
        except SlackApiError as e:
            logger.error(f"Slack API error: {e}")
            return False
    
    def get_channel_info(self, channel: str) -> Optional[Dict]:
        """
        Get information about a Slack channel.
        
        Args:
            channel: Channel name or ID
            
        Returns:
            Channel information dictionary or None if failed
        """
        if not self.client:
            return None
        
        try:
            response = self.client.conversations_info(channel=channel)
            return response.get('channel')
        except SlackApiError as e:
            logger.error(f"Failed to get channel info: {e}")
            return None
    
    @staticmethod
    def _get_timestamp() -> str:
        """Get current timestamp as string."""
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
