"""
Email Notifier for pipeline alerts.

Handles all email integration for alerting pipeline failures and successes.
"""

import os
import logging
import smtplib
from typing import List, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime

logger = logging.getLogger(__name__)


class EmailNotifier:
    """
    Send email notifications for pipeline events.
    """
    
    def __init__(self):
        """Initialize email configuration."""
        self.smtp_host = os.getenv('SMTP_HOST', 'localhost')
        self.smtp_port = int(os.getenv('SMTP_PORT', 587))
        self.smtp_user = os.getenv('SMTP_USER', '')
        self.smtp_password = os.getenv('SMTP_PASSWORD', '')
        self.from_email = os.getenv('AIRFLOW_FROM_EMAIL', 'airflow@example.com')
        self.to_email = os.getenv('AIRFLOW_TO_EMAIL', 'alerts@example.com')
        self.use_tls = os.getenv('SMTP_USE_TLS', 'True').lower() == 'true'
    
    def send_alert(self, subject: str, body: str, 
                   to_email: Optional[str] = None,
                   html: bool = False,
                   attachments: Optional[List[str]] = None) -> bool:
        """
        Send an alert email.
        
        Args:
            subject: Email subject line
            body: Email body content
            to_email: Recipient email address (defaults to configured email)
            html: Whether body contains HTML content
            attachments: List of file paths to attach
            
        Returns:
            True if email was sent successfully
        """
        to_email = to_email or self.to_email
        
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.from_email
            msg['To'] = to_email if isinstance(to_email, str) else ', '.join(to_email)
            msg['Date'] = self._get_timestamp()
            
            # Add body
            mime_type = 'html' if html else 'plain'
            msg.attach(MIMEText(body, mime_type, _charset='utf-8'))
            
            # Add attachments if provided
            if attachments:
                for filepath in attachments:
                    if os.path.exists(filepath):
                        self._attach_file(msg, filepath)
            
            # Send email
            return self._send_smtp(msg, to_email)
            
        except Exception as e:
            logger.error(f"Failed to prepare email: {e}")
            return False
    
    def send_failure_alert(self, dag_id: str, task_id: str, 
                          exception: str, log_url: Optional[str] = None) -> bool:
        """
        Send a formatted failure alert email.
        
        Args:
            dag_id: Airflow DAG ID
            task_id: Failed task ID
            exception: Exception message
            log_url: URL to task logs
            
        Returns:
            True if email was sent successfully
        """
        subject = f"🚨 Airflow Alert: {dag_id} - {task_id} FAILED"
        
        body = f"""
        <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; }}
                    .alert-box {{ background-color: #ffebee; border: 1px solid #c62828; padding: 20px; border-radius: 5px; }}
                    .info-table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
                    .info-table th {{ background-color: #f5f5f5; padding: 10px; text-align: left; }}
                    .info-table td {{ padding: 10px; border-bottom: 1px solid #ddd; }}
                    .error-box {{ background-color: #f5f5f5; padding: 10px; border-radius: 3px; font-family: monospace; }}
                </style>
            </head>
            <body>
                <h2>Pipeline Execution Failed</h2>
                <div class="alert-box">
                    <p><strong>A task in your Airflow pipeline has failed and requires attention.</strong></p>
                </div>
                
                <table class="info-table">
                    <tr>
                        <th>Parameter</th>
                        <th>Value</th>
                    </tr>
                    <tr>
                        <td>DAG ID</td>
                        <td><strong>{dag_id}</strong></td>
                    </tr>
                    <tr>
                        <td>Task ID</td>
                        <td><strong>{task_id}</strong></td>
                    </tr>
                    <tr>
                        <td>Failure Time</td>
                        <td>{self._get_timestamp()}</td>
                    </tr>
                </table>
                
                <h3>Error Details</h3>
                <div class="error-box">
                    {exception}
                </div>
                
                {f'<p><a href="{log_url}">View full logs</a></p>' if log_url else ''}
                
                <hr>
                <p style="color: #666; font-size: 12px;">
                    This is an automated message from Airflow. Please do not reply.
                </p>
            </body>
        </html>
        """
        
        return self.send_alert(subject, body, html=True)
    
    def send_success_notification(self, dag_id: str, duration: float) -> bool:
        """
        Send a success notification email.
        
        Args:
            dag_id: Airflow DAG ID
            duration: Execution duration in seconds
            
        Returns:
            True if email was sent successfully
        """
        subject = f"✅ Airflow Success: {dag_id} completed"
        
        body = f"""
        <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; }}
                    .success-box {{ background-color: #e8f5e9; border: 1px solid #2e7d32; padding: 20px; border-radius: 5px; }}
                    .info-table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
                    .info-table th {{ background-color: #f5f5f5; padding: 10px; text-align: left; }}
                    .info-table td {{ padding: 10px; border-bottom: 1px solid #ddd; }}
                </style>
            </head>
            <body>
                <h2>Pipeline Execution Completed Successfully</h2>
                <div class="success-box">
                    <p><strong>Your Airflow pipeline completed without errors.</strong></p>
                </div>
                
                <table class="info-table">
                    <tr>
                        <th>Parameter</th>
                        <th>Value</th>
                    </tr>
                    <tr>
                        <td>DAG ID</td>
                        <td><strong>{dag_id}</strong></td>
                    </tr>
                    <tr>
                        <td>Duration</td>
                        <td>{duration:.2f} seconds</td>
                    </tr>
                    <tr>
                        <td>Completion Time</td>
                        <td>{self._get_timestamp()}</td>
                    </tr>
                </table>
                
                <hr>
                <p style="color: #666; font-size: 12px;">
                    This is an automated message from Airflow. Please do not reply.
                </p>
            </body>
        </html>
        """
        
        return self.send_alert(subject, body, html=True)
    
    def _send_smtp(self, msg: MIMEMultipart, to_email) -> bool:
        """
        Send email via SMTP.
        
        Args:
            msg: MIMEMultipart message object
            to_email: Recipient email address(es)
            
        Returns:
            True if email was sent successfully
        """
        try:
            # Create SMTP connection
            if self.use_tls:
                server = smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=10)
                server.starttls()
            else:
                server = smtplib.SMTP_SSL(self.smtp_host, self.smtp_port, timeout=10)
            
            # Login if credentials provided
            if self.smtp_user and self.smtp_password:
                server.login(self.smtp_user, self.smtp_password)
            
            # Send email
            if isinstance(to_email, str):
                to_email = [to_email]
            
            server.sendmail(self.from_email, to_email, msg.as_string())
            server.quit()
            
            logger.info(f"Email sent successfully to {', '.join(to_email)}")
            return True
            
        except smtplib.SMTPAuthenticationError:
            logger.error("SMTP authentication failed. Check credentials.")
            return False
        except smtplib.SMTPException as e:
            logger.error(f"SMTP error: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False
    
    @staticmethod
    def _attach_file(msg: MIMEMultipart, filepath: str) -> None:
        """
        Attach a file to the email message.
        
        Args:
            msg: MIMEMultipart message object
            filepath: Path to file to attach
        """
        try:
            filename = os.path.basename(filepath)
            
            with open(filepath, 'rb') as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
            
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename= {filename}')
            msg.attach(part)
            
            logger.info(f"File {filename} attached to email")
            
        except Exception as e:
            logger.error(f"Failed to attach file {filepath}: {e}")
    
    @staticmethod
    def _get_timestamp() -> str:
        """Get current timestamp as formatted string."""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
