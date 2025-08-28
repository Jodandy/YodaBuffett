#!/usr/bin/env python3

"""
Email Configuration for IR Subscriptions
SECURITY: Use environment variables or secret manager in production
"""

import os
from typing import Dict, Optional

# Email configuration
EMAIL_CONFIG = {
    "address": "yodabuffett.ir@gmail.com",
    "password": os.getenv("IR_EMAIL_PASSWORD", "!BuffayTime3214"),  # Use env var in production
    "imap_server": "imap.gmail.com",
    "imap_port": 993,
    "smtp_server": "smtp.gmail.com", 
    "smtp_port": 587
}

# Security warning
if not os.getenv("IR_EMAIL_PASSWORD"):
    print("⚠️  WARNING: Using hardcoded password. Set IR_EMAIL_PASSWORD env variable for production.")

def get_email_config() -> Dict[str, any]:
    """Get email configuration (use this function to access config)"""
    return EMAIL_CONFIG

def mask_password(password: str) -> str:
    """Mask password for logging"""
    if len(password) < 4:
        return "***"
    return password[:2] + "*" * (len(password) - 4) + password[-2:]