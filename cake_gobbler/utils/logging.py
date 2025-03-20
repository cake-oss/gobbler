# SPDX-FileCopyrightText: 2025 Cake AI Technologies, Inc.
#
# SPDX-License-Identifier: Apache-2.0

"""
Logging utilities for the Cake Gobbler RAG system.

This module provides logging configuration and utilities.
"""

import logging
from rich.console import Console
from rich.logging import RichHandler

# Create rich console for fancy display
console = Console()

# Configure logging with rich
def configure_logging(verbose: bool = False) -> logging.Logger:
    """
    Configure logging for the application.
    
    Args:
        verbose: Whether to enable verbose logging
        
    Returns:
        Logger instance
    """
    import os
    from pathlib import Path
    from datetime import datetime
    
    # Set up log directory
    log_dir = os.environ.get('LOG_DIR', './logs')
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # Configure basic logging with both file and console handlers
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    log_file = Path(log_dir) / f'cake-gobbler_{timestamp}.log'
    
    # Get logger first before adding handlers
    logger = logging.getLogger("cake-gobbler")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.propagate = True  # Ensure propagation is enabled
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Add handlers to logger directly instead of using basicConfig
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    logger.addHandler(file_handler)
    
    rich_handler = RichHandler(rich_tracebacks=True)
    rich_handler.setLevel(logging.INFO)
    rich_handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(rich_handler)
    
    # Test log messages to verify logging configuration
    logger.debug(f"DEBUG TEST: Log file created at {log_file}")
    logger.info(f"INFO TEST: Logging system initialized")
    
    # Check if debug messages are being filtered
    
    return logger
