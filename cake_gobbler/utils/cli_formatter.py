# SPDX-FileCopyrightText: 2025 Cake AI Technologies, Inc.
#
# SPDX-License-Identifier: Apache-2.0

"""
CLI formatting utilities for the Cake Gobbler RAG system.

This module provides utilities for formatting console output in a consistent way.
"""

from typing import Dict, Any, Optional, List, Union, Tuple
from rich.console import Console
from rich.table import Table


def create_status_table(
    title: str, 
    data: Dict[str, Any], 
    columns: List[Tuple[str, Optional[str]]],
    expand: bool = False
) -> Table:
    """
    Create a simple two-column property-value table.
    
    Args:
        title: Title for the table
        data: Dictionary of property-value pairs
        columns: List of (name, color) tuples defining the columns
        expand: Whether the table should expand to fill the console width
        
    Returns:
        Rich Table object
    """
    table = Table(show_header=True, header_style="bold magenta", expand=expand, title=title)
    
    # Add columns
    for col_name, col_style in columns:
        table.add_column(col_name, style=col_style, no_wrap=False)
    
    # Add rows
    for key, value in data.items():
        table.add_row(key, str(value))
    
    return table


def format_status_color(status: str) -> str:
    """
    Get a color for a status string.
    
    Args:
        status: Status string
        
    Returns:
        Formatted status with color
    """
    color = {
        "success": "green",
        "error": "red",
        "skipped": "yellow",
        "failed": "red",
        "already_processed": "blue",
        "running": "yellow",
        "completed": "green",
        "completed_with_errors": "yellow",
        "not_started": "white"
    }.get(status.lower(), "white")
    
    return f"[{color}]{status}[/{color}]"


def create_list_table(
    title: str,
    headers: List[str],
    rows: List[List[Any]],
    expand: bool = True
) -> Table:
    """
    Create a table for displaying lists of items.
    
    Args:
        title: Title for the table
        headers: List of column headers
        rows: List of rows, each a list of values
        expand: Whether the table should expand to fill the console width
        
    Returns:
        Rich Table object
    """
    table = Table(show_header=True, header_style="bold magenta", expand=expand, title=title)
    
    # Add columns
    for header in headers:
        table.add_column(header)
    
    # Add rows
    for row in rows:
        # Convert any non-string values to strings
        string_row = [
            str(cell) if not isinstance(cell, str) else cell
            for cell in row
        ]
        table.add_row(*string_row)
    
    return table


def truncate_text(text: str, max_length: int = 50) -> str:
    """
    Truncate text to a maximum length.
    
    Args:
        text: Text to truncate
        max_length: Maximum length of the truncated text
        
    Returns:
        Truncated text
    """
    if len(text) <= max_length:
        return text
    return text[:max_length] + "..."
