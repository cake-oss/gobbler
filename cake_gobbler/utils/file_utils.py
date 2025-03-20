# SPDX-FileCopyrightText: 2025 Cake AI Technologies, Inc.
#
# SPDX-License-Identifier: Apache-2.0

"""
File utilities for the Cake Gobbler RAG system.

This module provides utilities for file operations.
"""

import os
import hashlib
import datetime
from pathlib import Path
from typing import List, Tuple


def find_pdf_files(path: str) -> List[str]:
    """
    Find all PDF files in a directory or return a single file.
    
    Args:
        path: Path to a PDF file or directory of PDFs
        
    Returns:
        List of absolute PDF file paths
    """
    pdf_files = []
    
    if os.path.isdir(path):
        # Use os.walk to traverse all subdirectories
        for root, dirs, files in os.walk(path):
            for file in files:
                # Case-insensitive check for .pdf extension
                if file.lower().endswith('.pdf'):
                    pdf_files.append(os.path.abspath(os.path.join(root, file)))
    elif os.path.isfile(path) and path.lower().endswith('.pdf'):
        pdf_files = [os.path.abspath(path)]

    return pdf_files

def calculate_file_fingerprint(file_path: str) -> str:
    """
    Calculate a SHA256 fingerprint for a file based on its path and file size.
    
    Args:
        file_path: Path to the file
        
    Returns:
        SHA256 hash combining file path and size as the fingerprint
    """
    # Get file stats
    stat_info = os.stat(file_path)
    file_size = stat_info.st_size
    
    # Create a hash from path and file size
    sha256_hash = hashlib.sha256()
    # Add absolute path to ensure consistency
    abs_path = os.path.abspath(file_path)
    sha256_hash.update(abs_path.encode('utf-8'))
    sha256_hash.update(str(file_size).encode('utf-8'))
    
    # Return the hexadecimal digest as the fingerprint
    return sha256_hash.hexdigest()
