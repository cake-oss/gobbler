#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2025 Cake AI Technologies, Inc.
#
# SPDX-License-Identifier: Apache-2.0

"""
PyMuPDF Interface - All PyMuPDF (fitz) operations through subprocess

This module provides a unified interface for all PyMuPDF operations, ensuring:
1. AGPL licensing compliance through subprocess isolation
2. No direct imports of fitz in the main application
3. No circular dependencies in the codebase

All PyMuPDF operations must use this interface instead of importing fitz directly.
"""

import json
import logging
import os
import subprocess
import time
from pathlib import Path
from typing import Dict, Optional, Any, Tuple, List

# Set up standard Python logging instead of the project's custom logging
# to avoid potential circular dependencies

# Create logs directory if it doesn't exist
logs_dir = Path('logs')
logs_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=logs_dir / 'pymupdf_interface.log'
)
logger = logging.getLogger("pymupdf_interface")


class PyMuPDFInterface:
    """
    Unified interface for all PyMuPDF operations through subprocess.
    This approach is used to comply with AGPL licensing requirements.
    """
    
    def __init__(self, verbose: bool = False):
        """
        Initialize the PyMuPDF interface.
        
        Args:
            verbose: Whether to enable verbose logging
        """
        self.verbose = verbose
        self._setup_logging()
    
    def _setup_logging(self):
        """Set up logging for the PyMuPDF interface."""
        self.logger = logging.getLogger("cake-gobbler.pymupdf_interface")
        if self.verbose:
            self.logger.setLevel(logging.DEBUG)
    
    def extract_text(self, pdf_path: str, password: Optional[str] = None) -> Tuple[str, Dict[str, Any]]:
        """
        Extract text from a PDF file.
        
        Args:
            pdf_path: Path to the PDF file
            password: Password for encrypted PDFs (if applicable)
            
        Returns:
            Tuple[str, Dict[str, Any]]: (Extracted text, extraction details)
        """
        self.logger.info(f"Extracting text from: {pdf_path}")
        
        try:
            filepath = Path(pdf_path)
            if not filepath.exists():
                return "", {"success": False, "error": f"File not found: {pdf_path}"}
            
            # Get page count first using the show command
            try:
                page_count_result = subprocess.run(
                    ["uv", "run", "--with", "pymupdf", "pymupdf", "show", str(filepath)],
                    capture_output=True,
                    text=True,
                    check=True
                )
                
                # Parse page count from output
                page_count = 0
                for line in page_count_result.stdout.split('\n'):
                    if "pages:" in line:
                        try:
                            # Extract page count using regex to be more robust
                            import re
                            match = re.search(r'pages:\s*(\d+)', line)
                            if match:
                                page_count = int(match.group(1))
                                break
                        except Exception as e:
                            self.logger.warning(f"Error parsing page count: {str(e)}")
                            self.logger.warning(f"Line: {line}")
                
                # Run the gettext command - this writes to a txt file next to the PDF
                subprocess.run(
                    ["uv", "run", "--with", "pymupdf", "pymupdf", "gettext", "-mode", "blocks", "-noligatures", str(filepath)],
                    capture_output=True,
                    text=True,
                    check=True
                )
                
                # Generate the output text file path based on the PDF path
                txt_path = str(filepath)[:-4] + '.txt'
                    
                # Read the text from the generated file
                text = None
                encodings_to_try = ['utf-8', 'utf-16', 'latin-1', 'cp1252', 'iso-8859-1']
                
                try:
                    with open(txt_path, 'rb') as f:
                        binary_content = f.read()
                    
                    for encoding in encodings_to_try:
                        try:
                            text = binary_content.decode(encoding)
                            self.logger.info(f"Successfully decoded text using {encoding}")
                            break
                        except UnicodeDecodeError:
                            self.logger.warning(f"Failed to decode with {encoding}, trying next encoding")
                            continue
                    
                    if text is None:
                        text = binary_content.decode('latin-1')
                        self.logger.warning("All encodings failed, using latin-1 as fallback")
                except Exception as e:
                    self.logger.error(f"Error reading text file: {str(e)}")
                    # Make sure to clean up the temporary file even if there's an error
                    if os.path.exists(txt_path):
                        try:
                            os.remove(txt_path)
                            self.logger.info(f"Removed temporary text file: {txt_path}")
                        except Exception as cleanup_e:
                            self.logger.warning(f"Failed to remove temporary file: {str(cleanup_e)}")
                    return "", {
                        "success": False,
                        "failure_reason": "text_file_read_error",
                        "failure_details": str(e)
                    }
                finally:
                    # Ensure the temporary file is removed
                    if os.path.exists(txt_path):
                        try:
                            os.remove(txt_path)
                            self.logger.info(f"Removed temporary text file: {txt_path}")
                        except Exception as cleanup_e:
                            self.logger.warning(f"Failed to remove temporary file: {str(cleanup_e)}")
                
                if not text:
                    return "", {
                        "success": False,
                        "failure_reason": "empty_text",
                        "failure_details": "Extracted text is empty or could not be read",
                        "page_count": page_count
                    }
                
                return text, {
                    "success": True,
                    "page_count": page_count,
                    "char_count": len(text)
                }
                
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Error extracting text: {e.stderr}")
                return "", {
                    "success": False,
                    "failure_reason": "extraction_command_failed",
                    "failure_details": str(e.stderr)
                }
            
        except Exception as e:
            self.logger.error(f"Error in text extraction: {str(e)}")
            return "", {
                "success": False,
                "failure_reason": "general_extraction_error",
                "failure_details": str(e)
            }
    
    def get_page_count(self, pdf_path: str, password: Optional[str] = None) -> Dict[str, Any]:
        """
        Get the number of pages and basic information about a PDF file.
        
        Args:
            pdf_path: Path to the PDF file
            password: Password for encrypted PDFs (if applicable)
            
        Returns:
            Dict[str, Any]: Dictionary with page count and PDF info
        """
        self.logger.info(f"Getting page count for: {pdf_path}")
        
        try:
            filepath = Path(pdf_path)
            if not filepath.exists():
                return {"success": False, "error": f"File not found: {pdf_path}"}
            
            # Use pymupdf command line tool to get PDF info
            try:
                page_count_result = subprocess.run(
                    ["uv", "run", "--with", "pymupdf", "pymupdf", "show", str(filepath)],
                    capture_output=True,
                    text=True,
                    check=True
                )
                
                # Parse page count and other info from output
                page_count = 0
                is_encrypted = False
                is_form_pdf = False
                
                for line in page_count_result.stdout.split('\n'):
                    if "pages:" in line:
                        try:
                            # Extract page count using regex to be more robust
                            import re
                            match = re.search(r'pages:\s*(\d+)', line)
                            if match:
                                page_count = int(match.group(1))
                        except Exception as e:
                            self.logger.warning(f"Error parsing page count: {str(e)}")
                            self.logger.warning(f"Line: {line}")
                    elif "encryption:" in line:
                        is_encrypted = "none" not in line.lower()
                    elif "Form:" in line:
                        is_form_pdf = "yes" in line.lower()
                
                return {
                    "success": True,
                    "page_count": page_count,
                    "is_encrypted": is_encrypted,
                    "is_form_pdf": is_form_pdf,
                    # Other properties aren't easily available from the 'show' command,
                    # but could be fetched with additional commands if needed
                    "has_js": False,
                    "is_pdf_signed": False
                }
                
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Error getting page count: {e.stderr}")
                return {"success": False, "error": f"Command failed: {e.stderr}"}
            except Exception as e:
                self.logger.error(f"Error running page count command: {str(e)}")
                return {"success": False, "error": str(e)}
        
        except Exception as e:
            self.logger.error(f"Error getting page count: {str(e)}")
            return {"success": False, "error": str(e)}
