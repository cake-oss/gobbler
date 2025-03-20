# SPDX-FileCopyrightText: 2025 Cake AI Technologies, Inc.
#
# SPDX-License-Identifier: Apache-2.0

"""
PDF Processor for the Cake Gobbler RAG system.

This module handles PDF processing operations, including text extraction,
PDF analysis, and preparation for chunking and embedding.
"""

import os
import logging
import subprocess
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, NamedTuple
from dataclasses import dataclass

from cake_gobbler.core.pdf_analyzer import PDFAnalyzer
from cake_gobbler.models.pdf_models import PDFAnalysisResult, PDFIssue, PDFIssueType
from cake_gobbler.utils.pymupdf_interface import PyMuPDFInterface


@dataclass
class ExtractionResult:
    """Results of a text extraction operation."""
    text: str
    page_count: int
    successful: bool = True
    failure_reason: Optional[str] = None
    failure_details: Optional[str] = None
    extraction_attempts: List[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.extraction_attempts is None:
            self.extraction_attempts = []


class PDFProcessor:
    """
    Handles PDF processing operations for the ingestion system.
    """
    
    def __init__(self, verbose: bool = False):
        """
        Initialize the PDF processor.
        
        Args:
            verbose: Whether to enable verbose logging
        """
        self.verbose = verbose
        self.analyzer = PDFAnalyzer(verbose=verbose)
        self.logger = logging.getLogger("cake-gobbler.pdf_processor")
        self.pymupdf = PyMuPDFInterface(verbose=verbose)
        
    def analyze_pdf(self, pdf_path: str, password: Optional[str] = None) -> PDFAnalysisResult:
        """
        Analyze a PDF file for encoding types, fonts, and potential issues.
        
        Args:
            pdf_path: Path to the PDF file
            password: Password for encrypted PDFs (if applicable)
            
        Returns:
            PDFAnalysisResult: Analysis results
        """
        self.logger.info(f"Analyzing PDF: {pdf_path}")
        try:
            result = self.analyzer.analyze_file(pdf_path, password)
            
            # Log analysis results
            if self.verbose:
                self.logger.info(f"Analysis completed for {pdf_path}")
                self.logger.info(f"  Pages: {result.num_pages}")
                self.logger.info(f"  Size: {result.filesize / 1024 / 1024:.2f} MB")
                self.logger.info(f"  Encrypted: {result.is_encrypted}")
                self.logger.info(f"  Damaged: {result.is_damaged}")
                self.logger.info(f"  Encoding types: {[et.name for et in result.encoding_types]}")
                self.logger.info(f"  Fonts: {len(result.fonts)}")
                self.logger.info(f"  Issues: {len(result.issues)}")
            
            return result
        except Exception as e:
            self.logger.error(f"Error analyzing PDF {pdf_path}: {str(e)}")
            raise
    
    def extract_text_from_pdf(self, pdf_path: str) -> Tuple[str, Dict[str, Any]]:
        """
        Extract text from a PDF file using the external PyMuPDF program
        
        This function extracts text and handles encoding detection through the PyMuPDF client,
        which communicates with an external process to comply with AGPL licensing requirements.
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            Tuple[str, Dict[str, Any]]: (Extracted text, Extraction diagnostics with encoding information)
        """
        self.logger.info(f"Extracting text from PDF: {pdf_path}")
        if not os.path.exists(pdf_path):
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")
        
        # Create initial extraction result
        result = ExtractionResult(
            text="",
            page_count=0
        )
        
        try:
            # Use the PyMuPDF interface to extract text
            text, extraction_details = self.pymupdf.extract_text(pdf_path)
            
            # Update result with details
            result.text = text
            result.page_count = extraction_details.get("page_count", 0)
            result.successful = extraction_details.get("success", False)
            
            # Add failure details if extraction failed
            if not result.successful:
                result.failure_reason = extraction_details.get("failure_reason", "unknown_error")
                result.failure_details = extraction_details.get("failure_details", "Unknown extraction error")
                return "", self._create_diagnostics_from_result(result)
            
            # Check if PDF has no pages
            if result.page_count == 0:
                result.successful = False
                result.failure_reason = "empty_document"
                result.failure_details = "PDF has 0 pages"
                return "", self._create_diagnostics_from_result(result)
            
            # Check if extracted text is empty or just whitespace
            if not text or text.strip() == "":
                result.successful = False
                result.failure_reason = "empty_text"
                result.failure_details = "Extracted text is empty or contains only whitespace"
                return "", self._create_diagnostics_from_result(result)
            
            # Add information about the extraction
            if not result.extraction_attempts:
                result.extraction_attempts = [{
                    "method": "pymupdf_external",
                    "success": True,
                    "char_count": len(text)
                }]
            
            self.logger.info(f"Extracted {len(text)} characters from {result.page_count} pages")
            
            return text, self._create_diagnostics_from_result(result)
        
        except Exception as e:
            self.logger.error(f"Error extracting text from PDF: {str(e)}")
            result.successful = False
            result.failure_reason = "general_extraction_error"
            result.failure_details = f"Error extracting text from PDF: {str(e)}"
            return "", self._create_diagnostics_from_result(result)

    def _create_diagnostics_from_result(self, result: ExtractionResult) -> Dict[str, Any]:
        """
        Convert ExtractionResult to diagnostics dictionary.
        
        Args:
            result: ExtractionResult object
            
        Returns:
            Dict[str, Any]: Extraction diagnostics dictionary
        """
        return {
            "extraction_attempts": result.extraction_attempts,
            "page_count": result.page_count,
            "failure_reason": result.failure_reason,
            "failure_details": result.failure_details
        }
    
    def is_pdf_acceptable(self, analysis_result: PDFAnalysisResult) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Determine if a PDF is acceptable for processing based on analysis results.
        
        Args:
            analysis_result: PDFAnalysisResult from analyze_pdf
            
        Returns:
            Tuple[bool, str, Dict[str, Any]]: (is_acceptable, reason, detailed_diagnostics)
        """
        # Create diagnostics dictionary
        detailed_diagnostics = {
            "issues_found": [],
            "primary_rejection_reason": None,
            "all_issues": [
                {
                    "type": issue.type.name,
                    "description": issue.description,
                    "severity": issue.severity,
                    "page_numbers": issue.page_numbers if issue.page_numbers else None,
                    "details": issue.details if issue.details else None
                }
                for issue in analysis_result.issues
            ]
        }
        
        # Check for critical issues
        if analysis_result.has_critical_issues:
            critical_issues = [
                issue.description 
                for issue in analysis_result.issues 
                if issue.severity == "high"
            ]
            detailed_diagnostics["issues_found"].append("critical_issues")
            detailed_diagnostics["primary_rejection_reason"] = "critical_issues"
            return False, f"Critical issues detected: {', '.join(critical_issues)}", detailed_diagnostics
        
        # Check if the PDF is damaged
        if analysis_result.is_damaged:
            detailed_diagnostics["issues_found"].append("damaged")
            detailed_diagnostics["primary_rejection_reason"] = "damaged"
            return False, "PDF is damaged", detailed_diagnostics
        
        # Check if the PDF is encrypted without a password
        if analysis_result.is_encrypted:
            detailed_diagnostics["issues_found"].append("encrypted")
            detailed_diagnostics["primary_rejection_reason"] = "encrypted"
            return False, "PDF is encrypted", detailed_diagnostics
        
        # Check if the PDF has no pages
        if analysis_result.num_pages == 0:
            detailed_diagnostics["issues_found"].append("empty_document")
            detailed_diagnostics["primary_rejection_reason"] = "empty_document"
            return False, "PDF has no pages", detailed_diagnostics
        
        # Check for scanned images
        scanned_issues = [
            issue for issue in analysis_result.issues 
            if issue.type == PDFIssueType.SCANNED_IMAGE
        ]
        if scanned_issues:
            detailed_diagnostics["issues_found"].append("scanned_image")
            detailed_diagnostics["primary_rejection_reason"] = "likely_scanned_document"
            return True, "PDF may be scanned (will attempt extraction but may fail)", detailed_diagnostics
        
        # Check for unusual encoding
        encoding_issues = [
            issue for issue in analysis_result.issues 
            if issue.type in (PDFIssueType.ENCODING_ISSUE, PDFIssueType.UTF16_ENCODING, PDFIssueType.MIXED_ENCODINGS)
        ]
        if encoding_issues:
            detailed_diagnostics["issues_found"].append("encoding_issues")
            # This is not a rejection reason, just a warning
            return True, "PDF has encoding issues (will attempt extraction but may have character problems)", detailed_diagnostics
        
        # All checks passed
        return True, "PDF is acceptable for processing", detailed_diagnostics
    
    def get_pdf_metadata(self, analysis_result: PDFAnalysisResult) -> Dict[str, Any]:
        """
        Extract metadata from analysis results for storage.
        
        Args:
            analysis_result: PDFAnalysisResult from analyze_pdf
            
        Returns:
            Dict[str, Any]: Metadata dictionary
        """
        # Extract encoding types
        encoding_types = [et.name for et in analysis_result.encoding_types] if analysis_result.encoding_types else []
        
        # Extract font information
        fonts = []
        for font in analysis_result.fonts:
            fonts.append({
                "name": font.name,
                "type": font.type,
                "encoding": font.encoding.name,
                "embedded": font.embedded,
                "subset": font.subset
            })
        
        # Extract issues
        issues = []
        for issue in analysis_result.issues:
            issues.append({
                "type": issue.type.name,
                "description": issue.description,
                "severity": issue.severity,
                "page_numbers": issue.page_numbers,
                "details": issue.details
            })
        
        # Create metadata dictionary
        metadata = {
            "filepath": str(analysis_result.filepath),
            "filesize": analysis_result.filesize,
            "filesize_mb": round(analysis_result.filesize / 1024 / 1024, 2),
            "num_pages": analysis_result.num_pages,
            "is_encrypted": analysis_result.is_encrypted,
            "is_damaged": analysis_result.is_damaged,
            "encoding_types": encoding_types,
            "fonts": fonts,
            "issues": issues,
            "pdf_metadata": analysis_result.metadata
        }
        
        return metadata
