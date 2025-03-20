# SPDX-FileCopyrightText: 2025 Cake AI Technologies, Inc.
#
# SPDX-License-Identifier: Apache-2.0

"""
PDF Encoding Analyzer - Core functionality for the Encoding Diagnosis Tool.

This module provides tools to analyze PDF files for encoding types,
password protection, and other common issues that might cause problems
during processing.
"""

import os
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import logging

# Import pikepdf for fallback analysis
import pikepdf
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# Import our PyMuPDF interface instead of direct fitz import
from cake_gobbler.utils.pymupdf_interface import PyMuPDFInterface


class EncodingType(Enum):
    """Enum representing different PDF encoding types."""
    ASCII = auto()
    UTF8 = auto()
    UTF8_WITH_BOM = auto()
    UTF16 = auto()
    UTF16BE = auto()     # UTF-16 Big Endian
    UTF16LE = auto()     # UTF-16 Little Endian
    UTF16BE_WITH_BOM = auto()  # UTF-16 Big Endian with BOM
    UTF16LE_WITH_BOM = auto()  # UTF-16 Little Endian with BOM
    IDENTITY_H = auto()  # CID fonts with Identity-H encoding
    WINANSI = auto()     # Windows ANSI encoding
    MACROMAN = auto()    # Mac OS Roman encoding
    LATIN1 = auto()      # ISO-8859-1 encoding
    CUSTOM = auto()      # Custom encoding
    UNKNOWN = auto()


class PDFIssueType(Enum):
    """Enum representing different types of PDF issues."""
    PASSWORD_PROTECTED = auto()
    DAMAGED = auto()
    ENCRYPTED = auto()
    MISSING_FONTS = auto()
    EMBEDDED_FILES = auto()
    JAVASCRIPT = auto()
    FORM_FIELDS = auto()
    DIGITAL_SIGNATURES = auto()
    LARGE_SIZE = auto()
    HIGH_COMPRESSION = auto()
    SCANNED_IMAGE = auto()
    WATERMARK = auto()
    CUSTOM_METADATA = auto()
    UNUSUAL_STRUCTURE = auto()
    ENCODING_ISSUE = auto()
    UTF16_ENCODING = auto()
    MIXED_ENCODINGS = auto()


@dataclass
class FontInfo:
    """Information about a font used in the PDF."""
    name: str
    type: str
    encoding: EncodingType
    embedded: bool
    subset: bool


@dataclass
class PDFIssue:
    """Represents an issue found in a PDF file."""
    type: PDFIssueType
    description: str
    severity: str  # "low", "medium", "high"
    page_numbers: List[int] = field(default_factory=list)
    details: Dict[str, str] = field(default_factory=dict)


@dataclass
class PDFAnalysisResult:
    """Results of a PDF encoding and structure analysis."""
    filepath: Path
    filesize: int
    num_pages: int
    is_encrypted: bool
    is_damaged: bool
    encoding_types: Set[EncodingType] = field(default_factory=set)
    fonts: List[FontInfo] = field(default_factory=list)
    issues: List[PDFIssue] = field(default_factory=list)
    metadata: Dict[str, str] = field(default_factory=dict)
    
    @property
    def has_issues(self) -> bool:
        """Check if the PDF has any issues."""
        return len(self.issues) > 0
    
    @property
    def has_critical_issues(self) -> bool:
        """Check if the PDF has any critical issues."""
        return any(issue.severity == "high" for issue in self.issues)
    
    def get_issues_by_type(self, issue_type: PDFIssueType) -> List[PDFIssue]:
        """Get all issues of a specific type."""
        return [issue for issue in self.issues if issue.type == issue_type]
    
    def get_issues_by_severity(self, severity: str) -> List[PDFIssue]:
        """Get all issues of a specific severity."""
        return [issue for issue in self.issues if issue.severity == severity]
    
    def print_report(self, console: Optional[Console] = None) -> None:
        """Print a formatted report of the analysis results."""
        if console is None:
            console = Console()
            
        # Basic file info
        file_info = Table.grid(padding=(0, 1))
        file_info.add_column(style="bold")
        file_info.add_column()
        
        file_info.add_row("File:", str(self.filepath))
        file_info.add_row("Size:", f"{self.filesize / 1024 / 1024:.2f} MB")
        file_info.add_row("Pages:", str(self.num_pages))
        file_info.add_row("Encrypted:", "Yes" if self.is_encrypted else "No")
        file_info.add_row("Damaged:", "Yes" if self.is_damaged else "No")
        
        console.print(Panel(file_info, title="PDF File Information", expand=False))
        
        # Encoding types
        console.print("\n[bold]Encoding Types:[/bold]")
        if self.encoding_types:
            for enc_type in self.encoding_types:
                console.print(f"  • {enc_type.name}")
        else:
            console.print("  • No specific encoding types detected")
        
        # Fonts
        console.print("\n[bold]Fonts:[/bold]")
        if self.fonts:
            font_table = Table()
            font_table.add_column("Name", style="cyan")
            font_table.add_column("Type", style="green")
            font_table.add_column("Encoding", style="yellow")
            font_table.add_column("Embedded", style="magenta")
            font_table.add_column("Subset", style="blue")
            
            for font in self.fonts:
                font_table.add_row(
                    font.name,
                    font.type,
                    font.encoding.name,
                    "Yes" if font.embedded else "No",
                    "Yes" if font.subset else "No"
                )
            
            console.print(font_table)
        else:
            console.print("  • No fonts detected or extracted")
        
        # Issues
        if self.issues:
            issue_table = Table(title="Issues")
            issue_table.add_column("Type", style="cyan")
            issue_table.add_column("Description")
            issue_table.add_column("Severity", style="bold")
            issue_table.add_column("Pages")
            
            for issue in self.issues:
                pages = ", ".join(str(p) for p in issue.page_numbers) if issue.page_numbers else "All"
                severity_style = {
                    "low": "green",
                    "medium": "yellow",
                    "high": "red bold"
                }.get(issue.severity, "")
                
                issue_table.add_row(
                    issue.type.name,
                    issue.description,
                    f"[{severity_style}]{issue.severity.upper()}[/{severity_style}]",
                    pages
                )
            
            console.print("\n")
            console.print(issue_table)
        
        # Metadata
        if self.metadata:
            metadata_table = Table.grid(padding=(0, 1))
            metadata_table.add_column(style="bold")
            metadata_table.add_column()
            
            for key, value in self.metadata.items():
                metadata_table.add_row(key + ":", value)
            
            console.print("\n")
            console.print(Panel(metadata_table, title="Metadata", expand=False))
        
        # Summary
        summary = []
        if self.has_critical_issues:
            summary.append("[bold red]This PDF has critical issues that may prevent proper processing.[/bold red]")
        elif self.has_issues:
            summary.append("[bold yellow]This PDF has some issues that might affect processing.[/bold yellow]")
        else:
            summary.append("[bold green]This PDF appears to be well-formed with no significant issues.[/bold green]")
        
        console.print("\n")
        console.print(Panel("\n".join(summary), title="Summary", expand=False))


class PDFAnalyzer:
    """
    Analyzer for PDF encoding types and structure.
    
    This class provides methods to analyze PDF files for encoding types,
    password protection, and other common issues that might cause problems
    during processing.
    """
    
    # Constants for encoding detection
    ENCODING_CONFIDENCE_THRESHOLD = 0.7
    
    def __init__(self, verbose: bool = False):
        """
        Initialize the PDF Encoding Analyzer.
        
        Args:
            verbose: Whether to print verbose output during analysis.
        """
        self.verbose = verbose
        self.console = Console()
        self.logger = logging.getLogger("cake-gobbler.pdf_analyzer")
        
        # Create PyMuPDF interface
        self.pymupdf = PyMuPDFInterface(verbose=verbose)
        
        if verbose:
            self.console.print("[bold yellow]Running in verbose mode[/bold yellow]")
            # Print directly to stdout for maximum visibility
            print("VERBOSE MODE ENABLED - Debug information will be printed directly to console")
    
    def analyze_file(self, filepath: str | Path, password: Optional[str] = None) -> PDFAnalysisResult:
        """
        Analyze a PDF file for encoding types and potential issues.
        
        Args:
            filepath: Path to the PDF file.
            password: Password for encrypted PDFs (if applicable).
            
        Returns:
            PDFAnalysisResult: Analysis results.
        """
        filepath = Path(filepath)
        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")
        
        if self.verbose:
            print(f"DEBUG: Analyzing file: {filepath}")
        
        filesize = filepath.stat().st_size
        
        # Initialize result
        result = PDFAnalysisResult(
            filepath=filepath,
            filesize=filesize,
            num_pages=0,
            is_encrypted=False,
            is_damaged=False
        )
        
        # Check if file is a valid PDF
        if not self._is_valid_pdf(filepath):
            result.is_damaged = True
            result.issues.append(PDFIssue(
                type=PDFIssueType.DAMAGED,
                description="File does not appear to be a valid PDF",
                severity="high"
            ))
            return result
        
        # Use PyMuPDF client to analyze PDF
        try:
            analysis = self.pymupdf.analyze_pdf(str(filepath), password)
            
            if not analysis.get("success", False):
                # If PyMuPDF fails, try with pikepdf
                try:
                    self._analyze_with_pikepdf(filepath, result, password)
                    return result
                except Exception as e2:
                    result.is_damaged = True
                    result.issues.append(PDFIssue(
                        type=PDFIssueType.DAMAGED,
                        description=f"Failed to analyze PDF: {analysis.get('error', 'Unknown error')}; {str(e2)}",
                        severity="high"
                    ))
                    return result
            
            # Process results from PyMuPDF client
            result.num_pages = analysis.get("num_pages", 0)
            result.is_encrypted = analysis.get("is_encrypted", False)
            result.is_damaged = analysis.get("is_damaged", False)
            
            # Process metadata
            if "metadata" in analysis and analysis["metadata"]:
                result.metadata = analysis["metadata"]
            
            # Process encoding types
            for enc_type_str in analysis.get("encoding_types", []):
                try:
                    # Convert string encoding type to enum
                    enc_type = EncodingType[enc_type_str]
                    result.encoding_types.add(enc_type)
                except (KeyError, ValueError):
                    if self.verbose:
                        print(f"DEBUG: Unknown encoding type: {enc_type_str}")
            
            # Process fonts
            for font_dict in analysis.get("fonts", []):
                try:
                    font_name = font_dict.get("name", "Unknown")
                    font_type = font_dict.get("type", "Unknown")
                    encoding_str = font_dict.get("encoding", "UNKNOWN")
                    embedded = font_dict.get("embedded", False)
                    subset = font_dict.get("subset", False)
                    
                    # Convert string encoding to enum
                    try:
                        encoding = EncodingType[encoding_str]
                    except (KeyError, ValueError):
                        encoding = EncodingType.UNKNOWN
                    
                    # Add font info
                    result.fonts.append(FontInfo(
                        name=font_name,
                        type=font_type,
                        encoding=encoding,
                        embedded=embedded,
                        subset=subset
                    ))
                except Exception as e:
                    if self.verbose:
                        print(f"DEBUG ERROR: Failed to process font: {str(e)}")
            
            # Process issues
            for issue_dict in analysis.get("issues", []):
                try:
                    issue_type_str = issue_dict.get("type", "UNKNOWN")
                    description = issue_dict.get("description", "Unknown issue")
                    severity = issue_dict.get("severity", "medium")
                    page_numbers = issue_dict.get("page_numbers", [])
                    details = issue_dict.get("details", {})
                    
                    # Convert string issue type to enum
                    try:
                        issue_type = PDFIssueType[issue_type_str]
                    except (KeyError, ValueError):
                        if self.verbose:
                            print(f"DEBUG ERROR: Unknown issue type: {issue_type_str}")
                        continue
                    
                    # Add issue
                    result.issues.append(PDFIssue(
                        type=issue_type,
                        description=description,
                        severity=severity,
                        page_numbers=page_numbers,
                        details=details
                    ))
                except Exception as e:
                    if self.verbose:
                        print(f"DEBUG ERROR: Failed to process issue: {str(e)}")
        
        except Exception as e:
            # If PyMuPDF client fails completely, try with pikepdf
            try:
                self._analyze_with_pikepdf(filepath, result, password)
            except Exception as e2:
                result.is_damaged = True
                result.issues.append(PDFIssue(
                    type=PDFIssueType.DAMAGED,
                    description=f"Failed to analyze PDF: {str(e)}; {str(e2)}",
                    severity="high"
                ))
        
        return result
    
    def _is_valid_pdf(self, filepath: Path) -> bool:
        """Check if the file is a valid PDF."""
        try:
            with open(filepath, 'rb') as f:
                header = f.read(1024)
                # Check for PDF signature
                return header.startswith(b'%PDF-')
        except Exception:
            return False
    
    def _analyze_with_pikepdf(self, filepath: Path, result: PDFAnalysisResult, password: Optional[str] = None) -> None:
        """Analyze PDF using pikepdf when PyMuPDF client fails."""
        try:
            with pikepdf.open(filepath, password=password or '') as pdf:
                result.num_pages = len(pdf.pages)
                
                # Extract metadata
                if pdf.docinfo:
                    result.metadata = {k: str(v) for k, v in pdf.docinfo.items()}
                
                # Check for embedded files
                if '/EmbeddedFiles' in pdf.Root:
                    result.issues.append(PDFIssue(
                        type=PDFIssueType.EMBEDDED_FILES,
                        description="PDF contains embedded files",
                        severity="medium"
                    ))
                
                # Check for unusual structure
                if len(pdf.objects) > result.num_pages * 10:
                    result.issues.append(PDFIssue(
                        type=PDFIssueType.UNUSUAL_STRUCTURE,
                        description=f"PDF has an unusually complex structure ({len(pdf.objects)} objects)",
                        severity="medium"
                    ))
                
                # Extract fonts from the PDF using pikepdf
                self._extract_fonts_with_pikepdf(pdf, result)
                
        except pikepdf.PasswordError:
            result.is_encrypted = True
            result.issues.append(PDFIssue(
                type=PDFIssueType.PASSWORD_PROTECTED,
                description="PDF is password protected",
                severity="high"
            ))
    
    def _extract_fonts_with_pikepdf(self, pdf: pikepdf.Pdf, result: PDFAnalysisResult) -> None:
        """Extract fonts from the PDF using pikepdf."""
        if self.verbose:
            print(f"DEBUG: Extracting fonts with pikepdf")
        
        font_set = set()  # Track unique fonts
        
        # Process each page
        for page_num, page in enumerate(pdf.pages):
            if '/Resources' not in page:
                continue
                
            resources = page.Resources
            if '/Font' not in resources:
                continue
                
            fonts = resources.Font
            for font_key in fonts.keys():
                try:
                    font_name = str(font_key)
                    font = fonts[font_key]
                    
                    # Get font properties
                    font_type = str(font.get('/Subtype', '')) if '/Subtype' in font else 'Unknown'
                    base_font = str(font.get('/BaseFont', '')) if '/BaseFont' in font else 'Unknown'
                    encoding_str = str(font.get('/Encoding', '')) if '/Encoding' in font else 'Unknown'
                    
                    # Skip if we've already processed this font
                    if base_font in font_set:
                        continue
                    
                    font_set.add(base_font)
                    
                    if self.verbose:
                        print(f"DEBUG: Found font on page {page_num+1}: {base_font}, Type: {font_type}, Encoding: {encoding_str}")
                    
                    # Determine encoding type
                    encoding = EncodingType.UNKNOWN
                    if 'Identity-H' in encoding_str:
                        encoding = EncodingType.IDENTITY_H
                        result.encoding_types.add(EncodingType.IDENTITY_H)
                    elif 'WinAnsi' in encoding_str:
                        encoding = EncodingType.WINANSI
                        result.encoding_types.add(EncodingType.WINANSI)
                    elif 'MacRoman' in encoding_str:
                        encoding = EncodingType.MACROMAN
                        result.encoding_types.add(EncodingType.MACROMAN)
                    elif 'Custom' in encoding_str:
                        encoding = EncodingType.CUSTOM
                        result.encoding_types.add(EncodingType.CUSTOM)
                    
                    # Check if font is embedded
                    is_embedded = False
                    is_subset = False
                    
                    if '/FontDescriptor' in font:
                        font_descriptor = font.FontDescriptor
                        if '/FontFile' in font_descriptor or '/FontFile2' in font_descriptor or '/FontFile3' in font_descriptor:
                            is_embedded = True
                    
                    # Check if font is subset (name starts with a prefix like ABCDEF+)
                    if '+' in base_font:
                        is_subset = True
                    
                    # Add font info
                    result.fonts.append(FontInfo(
                        name=base_font,
                        type=font_type,
                        encoding=encoding,
                        embedded=is_embedded,
                        subset=is_subset
                    ))
                    
                    # Check for missing fonts
                    if not is_embedded and font_type not in ('/Type1', '/MMType1'):
                        result.issues.append(PDFIssue(
                            type=PDFIssueType.MISSING_FONTS,
                            description=f"Non-embedded font: {base_font}",
                            severity="medium",
                            page_numbers=[page_num]
                        ))
                except Exception as e:
                    if self.verbose:
                        print(f"DEBUG ERROR: Failed to process font {font_key} on page {page_num+1}: {str(e)}")
    
    def _detect_encoding_with_chardet(self, text_bytes: bytes) -> Tuple[str, float, EncodingType]:
        """
        Detect text encoding using chardet.
        
        Args:
            text_bytes: Bytes to analyze
            
        Returns:
            Tuple of (encoding_name, confidence, encoding_type)
        """
        import chardet
        
        # First check for BOM
        if text_bytes.startswith(b'\xFE\xFF'):
            return ('UTF-16BE', 1.0, EncodingType.UTF16BE_WITH_BOM)
        elif text_bytes.startswith(b'\xFF\xFE'):
            return ('UTF-16LE', 1.0, EncodingType.UTF16LE_WITH_BOM)
        elif text_bytes.startswith(b'\xEF\xBB\xBF'):
            return ('UTF-8', 1.0, EncodingType.UTF8_WITH_BOM)
        
        # If no BOM, use chardet
        result = chardet.detect(text_bytes)
        encoding_name = result['encoding']
        confidence = result['confidence']
        
        # Map the encoding name to our EncodingType enum
        encoding_type = EncodingType.UNKNOWN
        if encoding_name:
            encoding_name = encoding_name.upper()
            if encoding_name == 'ASCII':
                encoding_type = EncodingType.ASCII
            elif encoding_name == 'UTF-8':
                encoding_type = EncodingType.UTF8
            elif encoding_name == 'UTF-16BE':
                encoding_type = EncodingType.UTF16BE
            elif encoding_name == 'UTF-16LE':
                encoding_type = EncodingType.UTF16LE
            elif encoding_name == 'ISO-8859-1' or encoding_name == 'LATIN1':
                encoding_type = EncodingType.LATIN1
            else:
                encoding_type = EncodingType.CUSTOM
        
        # If confidence is low or no encoding detected, check for UTF-16 patterns
        if not encoding_name or confidence < self.ENCODING_CONFIDENCE_THRESHOLD:
            utf16_result = self._check_utf16_pattern(text_bytes)
            if utf16_result:
                return utf16_result
        
        return (encoding_name, confidence, encoding_type)
    
    def _check_utf16_pattern(self, text_bytes: bytes) -> Optional[Tuple[str, float, EncodingType]]:
        """
        Check for UTF-16 encoding by examining byte patterns.
        
        Args:
            text_bytes: Bytes to analyze
            
        Returns:
            Tuple of (encoding_name, confidence, encoding_type) or None if not UTF-16
        """
        # Need at least a few bytes to check
        if len(text_bytes) < 4:
            return None
        
        # Check for BE pattern (null byte, char, null byte, char)
        # Take a sample of the first 100 bytes or less
        sample_size = min(100, len(text_bytes))
        
        # Count null bytes at even and odd positions
        even_nulls = sum(1 for i in range(0, sample_size, 2) if i < len(text_bytes) and text_bytes[i] == 0)
        odd_nulls = sum(1 for i in range(1, sample_size, 2) if i < len(text_bytes) and text_bytes[i] == 0)
        
        # Calculate the percentage of null bytes at each position
        even_null_percentage = even_nulls / ((sample_size + 1) // 2)
        odd_null_percentage = odd_nulls / (sample_size // 2)
        
        # If high percentage of nulls at even positions, likely UTF-16BE
        if even_null_percentage > self.ENCODING_CONFIDENCE_THRESHOLD:
            return ('UTF-16BE', 0.9, EncodingType.UTF16BE)
        
        # If high percentage of nulls at odd positions, likely UTF-16LE
        if odd_null_percentage > self.ENCODING_CONFIDENCE_THRESHOLD:
            return ('UTF-16LE', 0.9, EncodingType.UTF16LE)
        
        return None
