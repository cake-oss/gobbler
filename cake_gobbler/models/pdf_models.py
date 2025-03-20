# SPDX-FileCopyrightText: 2025 Cake AI Technologies, Inc.
#
# SPDX-License-Identifier: Apache-2.0

"""
PDF Analysis Models for the Cake Gobbler RAG system.

This module defines data models related to PDF analysis and processing.
"""

from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

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
