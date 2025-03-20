# SPDX-FileCopyrightText: 2025 Cake AI Technologies, Inc.
#
# SPDX-License-Identifier: Apache-2.0

"""
Database Models for the Cake Gobbler RAG system.

This module defines data models related to database operations.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any, Optional, List


@dataclass
class IngestionRecord:
    """Represents a record of a PDF ingestion in the database."""
    id: Optional[int] = None
    file_path: str = ""
    status: str = ""  # "success", "error", "skipped"
    error_message: str = ""
    issues: str = ""
    ingestion_time: str = ""
    encoding_types: str = ""
    is_encrypted: bool = False
    is_damaged: bool = False
    num_pages: int = 0
    filesize: int = 0
    fonts: str = ""
    analysis_result: str = ""
    run_id: Optional[str] = None
    file_fingerprint: Optional[str] = None


@dataclass
class RunRecord:
    """Represents a record of a processing run in the database."""
    run_id: str
    start_time: str
    end_time: Optional[str] = None
    status: str = "not_started"  # "not_started", "running", "completed", "completed_with_errors", "failed"
    total_files: int = 0
    processed_files: int = 0
    failed_files: int = 0
    skipped_files: int = 0
    already_processed_files: int = 0
    total_processing_time: Optional[float] = None
    
    # Essential metadata fields
    run_name: Optional[str] = None
    collection: Optional[str] = None
    embedding_model: Optional[str] = None
    chunk_size: Optional[int] = None
    chunk_overlap: Optional[int] = None


@dataclass
class RunStatistics:
    """Statistics for a processing run."""
    run_id: str
    start_time: str
    end_time: Optional[str]
    status: str
    total_files: int
    processed_files: int
    failed_files: int
    skipped_files: int
    already_processed_files: int
    completion_percentage: float
    processing_time_seconds: float
    
    # Essential metadata fields
    run_name: Optional[str] = None
    collection: Optional[str] = None
    embedding_model: Optional[str] = None
    chunk_size: Optional[int] = None
    chunk_overlap: Optional[int] = None
