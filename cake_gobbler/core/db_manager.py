# SPDX-FileCopyrightText: 2025 Cake AI Technologies, Inc.
#
# SPDX-License-Identifier: Apache-2.0

"""
Database Manager for the Cake Gobbler RAG system.

This module handles all database operations, including schema creation,
logging ingestion results, storing detailed analysis data,
and managing run information.
"""

import json
import os
import sqlite3
import datetime
from typing import Dict, List, Optional, Any, Union, Tuple

from cake_gobbler.models.db_models import IngestionRecord, RunRecord
from cake_gobbler.models.pdf_models import PDFAnalysisResult


class DatabaseManager:
    """
    Manages database operations for the PDF ingestion system.
    """
    
    def __init__(self, db_path: str):
        """
        Initialize the database manager.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.init_db()
    
    def init_db(self):
        """
        Initialize the database connection and create tables if they don't exist.
        """
        # Check if database file exists, if not, we need to create it from scratch
        db_exists = os.path.exists(self.db_path)
        
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        
        # If database file didn't exist or we're in a migration scenario, 
        # handle potential schema changes
        if not db_exists:
            print(f"Creating new database at {self.db_path}")
        
        # Create the runs table with extracted columns
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                start_time TEXT,
                end_time TEXT,
                status TEXT,
                total_files INTEGER,
                processed_files INTEGER,
                failed_files INTEGER,
                skipped_files INTEGER,
                already_processed_files INTEGER,
                total_processing_time REAL,
                
                -- Essential metadata fields
                run_name TEXT,
                collection TEXT,
                embedding_model TEXT,
                chunk_size INTEGER,
                chunk_overlap INTEGER
            )
        ''')
        
        # Create the enhanced ingestion_log table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS ingestion_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT,
                collection TEXT,
                status TEXT,
                error_message TEXT,
                issues TEXT,
                ingestion_time TEXT,
                
                -- New fields for enhanced analysis data
                encoding_types TEXT,
                is_encrypted INTEGER,
                is_damaged INTEGER,
                num_pages INTEGER,
                filesize INTEGER,
                
                -- Detailed analysis results
                analysis_result TEXT,
                
                -- Run reference
                run_id TEXT REFERENCES runs(run_id),
                
                -- File fingerprint for tracking and resumption
                file_fingerprint TEXT
            )
        ''')
        
        # Add index for fingerprint lookups
        self.cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_file_fingerprint 
            ON ingestion_log(file_fingerprint)
        ''')
        
        # Add index for file path lookups
        self.cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_file_path 
            ON ingestion_log(file_path)
        ''')
        
        self.conn.commit()
        
    def log_ingestion(self, file_path: str, collection: str, status: str, analysis_result=None, error_message: str = "", 
                   issues: str = "", run_id: str = None, file_fingerprint: str = None):
        """
        Log an ingestion attempt with detailed analysis results.
        
        Args:
            file_path: Path to the PDF file
            status: Status of the ingestion (success, error, skipped)
            analysis_result: PDFAnalysisResult object from the analyzer
            error_message: Error message if any
            issues: JSON string of issues if any
            run_id: Optional run ID to associate with this ingestion
            file_fingerprint: Unique fingerprint of the file for tracking changes
        """
        timestamp = datetime.datetime.now().isoformat()
        
        # Default values for analysis fields
        encoding_types = None
        is_encrypted = 0
        is_damaged = 0
        num_pages = 0
        filesize = 0
        analysis_json = None
        
        # If analysis_result is provided, extract the detailed information
        if analysis_result:
            # Convert encoding types enum to strings
            encoding_types_list = [et.name for et in analysis_result.encoding_types] if analysis_result.encoding_types else []
            encoding_types = json.dumps(encoding_types_list)
            
            # Convert boolean to integer for SQLite
            is_encrypted = 1 if analysis_result.is_encrypted else 0
            is_damaged = 1 if analysis_result.is_damaged else 0
            
            # Basic file info
            num_pages = analysis_result.num_pages
            filesize = analysis_result.filesize
            
            # Convert the entire analysis result to JSON
            # We need to create a serializable dict from the analysis_result
            analysis_dict = {
                "filepath": str(analysis_result.filepath),
                "filesize": analysis_result.filesize,
                "num_pages": analysis_result.num_pages,
                "is_encrypted": analysis_result.is_encrypted,
                "is_damaged": analysis_result.is_damaged,
                "encoding_types": encoding_types_list,
                "issues": [
                    {
                        "type": issue.type.name,
                        "description": issue.description,
                        "severity": issue.severity,
                        "page_numbers": issue.page_numbers,
                        "details": issue.details
                    }
                    for issue in analysis_result.issues
                ],
                "metadata": analysis_result.metadata
            }
            analysis_json = json.dumps(analysis_dict)
        
        # Insert the record with all the detailed information
        self.cursor.execute('''
            INSERT INTO ingestion_log (
                file_path, collection, status, error_message, issues, ingestion_time,
                encoding_types, is_encrypted, is_damaged, num_pages, filesize,
                analysis_result, run_id, file_fingerprint
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            file_path, collection, status, error_message, issues, timestamp,
            encoding_types, is_encrypted, is_damaged, num_pages, filesize,
            analysis_json, run_id, file_fingerprint
        ))
        
        # Commit the transaction
        self.conn.commit()
    
    def get_ingestion_log(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get the most recent ingestion log entries.
        
        Args:
            limit: Maximum number of entries to return
            
        Returns:
            List of dictionaries containing the log entries
        """
        self.cursor.execute('''
            SELECT * FROM ingestion_log
            ORDER BY id DESC
            LIMIT ?
        ''', (limit,))
        
        columns = [column[0] for column in self.cursor.description]
        results = []
        
        for row in self.cursor.fetchall():
            result = dict(zip(columns, row))
            results.append(result)
        
        return results
    
    def get_ingestion_by_file(self, file_path: str, collection_name: str = None) -> Optional[Dict[str, Any]]:
        """
        Get the most recent ingestion log entry for a specific file and collection.
        
        Args:
            file_path: Path to the PDF file
            collection_name: The collection name to check for
            
        Returns:
            Dictionary containing the log entry or None if not found
        """
        if collection_name is None:
            # If no collection specified, just return the most recent entry for this file
            self.cursor.execute('''
                SELECT * FROM ingestion_log
                WHERE file_path = ?
                ORDER BY id DESC
                LIMIT 1
            ''', (file_path,))
        else:
            # If collection is specified, join with runs table to check collection
            self.cursor.execute('''
                SELECT il.* FROM ingestion_log il
                JOIN runs r ON il.run_id = r.run_id
                WHERE il.file_path = ?
                AND r.collection = ?
                ORDER BY il.id DESC
                LIMIT 1
            ''', (file_path, collection_name))
        
        row = self.cursor.fetchone()
        if not row:
            return None
        
        columns = [column[0] for column in self.cursor.description]
        return dict(zip(columns, row))
    
    def get_files_with_encoding_type(self, encoding_type: str) -> List[Dict[str, Any]]:
        """
        Get all files that have a specific encoding type.
        
        Args:
            encoding_type: Name of the encoding type to search for
            
        Returns:
            List of dictionaries containing the log entries
        """
        # We need to search for the encoding type in the JSON array
        search_pattern = f'%"{encoding_type}"%'
        
        self.cursor.execute('''
            SELECT * FROM ingestion_log
            WHERE encoding_types LIKE ?
            ORDER BY id DESC
        ''', (search_pattern,))
        
        columns = [column[0] for column in self.cursor.description]
        results = []
        
        for row in self.cursor.fetchall():
            result = dict(zip(columns, row))
            results.append(result)
        
        return results
    
    def get_files_with_font_type(self, font_type: str) -> List[Dict[str, Any]]:
        """
        Get all files that have a specific font type.
        
        Args:
            font_type: Type of font to search for
            
        Returns:
            List of dictionaries containing the log entries
        """
        # We need to search for the font type in the JSON array
        search_pattern = f'%"type": "{font_type}"%'
        
        self.cursor.execute('''
            SELECT * FROM ingestion_log
            WHERE fonts LIKE ?
            ORDER BY id DESC
        ''', (search_pattern,))
        
        columns = [column[0] for column in self.cursor.description]
        results = []
        
        for row in self.cursor.fetchall():
            result = dict(zip(columns, row))
            results.append(result)
        
        return results
    
    def get_files_with_issue_type(self, issue_type: str) -> List[Dict[str, Any]]:
        """
        Get all files that have a specific issue type.
        
        Args:
            issue_type: Type of issue to search for
            
        Returns:
            List of dictionaries containing the log entries
        """
        # We need to search for the issue type in the JSON array
        search_pattern = f'%"type": "{issue_type}"%'
        
        self.cursor.execute('''
            SELECT * FROM ingestion_log
            WHERE analysis_result LIKE ?
            ORDER BY id DESC
        ''', (search_pattern,))
        
        columns = [column[0] for column in self.cursor.description]
        results = []
        
        for row in self.cursor.fetchall():
            result = dict(zip(columns, row))
            results.append(result)
        
        return results
    
    def create_run(self, run_id: str, start_time: str, total_files: int = 0, 
                 run_record: RunRecord = None, metadata: Dict[str, Any] = None) -> None:
        """
        Create a new run record in the database.
        
        Args:
            run_id: Unique identifier for the run
            start_time: ISO-formatted start time
            total_files: Total number of files to process
            run_record: RunRecord object with all fields set
            metadata: Dictionary of metadata to extract values from
        """
        # Default values
        run_name = None
        collection = None
        embedding_model = None
        chunk_size = None
        chunk_overlap = None
        
        # If run_record is provided, use its values
        if run_record:
            run_name = run_record.run_name
            collection = run_record.collection
            embedding_model = run_record.embedding_model
            chunk_size = run_record.chunk_size
            chunk_overlap = run_record.chunk_overlap
        # Otherwise try to extract values from metadata
        elif metadata:
            run_name = metadata.get("run_name")
            
            # Extract command line args
            cmd_args = metadata.get("command_line_args", {})
            if cmd_args:
                collection = cmd_args.get("collection")
                embedding_model = cmd_args.get("embedding_model")
                chunk_size = cmd_args.get("chunk_size")
                chunk_overlap = cmd_args.get("chunk_overlap")
        
        self.cursor.execute('''
            INSERT INTO runs (
                run_id, start_time, status, total_files,
                processed_files, failed_files, skipped_files, already_processed_files,
                run_name, collection, embedding_model,
                chunk_size, chunk_overlap
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            run_id, start_time, "running", total_files,
            0, 0, 0, 0,
            run_name, collection, embedding_model,
            chunk_size, chunk_overlap
        ))
        self.conn.commit()
    
    def update_run(self, run_id: str, end_time: str = None, status: str = None,
                  processed_files: int = None, failed_files: int = None,
                  skipped_files: int = None, already_processed_files: int = None,
                  total_processing_time: float = None, run_record: RunRecord = None,
                  metadata: Dict[str, Any] = None) -> None:
        """
        Update an existing run record in the database.
        
        Args:
            run_id: Unique identifier for the run
            end_time: ISO-formatted end time
            status: Run status
            processed_files: Number of successfully processed files
            failed_files: Number of failed files
            skipped_files: Number of skipped files
            already_processed_files: Number of already processed files
            total_processing_time: Total processing time in seconds
            run_record: RunRecord object with fields to update
            metadata: Dictionary of metadata to extract values from
        """
        # Build the SET clause and parameters dynamically based on provided values
        set_clauses = []
        params = []
        
        if end_time is not None:
            set_clauses.append("end_time = ?")
            params.append(end_time)
        
        if status is not None:
            set_clauses.append("status = ?")
            params.append(status)
        
        if processed_files is not None:
            set_clauses.append("processed_files = ?")
            params.append(processed_files)
        
        if failed_files is not None:
            set_clauses.append("failed_files = ?")
            params.append(failed_files)
        
        if skipped_files is not None:
            set_clauses.append("skipped_files = ?")
            params.append(skipped_files)
            
        if already_processed_files is not None:
            set_clauses.append("already_processed_files = ?")
            params.append(already_processed_files)
        
        if total_processing_time is not None:
            set_clauses.append("total_processing_time = ?")
            params.append(total_processing_time)
        
        # Update specific metadata fields from the run_record
        if run_record:
            if run_record.run_name is not None:
                set_clauses.append("run_name = ?")
                params.append(run_record.run_name)
            
            if run_record.collection is not None:
                set_clauses.append("collection = ?")
                params.append(run_record.collection)
                
            if run_record.embedding_model is not None:
                set_clauses.append("embedding_model = ?")
                params.append(run_record.embedding_model)
                
            if run_record.chunk_size is not None:
                set_clauses.append("chunk_size = ?")
                params.append(run_record.chunk_size)
                
            if run_record.chunk_overlap is not None:
                set_clauses.append("chunk_overlap = ?")
                params.append(run_record.chunk_overlap)
        
        # If metadata dictionary is provided, extract fields and update them
        elif metadata is not None:
            # Update run_name if present
            if "run_name" in metadata:
                set_clauses.append("run_name = ?")
                params.append(metadata["run_name"])
            
            # Extract command line args
            cmd_args = metadata.get("command_line_args", {})
            if cmd_args:
                if "collection" in cmd_args:
                    set_clauses.append("collection = ?")
                    params.append(cmd_args["collection"])
                    
                if "embedding_model" in cmd_args:
                    set_clauses.append("embedding_model = ?")
                    params.append(cmd_args["embedding_model"])
                    
                if "chunk_size" in cmd_args:
                    set_clauses.append("chunk_size = ?")
                    params.append(cmd_args["chunk_size"])
                    
                if "chunk_overlap" in cmd_args:
                    set_clauses.append("chunk_overlap = ?")
                    params.append(cmd_args["chunk_overlap"])
                    
            # Handle already_processed_files
            if "already_processed_files" in metadata and already_processed_files is None:
                set_clauses.append("already_processed_files = ?")
                params.append(metadata["already_processed_files"])
        
        # If no parameters were provided, return early
        if not set_clauses:
            return
        
        # Build and execute the SQL query
        query = f"UPDATE runs SET {', '.join(set_clauses)} WHERE run_id = ?"
        params.append(run_id)
        
        self.cursor.execute(query, params)
        self.conn.commit()
    
    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific run record from the database.
        
        Args:
            run_id: Unique identifier for the run
            
        Returns:
            Dict[str, Any]: Run record or None if not found
        """
        self.cursor.execute('''
            SELECT * FROM runs
            WHERE run_id = ?
        ''', (run_id,))
        
        row = self.cursor.fetchone()
        if not row:
            return None
        
        columns = [column[0] for column in self.cursor.description]
        return dict(zip(columns, row))
        
    def get_run_record(self, run_id: str) -> Optional[RunRecord]:
        """
        Get a specific run record as a RunRecord object.
        
        Args:
            run_id: Unique identifier for the run
            
        Returns:
            RunRecord: Run record object or None if not found
        """
        run_dict = self.get_run(run_id)
        if not run_dict:
            return None
        
        # Create RunRecord from the dictionary
        record = RunRecord(
            run_id=run_dict["run_id"],
            start_time=run_dict["start_time"],
            end_time=run_dict.get("end_time"),
            status=run_dict.get("status", "not_started"),
            total_files=run_dict.get("total_files", 0),
            processed_files=run_dict.get("processed_files", 0),
            failed_files=run_dict.get("failed_files", 0),
            skipped_files=run_dict.get("skipped_files", 0),
            already_processed_files=run_dict.get("already_processed_files", 0),
            total_processing_time=run_dict.get("total_processing_time"),
            
            # Metadata fields
            run_name=run_dict.get("run_name"),
            collection=run_dict.get("collection"),
            embedding_model=run_dict.get("embedding_model"),
            chunk_size=run_dict.get("chunk_size"),
            chunk_overlap=run_dict.get("chunk_overlap")
        )
        
        return record
    
    def get_all_runs(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get all run records from the database.
        
        Args:
            limit: Maximum number of records to return
            
        Returns:
            List[Dict[str, Any]]: List of run records
        """
        self.cursor.execute('''
            SELECT * FROM runs
            ORDER BY start_time DESC
            LIMIT ?
        ''', (limit,))
        
        columns = [column[0] for column in self.cursor.description]
        results = []
        
        for row in self.cursor.fetchall():
            result = dict(zip(columns, row))
            results.append(result)
        
        return results
    
    def get_ingestions_for_run(self, run_id: str) -> List[Dict[str, Any]]:
        """
        Get all ingestion records for a specific run.
        
        Args:
            run_id: Unique identifier for the run
            
        Returns:
            List[Dict[str, Any]]: List of ingestion records
        """
        self.cursor.execute('''
            SELECT * FROM ingestion_log
            WHERE run_id = ?
            ORDER BY ingestion_time ASC
        ''', (run_id,))
        
        columns = [column[0] for column in self.cursor.description]
        results = []
        
        for row in self.cursor.fetchall():
            result = dict(zip(columns, row))
            results.append(result)
        
        return results
    
    def get_ingestion_by_fingerprint(self, file_fingerprint: str, collection_name: str) -> Optional[Dict[str, Any]]:
        """
        Get the most recent ingestion log entry for a specific file fingerprint and collection.
        
        Args:
            file_fingerprint: Unique fingerprint of the file
            collection_name: The collection name to check
            
        Returns:
            Dictionary containing the log entry or None if not found
        """
        # Join with runs table to check collection
        self.cursor.execute('''
            SELECT il.* FROM ingestion_log il
            JOIN runs r ON il.run_id = r.run_id
            WHERE il.file_fingerprint = ?
            AND r.collection = ?
            ORDER BY il.id DESC
            LIMIT 1
        ''', (file_fingerprint, collection_name))
        
        row = self.cursor.fetchone()
        if not row:
            return None
        
        columns = [column[0] for column in self.cursor.description]
        return dict(zip(columns, row))
    
    def file_needs_processing(self, file_fingerprint: str, collection_name: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Determine if a file needs processing based on its fingerprint and collection name.
        
        Args:
            file_path: Path to the file
            file_fingerprint: Unique fingerprint of the file
            collection_name: The Weaviate collection name for this ingestion
            
        Returns:
            Tuple[bool, Optional[Dict[str, Any]]]: 
                - Boolean indicating if the file needs processing
                - Previous ingestion record if it exists, otherwise None
        """
        # First try to find by fingerprint and collection
        previous_ingestion = self.get_ingestion_by_fingerprint(file_fingerprint, collection_name)
        
        # If found, no need to process again for this collection. Note: this does not check to see if the status was successful, only that the file was ingested.
        if previous_ingestion:
            return False, previous_ingestion
        else:
            return True, None
        
    def close(self):
        """
        Close the database connection.
        """
        if self.cursor:
            self.cursor.close()
            self.cursor = None
            
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_all_records(self, collection_name: str) -> List[Dict[str, Any]]:
        """
        Get all records for a specific collection.
        """
        self.cursor.execute('''
            SELECT * FROM ingestion_log
            WHERE collection = ?
        ''', (collection_name,))
        
        columns = [column[0] for column in self.cursor.description]
        results = []
        
        for row in self.cursor.fetchall():
            result = dict(zip(columns, row))
            results.append(result)
        
        return results
    
    def delete_record(self, collection_name: str, file_path: str) -> None:
        """
        Delete a record from the database.
        """
        self.cursor.execute('''
            DELETE FROM ingestion_log
            WHERE collection = ? AND file_path = ?
        ''', (collection_name, file_path))
        self.conn.commit()
