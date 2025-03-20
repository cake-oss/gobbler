# SPDX-FileCopyrightText: 2025 Cake AI Technologies, Inc.
#
# SPDX-License-Identifier: Apache-2.0

"""
Run Manager for the Cake Gobbler RAG system.

This module handles run management for the PDF ingestion system,
including tracking run statistics, managing run state, and
storing run metadata in the database. It also manages shared resources
like embedding models to optimize memory usage and performance.
"""

import uuid
import datetime
from typing import Dict, Any, Optional, List, Union
from cake_gobbler.utils.file_utils import calculate_file_fingerprint
from cake_gobbler.models.db_models import RunStatistics, RunRecord
from cake_gobbler.core.embedding_model_manager import EmbeddingModelManager


class RunManager:
    """
    Manages run operations for the PDF ingestion system.
    
    This class tracks ingestion runs, maintains statistics, and manages
    shared resources like embedding models to optimize memory usage.
    """
    
    def __init__(self, db_manager, embedding_model_managers: List[EmbeddingModelManager]):
        """
        Initialize the run manager.
        
        Args:
            db_manager: DatabaseManager instance for database operations
            embedding_model_managers: List of EmbeddingModelManager instances for embedding operations
        """
        self.db_manager = db_manager
        self.embedding_model_managers = embedding_model_managers
        self.run_id = None
        self.start_time = None
        self.end_time = None
        self.processed_files = 0
        self.failed_files = 0
        self.skipped_files = 0
        self.already_processed_files = 0  # Files skipped because they were already processed
        self.total_files = 0
        self.status = "not_started"  # Possible values: not_started, running, completed, failed
        
        # Run configuration
        self.run_name = None
        self.collection = None
        self.embedding_model_name = None
        self.chunk_size = None
        self.chunk_overlap = None
    
    def start_run(self, total_files: int = 0, run_id: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Start a new run and record it in the database.
        
        Args:
            total_files: Total number of files to process
            run_id: Optional run ID (will be generated if not provided)
            metadata: Optional metadata for the run
            
        Returns:
            str: The run ID
        """
        # Generate a unique run ID if not provided
        self.run_id = run_id if run_id else str(uuid.uuid4())
        
        # Record start time
        self.start_time = datetime.datetime.now()
        
        # Initialize counters
        self.total_files = total_files
        self.processed_files = 0
        self.failed_files = 0
        self.skipped_files = 0
        self.already_processed_files = 0
        
        # Set status to running
        self.status = "running"
        
        # Create RunRecord with all the fields
        run_record = None
        if metadata:
            # Extract run name
            self.run_name = metadata.get("run_name")
            
            # Extract command line args
            cmd_args = metadata.get("command_line_args", {})
            if cmd_args:
                self.collection = cmd_args.get("collection")
                self.embedding_model_name = cmd_args.get("embedding_model")
                self.chunk_size = cmd_args.get("chunk_size")
                self.chunk_overlap = cmd_args.get("chunk_overlap")
        
        run_record = RunRecord(
            run_id=self.run_id,
            start_time=self.start_time.isoformat(),
            status=self.status,
            total_files=self.total_files,
            processed_files=self.processed_files,
            failed_files=self.failed_files,
            skipped_files=self.skipped_files,
            already_processed_files=self.already_processed_files
        )
        
        # Set run record fields
        run_record.run_name = self.run_name
        run_record.collection = self.collection
        run_record.embedding_model = self.embedding_model_name
        run_record.chunk_size = self.chunk_size
        run_record.chunk_overlap = self.chunk_overlap
        
        # Store run in database with run record
        self.db_manager.create_run(
            self.run_id,
            self.start_time.isoformat(),
            total_files=self.total_files,
            run_record=run_record
        )
        
        return self.run_id
    
    def increment_processed(self) -> int:
        """
        Increment the processed files counter and update the database.
        
        Returns:
            int: The new processed files count
        """
        if not self.run_id:
            raise ValueError("No run in progress. Call start_run() first.")
        
        self.processed_files += 1
        
        # Update run data in database
        self.db_manager.update_run(
            self.run_id,
            processed_files=self.processed_files
        )
        
        return self.processed_files
    
    def increment_failed(self) -> int:
        """
        Increment the failed files counter and update the database.
        
        Returns:
            int: The new failed files count
        """
        if not self.run_id:
            raise ValueError("No run in progress. Call start_run() first.")
        
        self.failed_files += 1
        
        # Update run data in database
        self.db_manager.update_run(
            self.run_id,
            failed_files=self.failed_files
        )
        
        return self.failed_files
    
    def increment_skipped(self) -> int:
        """
        Increment the skipped files counter and update the database.
        
        Returns:
            int: The new skipped files count
        """
        if not self.run_id:
            raise ValueError("No run in progress. Call start_run() first.")
        
        self.skipped_files += 1
        
        # Update run data in database
        self.db_manager.update_run(
            self.run_id,
            skipped_files=self.skipped_files
        )
        
        return self.skipped_files
        
    def increment_already_processed(self) -> int:
        """
        Increment the already processed files counter and update the database.
        
        Returns:
            int: The new already processed files count
        """
        if not self.run_id:
            raise ValueError("No run in progress. Call start_run() first.")
        
        self.already_processed_files += 1
        
        # Update already_processed_files and skipped_files
        self.skipped_files += 1
        self.db_manager.update_run(
            self.run_id,
            already_processed_files=self.already_processed_files,
            skipped_files=self.skipped_files
        )
        
        return self.already_processed_files
    
    def get_run_stats(self) -> RunStatistics:
        """
        Get current run statistics.
        
        Returns:
            RunStatistics: Run statistics object
        """
        if not self.run_id:
            raise ValueError("No run in progress. Call start_run() first.")
        
        # Calculate processing time so far
        if self.end_time:
            processing_time = (self.end_time - self.start_time).total_seconds()
        else:
            processing_time = (datetime.datetime.now() - self.start_time).total_seconds()
        
        # Calculate completion percentage - don't double count already_processed files
        # since they're already included in skipped_files (see increment_already_processed)
        if self.total_files > 0:
            completion_percentage = (self.processed_files + self.failed_files + self.skipped_files) / self.total_files * 100
        else:
            completion_percentage = 0
        
        # Get current run record from database to ensure we have the latest values
        run_record = self.db_manager.get_run_record(self.run_id)
        
        # Return statistics
        return RunStatistics(
            run_id=self.run_id,
            start_time=self.start_time.isoformat(),
            end_time=self.end_time.isoformat() if self.end_time else None,
            status=self.status,
            total_files=self.total_files,
            processed_files=self.processed_files,
            failed_files=self.failed_files,
            skipped_files=self.skipped_files,
            already_processed_files=self.already_processed_files,
            completion_percentage=completion_percentage,
            processing_time_seconds=processing_time,
            
            # Specific fields from run record
            run_name=run_record.run_name,
            collection=run_record.collection,
            embedding_model=run_record.embedding_model,
            chunk_size=run_record.chunk_size,
            chunk_overlap=run_record.chunk_overlap
        )
    
    def end_run(self) -> RunStatistics:
        """
        End the current run, unload resources, and update the database.
        
        Returns:
            RunStatistics: Run statistics object
        """
        if not self.run_id:
            raise ValueError("No run in progress. Call start_run() first.")
        
        try:
            # Unload resources to free memory
            for embedding_model_manager in self.embedding_model_managers:
                try:
                    embedding_model_manager.unload_embedding_model.remote()
                except Exception as e:
                    # Log error but continue with cleanup
                    print(f"Error unloading embedding model: {str(e)}")
            
            # Record end time
            self.end_time = datetime.datetime.now()
            
            # Calculate total processing time in seconds
            total_processing_time = (self.end_time - self.start_time).total_seconds()
            
            # Determine final status
            if self.failed_files > 0:
                self.status = "completed_with_errors"
            else:
                self.status = "completed"
            
            # Create a run record with all fields to update
            run_record = RunRecord(
                run_id=self.run_id,
                start_time="",  # Not updating this field
                end_time=self.end_time.isoformat(),
                status=self.status,
                processed_files=self.processed_files,
                failed_files=self.failed_files,
                skipped_files=self.skipped_files,
                already_processed_files=self.already_processed_files,
                total_processing_time=total_processing_time
            )
            
            # Update the database
            self.db_manager.update_run(
                self.run_id,
                end_time=self.end_time.isoformat(),
                status=self.status,
                processed_files=self.processed_files,
                failed_files=self.failed_files,
                skipped_files=self.skipped_files,
                already_processed_files=self.already_processed_files,
                total_processing_time=total_processing_time,
                run_record=run_record
            )
            
            # Return run statistics
            return self.get_run_stats()
        except Exception as e:
            # Mark as failed if any error occurs during end_run
            self.status = "failed"
            try:
                self.db_manager.update_run(
                    self.run_id,
                    end_time=datetime.datetime.now().isoformat(),
                    status=self.status
                )
            except Exception:
                # Ignore errors in error handling
                pass
            raise
