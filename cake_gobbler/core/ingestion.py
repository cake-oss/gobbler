# SPDX-FileCopyrightText: 2025 Cake AI Technologies, Inc.
#
# SPDX-License-Identifier: Apache-2.0

"""
Ingestion Module for the Cake Gobbler RAG system.

This module handles the end-to-end process of ingesting PDFs, including
analyzing, extracting text, chunking, embedding, and storing in Weaviate.
"""

import json
import logging
import os
from typing import Dict, List, Any, Optional, Tuple

from cake_gobbler.models.config import AppConfig, WeaviateConfig, ProcessingConfig
from cake_gobbler.core.pdf_processor import PDFProcessor

from cake_gobbler.core.db_manager import DatabaseManager
from cake_gobbler.core.run_manager import RunManager
from cake_gobbler.core.weaviate_manager import WeaviateManager
from cake_gobbler.core.embedding_model_manager import EmbeddingModelManager
from cake_gobbler.utils.file_utils import calculate_file_fingerprint
import ray

class IngestionManager:
    """
    Manages the PDF ingestion process end-to-end.
    """
    
    def __init__(self, app_config: AppConfig):
        """
        Initialize the ingestion manager.
        
        Args:
            app_config: Application configuration
            
        Raises:
            ConnectionError: If unable to connect to Weaviate
        """
        

        self.config = app_config
        self.logger = logging.getLogger("cake-gobbler.ingestion")
        self.logger.info("Initializing ingestion manager...")
        from cake_gobbler.core.text_processor import TextProcessor
        self.logger.info("TextProcessor imported")
        
        # Initialize components
        self.db_manager = DatabaseManager(app_config.processing.db_path)
        self.pdf_processor = PDFProcessor(verbose=app_config.verbose)
        self.text_processor = TextProcessor(
            chunk_size=app_config.processing.chunk_size,
            chunk_overlap=app_config.processing.chunk_overlap,
            verbose=app_config.verbose
        )
        self.weaviate_manager = WeaviateManager(app_config.weaviate)

        self._embedding_model_managers = None
        self.run_manager = None  # Will be initialized when embedding managers are created

    def get_run_manager(self) -> RunManager:
        """
        Lazy load run manager.
        """
        if self.run_manager is None:
            self.run_manager = RunManager(self.db_manager, self.get_embedding_model_managers())
        return self.run_manager


    def get_embedding_model_managers(self) -> List[EmbeddingModelManager]:
        """
        Lazy load embedding model managers.
        
        Returns:
            List of EmbeddingModelManager instances
        """

        if self._embedding_model_managers is None:
            self.logger.info("Initializing embedding model managers. This can take a while...")
            num_workers = self.config.processing.ray_workers
            self.logger.info(f"Creating {num_workers} Ray workers for embedding model parallelism")
            self._embedding_model_managers = [EmbeddingModelManager.remote() for _ in range(num_workers)]
            self.logger.info("Embedding model managers initialized.")
        return self._embedding_model_managers
    
    def start_run(self, total_files: int = 0) -> str:
        """
        Start a new ingestion run.
        
        Args:
            total_files: Total number of files to process
            
        Returns:
            Run ID
        """
        # Prepare run metadata
        run_metadata = {
            "command_line_args": {
                "collection": self.config.collection,
                "chunk_size": self.config.processing.chunk_size,
                "chunk_overlap": self.config.processing.chunk_overlap,
                "embedding_model": self.config.processing.embedding_model,
                "verbose": self.config.verbose
            },
            "system_info": {
                "platform": os.name,
                "python_version": os.sys.version
            },
            "weaviate_connection": {
                "http_host": self.config.weaviate.http_host,
                "http_port": self.config.weaviate.http_port,
                "grpc_host": self.config.weaviate.grpc_host,
                "grpc_port": self.config.weaviate.grpc_port
            }
        }
        
        # Add run name to metadata if provided
        if self.config.run_name:
            run_metadata["run_name"] = self.config.run_name
            
        # Start run
        run_manager = self.get_run_manager()
        run_id = run_manager.start_run(
            total_files=total_files,
            run_id=self.config.run_id,
            metadata=run_metadata
        )
        
        # Pre-load the embedding model at the start of the run
        refs = []
        for embedding_model_manager in self.get_embedding_model_managers():
            refs.append(embedding_model_manager.load_embedding_model.remote(self.config.processing.embedding_model))
        ray.get(refs)
        
        return run_id
    
    def end_run(self):
        """End the current ingestion run."""
        if self.run_manager is not None:
            return self.run_manager.end_run()
    
    def ingest_file(self, pdf_path: str) -> str:
        """
        Ingest a PDF file: analyze, process if acceptable, and log results.
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            str: Status of ingestion (success, error, skipped, already_processed)
        """
        self.logger.info(f"Ingesting file: {pdf_path}")
        
        # Get run_id
        run_id = self.run_manager.run_id
        
        # Calculate file fingerprint
        file_fingerprint = calculate_file_fingerprint(pdf_path)
        
        # Check if file needs processing
        if file_fingerprint:
            needs_processing, previous_record = self.db_manager.file_needs_processing(
                file_fingerprint,
                self.config.collection
            )
             
            if not needs_processing:
                self.logger.info(f"Skipping {pdf_path} - already processed successfully for collection '{self.config.collection}' (fingerprint: {file_fingerprint})")
                self.run_manager.increment_already_processed()
                return "already_processed"
            
        # Delete any previous ingestion records for this file in weaviate
        # This is to ensure that we are not using a previously failed ingestion as a basis for the new ingestion
        # and that we are starting fresh from the current run for this file.
        self.weaviate_manager.delete(self.config.collection, full_path=pdf_path)
        
        # Run analysis on the PDF file
        try:
            analysis_result = self.pdf_processor.analyze_pdf(pdf_path)
        except Exception as e:
            error_msg = f"Analysis error: {str(e)}"
            self.logger.error(error_msg)
            self.db_manager.log_ingestion(
                pdf_path, self.config.collection, "error", error_message=error_msg, run_id=run_id,
                file_fingerprint=file_fingerprint
            )
            self.run_manager.increment_failed()
            return "error"

        # Check if PDF is acceptable for processing
        is_acceptable, reason, detailed_diagnostics = self.pdf_processor.is_pdf_acceptable(analysis_result)
        
        # Add detailed diagnostics to analysis_result metadata
        if "metadata" not in analysis_result.metadata:
            analysis_result.metadata["diagnostics"] = {}
        analysis_result.metadata["diagnostics"]["acceptance_check"] = detailed_diagnostics
        
        if not is_acceptable:
            self.logger.error(f"PDF not acceptable: {reason}")
            # Include detailed diagnostics in the error message
            detailed_reason = f"{reason}. Details: {json.dumps(detailed_diagnostics)}"
            self.db_manager.log_ingestion(
                pdf_path, self.config.collection, "skipped", error_message=detailed_reason, run_id=run_id,
                file_fingerprint=file_fingerprint
            )
            self.run_manager.increment_skipped()
            return "skipped"

        # If analysis passes, try processing the PDF
        try:
            # Extract text from the PDF
            text, extraction_diagnostics = self.pdf_processor.extract_text_from_pdf(pdf_path)
            
            # Add extraction diagnostics to analysis_result metadata
            analysis_result.metadata["diagnostics"]["text_extraction"] = extraction_diagnostics
            
            # Check if extraction failed
            if not text:
                # Prepare detailed error message
                if extraction_diagnostics and extraction_diagnostics.get("failure_reason"):
                    failure_reason = extraction_diagnostics.get("failure_reason")
                    failure_details = extraction_diagnostics.get("failure_details", "No details provided")
                    
                    # Map failure reasons to user-friendly messages
                    reason_messages = {
                        "empty_document": "PDF has no pages",
                        "extraction_command_failed": "Text extraction command failed",
                        "empty_extraction_output": "PDF produced empty output during extraction",
                        "decoding_error": "Character encoding issues prevented text extraction",
                        "decoding_failed": "Failed to decode text with any supported encoding",
                        "empty_text": "PDF contains no extractable text",
                        "command_execution_error": "Error executing text extraction command",
                        "general_extraction_error": "General error during text extraction"
                    }
                    
                    user_message = reason_messages.get(failure_reason, "Unknown extraction issue")
                    error_msg = f"Text extraction failed: {user_message}. {failure_details}"
                else:
                    error_msg = f"Extracted text is too short: only {len(text)} characters. This PDF may not contain extractable text."
                
                self.logger.error(error_msg)
                
                # Add more specific guidance based on diagnostics
                if extraction_diagnostics:
                    if extraction_diagnostics.get("failure_reason") == "empty_extraction_output":
                        self.logger.error("PDF likely contains only scanned images without OCR text layer.")
                    elif extraction_diagnostics.get("page_count", 0) > 0 and not text:
                        self.logger.error("PDF has pages but no extractable text. It may contain only images or have content protection.")
                
                # Include detailed diagnostics in the database log
                detailed_error = {
                    "message": error_msg,
                    "extraction_diagnostics": extraction_diagnostics,
                    "analysis_diagnostics": detailed_diagnostics
                }
                
                self.db_manager.log_ingestion(
                    pdf_path, self.config.collection, "skipped", error_message=json.dumps(detailed_error), run_id=run_id,
                    file_fingerprint=file_fingerprint
                )
                self.run_manager.increment_skipped()
                return "skipped"
            
            # Split the text into chunks
            chunks = self.text_processor.split_text_into_chunks(text)
            
            # Check if any chunks were created
            if not chunks:
                error_msg = "No chunks were created from the extracted text. Cannot proceed with embedding."
                self.logger.error(error_msg)
                self.db_manager.log_ingestion(
                    pdf_path, self.config.collection, "skipped", error_message=error_msg, run_id=run_id,
                    file_fingerprint=file_fingerprint
                )
                self.run_manager.increment_skipped()
                return "skipped"
            
            # Prepare metadata for storage
            metadata = {
                "full_path": os.path.abspath(pdf_path),
                "chunk_size": self.config.processing.chunk_size,
                "total_chunks": len(chunks)
                # "source": os.path.basename(pdf_path),   # not needed since we can parse the full path
                # "embedding_model": self.config.processing.embedding_model,  # not needed currently
                # "chunk_overlap": self.config.processing.chunk_overlap,  # not needed currently
            }
            
            # Connect to Weaviate and get/create collection
            self.weaviate_manager.connect()
            collection = self.weaviate_manager.create_or_get_collection(
                self.config.collection
            )
            
            # Embed chunks using distributed embedding model managers
            embeddings = self.distribute_embeddings(chunks)
            
            # Store chunks and embeddings in Weaviate
            self.weaviate_manager.store_chunks(collection, chunks, embeddings, metadata)
            
            # Log successful ingestion
            self.db_manager.log_ingestion(
                pdf_path, self.config.collection, "success", analysis_result, run_id=run_id,
                file_fingerprint=file_fingerprint
            )
            
            # Update run statistics
            self.run_manager.increment_processed()
            
            self.logger.info(f"PDF ingestion completed successfully.")
            return "success"
        except Exception as e:
            error_msg = f"Ingestion error: {str(e)}"
            self.logger.error(error_msg)
            self.db_manager.log_ingestion(
                pdf_path, self.config.collection, "error", analysis_result, error_message=error_msg, run_id=run_id,
                file_fingerprint=file_fingerprint
            )
            self.run_manager.increment_failed()
            return "error"
    
    def distribute_embeddings(self, chunks: List[str]) -> List[List[float]]:
        """
        Distribute the embedding of chunks across all available embedding model managers.
        
        Args:
            chunks: List of text chunks to embed
            
        Returns:
            List of embeddings (as lists of floats)
        """
        # If we only have one chunk or one manager, just use the first manager
        embedding_model_managers = self.get_embedding_model_managers()
        if len(chunks) <= 1 or len(embedding_model_managers) <= 1:
            return ray.get(embedding_model_managers[0].embed_chunks.remote(chunks))
        
        # Split chunks into smaller batches
        batch_size = max(1, len(chunks) // len(embedding_model_managers))
        batches = [chunks[i:i + batch_size] for i in range(0, len(chunks), batch_size)]
        
        # Dictionary mapping an actor to its current task (a future)
        actor_tasks = {}
        results = []
        
        # Submit the initial tasks to each actor
        for i, actor in enumerate(embedding_model_managers):
            if i < len(batches):
                actor_tasks[actor] = actor.embed_chunks.remote(batches[i])
        
        # Start processing remaining batches dynamically
        next_batch_idx = len(embedding_model_managers)  # index of the next batch to process
        
        while actor_tasks:
            # Wait for at least one actor to complete its task
            done_task_ids, _ = ray.wait(list(actor_tasks.values()), num_returns=1)
            
            # Identify which actor finished its task
            for actor, task in list(actor_tasks.items()):
                if task in done_task_ids:
                    # Retrieve the result from this task
                    batch_result = ray.get(task)
                    results.extend(batch_result)
                    
                    # If there are still batches to process, assign the next one
                    if next_batch_idx < len(batches):
                        actor_tasks[actor] = actor.embed_chunks.remote(batches[next_batch_idx])
                        next_batch_idx += 1
                    else:
                        # No more batches to process for this actor
                        del actor_tasks[actor]
                    break
        return results

    def close(self):
        """Close all connections and clean up resources."""
        try:
            # Close Weaviate connection
            if hasattr(self, 'weaviate_manager') and self.weaviate_manager:
                try:
                    self.weaviate_manager.close()
                except Exception as e:
                    self.logger.error(f"Error closing Weaviate manager: {str(e)}")
                self.weaviate_manager = None
            
            # Close database connection
            if hasattr(self, 'db_manager') and self.db_manager:
                try:
                    self.db_manager.close()
                except Exception as e:
                    self.logger.error(f"Error closing database manager: {str(e)}")
                self.db_manager = None
            
            # Clean up any CUDA resources
            try:
                import torch
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
            except Exception as e:
                self.logger.error(f"Error clearing CUDA cache: {str(e)}")
            
            # Clean up embedding model managers
            if self._embedding_model_managers:
                for manager in self._embedding_model_managers:
                    try:
                        manager.unload_embedding_model.remote()
                    except Exception as e:
                        self.logger.error(f"Error unloading embedding model: {str(e)}")
                self._embedding_model_managers = None
                
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")
