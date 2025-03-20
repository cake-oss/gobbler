# SPDX-FileCopyrightText: 2025 Cake AI Technologies, Inc.
#
# SPDX-License-Identifier: Apache-2.0

"""
Manages embedding model operations for the Cake Gobbler RAG system.

This module handles loading, unloading, and using embedding models
for generating text embeddings. It optimizes memory usage by managing
model lifecycle and providing a centralized interface for embedding operations.
"""

import logging
import platform
from typing import List
from cake_gobbler.utils.logging import configure_logging

import ray

@ray.remote(num_gpus=1, runtime_env={"pip": ["sentence-transformers", "typing"]})
class EmbeddingModelManager:
    """
    Manages embedding model operations.
    
    This class handles the lifecycle of embedding models, including loading,
    unloading, and using them to generate embeddings for text chunks.
    """
    
    def __init__(self):
        """Initialize the embedding model manager."""
        self.embedding_model = None
        self.embedding_model_name = None
        self.logger = configure_logging()
    
    def load_embedding_model(self, model_name: str) -> None:
        """
        Load the embedding model if not already loaded or if a different model is requested.
        
        Args:
            model_name: Name of the embedding model to load
        """
        import torch
        from sentence_transformers import SentenceTransformer
        if self.embedding_model is None or self.embedding_model_name != model_name:
            # Clear GPU memory if there's an existing model
            if self.embedding_model is not None:
                self.unload_embedding_model()
            
            # Load the new model
            try:
                self.logger.info(f"Loading embedding model: {model_name}")
                device = "cuda" if torch.cuda.is_available() else "cpu"
                # check if mac and use metal
                if platform.system() == "Darwin":
                    device = "mps" if torch.backends.mps.is_available() else "cpu"

                # if /home/ray/shared exists, use it as the cache folder
                import os
                if os.path.exists("/home/ray/shared"):
                    os.makedirs("/home/ray/shared/models", exist_ok=True)
                    self.embedding_model = SentenceTransformer(model_name, device=device, cache_folder="/home/ray/shared/models")
                else:
                    self.embedding_model = SentenceTransformer(model_name, device=device)
                self.embedding_model_name = model_name
                self.logger.info(f"Successfully loaded embedding model: {model_name}")
            except Exception as e:
                error_msg = f"Error loading embedding model: {str(e)}"
                self.logger.error(error_msg)
                print(error_msg)
                raise
    
    def unload_embedding_model(self) -> None:
        """
        Unload the embedding model to free memory.
        """
        import torch
        if self.embedding_model is not None:
            model_name = self.embedding_model_name
            # Remove references to the model
            del self.embedding_model
            self.embedding_model = None
            
            # Clear CUDA cache if available
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
            
            self.logger.info(f"Unloaded embedding model: {model_name}")
            self.embedding_model_name = None
    
    def embed_chunks(self, chunks: List[str]) -> List[List[float]]:
        """
        Embed a list of text chunks using the loaded model.
        
        Args:
            chunks: List of text chunks to embed
            
        Returns:
            List of embeddings (as lists of floats)
        """
        if self.embedding_model is None:
            raise ValueError("Embedding model not loaded. Call load_embedding_model() first.")
        
        try:
            # Embed the chunks
            embeddings = self.embedding_model.encode(chunks, show_progress_bar=True)
            
            # Convert to list format
            embeddings_list = [embedding.tolist() for embedding in embeddings]
            return embeddings_list
        except Exception as e:
            print(f"Error embedding chunks: {str(e)}")
            raise
