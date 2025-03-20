# SPDX-FileCopyrightText: 2025 Cake AI Technologies, Inc.
#
# SPDX-License-Identifier: Apache-2.0

"""
Text Processor for the Cake Gobbler RAG system.

This module handles text processing operations, including chunking and embedding.
"""

import logging
import nltk
from typing import List, Optional

from langchain_text_splitters import RecursiveCharacterTextSplitter, TokenTextSplitter
from sentence_transformers import SentenceTransformer


class TextProcessor:
    """
    Process text for chunking and embedding.
    """
    
    def __init__(self, chunk_size: int, chunk_overlap: int, verbose: bool = False):
        """
        Initialize the text processor.
        
        Args:
            chunk_size: Number of tokens per chunk
            chunk_overlap: Number of tokens to overlap between chunks
            verbose: Enable verbose logging
        """
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.verbose = verbose
        self.logger = logging.getLogger("cake-gobbler.text_processor")
        
        # Ensure NLTK resources are downloaded
        self._download_nltk_resources()
    
    def _download_nltk_resources(self):
        """Download required NLTK resources if not already present."""
        try:
            nltk.data.find('tokenizers/punkt')
            self.logger.info("NLTK punkt tokenizer already downloaded")
        except LookupError:
            self.logger.info("Downloading NLTK punkt tokenizer...")
            nltk.download('punkt')
    
    def split_text_into_chunks(self, text: str) -> List[str]:
        """
        Split text into chunks for processing.
        
        Args:
            text: Text to split
            
        Returns:
            List of text chunks
        """
        self.logger.info(f"Splitting text into chunks (chunk_size={self.chunk_size}, overlap={self.chunk_overlap})")

        text_splitter = TokenTextSplitter(
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap
        )
        
        try:
            chunks = text_splitter.split_text(text)
            
            # Log each chunk with formatting and character count
            #for i, chunk in enumerate(chunks, 1):
            #    self.logger.info(f"\n{'='*80}\nChunk {i}/{len(chunks)} (chars: {len(chunk)})\n{'-'*80}\n{chunk}\n{'='*80}")
                
        except Exception as e:
            self.logger.error(f"Error splitting text: {str(e)}, text=\n\n\n======\n{text}\n=====\n\n")
            raise
        
        self.logger.info(f"Created {len(chunks)} chunks")

        return chunks
    
    def load_embedding_model(self, model_name: str, device: Optional[str] = "cuda") -> SentenceTransformer:
        """
        Load the sentence transformer model for embeddings.
        
        Args:
            model_name: Name of the model to load
            device: Device to use (cuda or cpu)
            
        Returns:
            Loaded model
        """
        self.logger.info(f"Loading embedding model: {model_name}")
        try:
            model = SentenceTransformer(model_name, device=device)
            self.logger.info("Embedding model loaded successfully")
            return model
        except Exception as e:
            self.logger.error(f"Error loading embedding model: {str(e)}")
            raise
    
    def embed_chunks(self, chunks: List[str], model: SentenceTransformer) -> List[List[float]]:
        """
        Embed chunks using the provided model.
        
        Args:
            chunks: List of text chunks to embed
            model: SentenceTransformer model to use
            
        Returns:
            List of embeddings (as lists of floats)
        """
        self.logger.info(f"Embedding {len(chunks)} chunks")
        try:
            embeddings = model.encode(chunks, show_progress_bar=True)
            
            # Convert to list format
            embeddings_list = [embedding.tolist() for embedding in embeddings]
            
            self.logger.info(f"Embedded chunks successfully; embedding dimension: {len(embeddings_list[0]) if embeddings_list else 0}")
            return embeddings_list
        except Exception as e:
            self.logger.error(f"Error embedding chunks: {str(e)}")
            raise
