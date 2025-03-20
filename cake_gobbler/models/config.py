# SPDX-FileCopyrightText: 2025 Cake AI Technologies, Inc.
#
# SPDX-License-Identifier: Apache-2.0

"""
Configuration Models for the Cake Gobbler RAG system.

This module defines configuration models and default values.
"""

import os
from dataclasses import dataclass
from typing import Optional

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()


@dataclass
class WeaviateConfig:
    """Configuration for Weaviate connection."""
    http_host: str = os.getenv("WEAVIATE_HTTP_HOST", "weaviate.weaviate")
    http_port: int = int(os.getenv("WEAVIATE_HTTP_PORT") or "80")
    grpc_host: str = os.getenv("WEAVIATE_GRPC_HOST", "weaviate-grpc.weaviate")
    grpc_port: int = int(os.getenv("WEAVIATE_GRPC_PORT") or "50051")
    timeout: int = int(os.getenv("WEAVIATE_TIMEOUT") or "10")  # Reduced default timeout for faster connection error detection


@dataclass
class ProcessingConfig:
    """Configuration for PDF processing."""
    chunk_size: int = int(os.getenv("CHUNK_SIZE", "1024"))
    chunk_overlap: int = int(os.getenv("CHUNK_OVERLAP", "20"))
    embedding_model: str = os.getenv("EMBEDDING_MODEL", "BAAI/bge-large-en-v1.5")
    db_path: str = os.getenv("DB_PATH", "cake-gobbler-log.db")


@dataclass
class AppConfig:
    """Main application configuration."""
    weaviate: WeaviateConfig
    processing: ProcessingConfig
    verbose: bool = False
    collection: Optional[str] = None
    run_id: Optional[str] = None
    run_name: Optional[str] = None
    
    def __post_init__(self):
        if not hasattr(self, 'weaviate') or self.weaviate is None:
            self.weaviate = WeaviateConfig()
        if not hasattr(self, 'processing') or self.processing is None:
            self.processing = ProcessingConfig()
