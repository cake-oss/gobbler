# SPDX-FileCopyrightText: 2025 Cake AI Technologies, Inc.
#
# SPDX-License-Identifier: Apache-2.0

"""
Weaviate Manager for the Cake Gobbler RAG system.

This module handles all operations related to Weaviate, including connection,
collection creation, and storage of embeddings.
"""

import json
import logging
import uuid
from typing import Dict, List, Any, Optional
import datetime
 
import weaviate
from weaviate.collections.classes.config import DataType
from weaviate.config import AdditionalConfig, Timeout
from weaviate.collections.classes.filters import Filter

from cake_gobbler.models.config import WeaviateConfig


class WeaviateManager:
    """
    Manages Weaviate operations for the Cake Gobbler RAG system.
    """
    
    def __init__(self, config: WeaviateConfig):
        """
        Initialize the Weaviate manager.
        
        Args:
            config: Weaviate connection configuration
        """
        self.config = config
        self.client = None
        self.logger = logging.getLogger("cake-gobbler.weaviate_manager")
    
    def connect(self) -> None:
        """
        Connect to the Weaviate instance.
        
        Raises:
            ConnectionError: If unable to connect to Weaviate
        """
        self.logger.info(f"Connecting to Weaviate at {self.config.http_host}:{self.config.http_port}")
        try:
            # Debug configuration values before connection
            self.logger.info(f"Connection timeout: {self.config.timeout} seconds")
            
            try:
                self.client = weaviate.connect_to_custom(
                    http_host=self.config.http_host,
                    http_port=self.config.http_port,
                    http_secure=False,
                    grpc_host=self.config.grpc_host,
                    grpc_port=self.config.grpc_port,
                    grpc_secure=False,
                    headers={"Authorization": "NONE"},
                    skip_init_checks=False,  # Changed to enforce init checks
                    additional_config=AdditionalConfig(timeout=Timeout(init=self.config.timeout))
                )
            except Exception as conn_e:
                # Provide a user-friendly message for connection errors
                if "connection refused" in str(conn_e).lower():
                    raise ConnectionError(f"Connection refused. Is Weaviate running at {self.config.http_host}:{self.config.http_port}?")
                elif "name resolution" in str(conn_e).lower() or "name or service not known" in str(conn_e).lower():
                    raise ConnectionError(f"Could not resolve hostname: {self.config.http_host}. Please check the hostname.")
                elif "timeout" in str(conn_e).lower():
                    raise ConnectionError(f"Connection timed out after {self.config.timeout} seconds.")
                else:
                    # Forward original error if not specifically handled
                    raise ConnectionError(f"Failed to connect: {str(conn_e)}")
            
            # Test connection by listing collections
            try:
                collections = self.client.collections.list_all()
                self.logger.info(f"Connection successful! Found {len(collections)} collections.")
            except Exception as api_e:
                # Handle API-level errors (when connection succeeded but API call failed)
                raise ConnectionError(f"Connected to server but API call failed: {str(api_e)}")
                
        except ConnectionError:
            # Re-raise connection errors without wrapping
            raise
        except Exception as e:
            # Wrap other unexpected errors
            error_msg = f"Unexpected error connecting to Weaviate: {str(e)}"
            self.logger.error(error_msg)
            raise ConnectionError(error_msg)
    
    def close(self) -> None:
        """Close the Weaviate connection."""
        if self.client:
            try:
                self.client.close()
                self.logger.info("Weaviate connection closed")
            except Exception as e:
                self.logger.error(f"Error closing Weaviate connection: {str(e)}")
            finally:
                self.client = None
    
    
    def create_or_get_collection(self, collection_name: str) -> Any:
        """
        Create a new collection or get an existing one.
        
        Args:
            collection_name: Name of the collection (must follow Weaviate PascalCase convention)
            
        Returns:
            The Weaviate collection object
            
        Raises:
            ValueError: If collection exists
        """
        self.logger.info(f"Creating or getting collection '{collection_name}'")
        
        if not self.client:
            self.connect()
        
        try:
            # Check if collection exists
            try:
                collection = self.client.collections.get(collection_name)
                self.debug.info(f"Collection '{collection_name}' = {collection}")
                return collection
            except Exception:
                self.logger.info(f"Collection '{collection_name}' does not exist in Weaviate")
            
            # Create new collection
            self.logger.info(f"Creating new collection '{collection_name}'")
            collection = self.client.collections.create(
                name=collection_name,
                properties=[
                    {
                        "name": "text",
                        "data_type": DataType.TEXT,
                        "description": "The text content of the chunk"
                    },
                    {
                        "name": "full_path",
                        "data_type": DataType.TEXT,
                        "description": "The full path to the source file"
                    },
                    {
                        "name": "chunk_index",
                        "data_type": DataType.NUMBER,
                        "description": "The index of the chunk in the source file"
                    },
                    {
                        "name": "total_chunks",
                        "data_type": DataType.NUMBER,
                        "description": "The total number of chunks in the source file"
                    },
                    {
                        "name": "ts",
                        "data_type": DataType.DATETIME,
                        "description": "The timestamp of the ingestion"
                    }
                    
                ],
                vectorizer_config=None
            )
            self.logger.info(f"Created collection '{collection_name}'")
            return collection
        except Exception as e:
            error_msg = f"Error creating/getting collection: {str(e)}"
            self.logger.error(error_msg)
            raise
    
    def store_chunks(self, collection, chunks: List[str], embeddings: List[List[float]], metadata: Dict[str, Any]) -> None:
        """
        Store chunks and embeddings in Weaviate.
        
        Args:
            collection: The Weaviate collection object
            chunks: List of text chunks
            embeddings: List of embeddings
            metadata: Metadata to store with each chunk
            
        Raises:
            ValueError: If the number of chunks and embeddings do not match
        """
        if len(chunks) != len(embeddings):
            error_msg = f"Number of chunks ({len(chunks)}) does not match number of embeddings ({len(embeddings)})"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        self.logger.info(f"Storing {len(chunks)} chunks in Weaviate")
        try:
            with collection.batch.dynamic() as batch:
                for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
                    chunk_metadata = metadata.copy()
                    chunk_metadata["chunk_index"] = i
                    ingestion_timestamp = datetime.datetime.now(datetime.timezone.utc)
                    
                    properties = {
                        "text": chunk,
                        "full_path": chunk_metadata.get("full_path", ""),
                        "chunk_index": chunk_metadata.get("chunk_index", 0),
                        "total_chunks": chunk_metadata.get("total_chunks", 0),
                        "ts": ingestion_timestamp
                    }

                    # Add all metadata key-value pairs to properties
                    for key, value in metadata.items():
                        properties[key] = value
                    
                    batch.add_object(
                        properties=properties,
                        vector=embedding,
                        uuid=str(uuid.uuid4())
                    )
            self.logger.info(f"Stored {len(chunks)} chunks successfully")
        except Exception as e:
            error_msg = f"Error storing chunks in Weaviate: {str(e)}"
            self.logger.error(error_msg)
            raise
    
    def search(self, collection_name: str, query_embedding: List[float], limit: int = 5) -> List[Dict[str, Any]]:
        """
        Search for similar chunks in Weaviate.
        
        Args:
            collection_name: Name of the collection to search (must follow Weaviate PascalCase convention)
            query_embedding: Embedding of the query
            limit: Maximum number of results to return
            
        Returns:
            List of search results
        """
        if not self.client:
            self.connect()
            
        collection = self.client.collections.get(collection_name)
        
        self.logger.info(f"Searching collection '{collection_name}' for similar chunks")
        results = collection.query.near_vector(
            near_vector=query_embedding,
            limit=limit,
            include_vector=False,
            return_properties=["text", "metadata_str"]
        )
        
        # Format results
        formatted_results = []
        for i, obj in enumerate(results.objects):
            props = obj.properties if hasattr(obj, "properties") else {}
            text = props.get("text", "No text available")
            metadata_str = props.get("metadata_str", "{}")
            
            try:
                metadata = json.loads(metadata_str)
            except json.JSONDecodeError:
                metadata = {}
                
            formatted_results.append({
                "text": text,
                "metadata": metadata,
                "score": obj.metadata.certainty if hasattr(obj, "metadata") and hasattr(obj.metadata, "certainty") else None
            })
            
        self.logger.info(f"Found {len(formatted_results)} results")
        return formatted_results
        
    def delete(self, collection_name: str, full_path: str) -> Dict[str, Any]:
        """
        Delete all chunks from a collection where full_path matches the provided path.
        
        Args:
            collection_name: Name of the collection (must follow Weaviate PascalCase convention)
            full_path: The full path value to match
            
        Returns:
            Dictionary with the deletion response
        """
        if not self.client:
            self.connect()

        try:
            collection = self.client.collections.get(collection_name)
            if not collection.exists():
                self.logger.info(f"Collection '{collection_name}' not found")
                return {}
            
            response = collection.data.delete_many(
                where=Filter.by_property("full_path").equal(full_path)
            )
            
            self.logger.info(f"Deleted objects from collection '{collection_name}', response: {response}")
            return response
        except Exception as e:
            error_msg = f"Error deleting objects from collection '{collection_name}': {str(e)}"
            self.logger.error(error_msg)
            raise
