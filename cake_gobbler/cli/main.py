# SPDX-FileCopyrightText: 2025 Cake AI Technologies, Inc.
#
# SPDX-License-Identifier: Apache-2.0

"""
Main CLI module for the Cake Gobbler RAG system.

This module provides the main CLI interface for the application.
"""

from rich.traceback import install
install()

import sys
import os
from dotenv import load_dotenv

# Load environment variables first, before any other imports
load_dotenv()

import json
import re
from typing import Optional
import typer
from rich.console import Console
from rich.table import Table

from cake_gobbler.models.config import AppConfig, WeaviateConfig, ProcessingConfig
from cake_gobbler.models.config import ProcessingConfig as DefaultConfig
from cake_gobbler.core.ingestion import IngestionManager
from cake_gobbler.core.db_manager import DatabaseManager
from cake_gobbler.utils.logging import configure_logging
from cake_gobbler.utils.file_utils import find_pdf_files

# Create Typer app
app = typer.Typer(help="Cake Gobbler RAG - PDF Ingestion System", no_args_is_help=True)
console = Console()

logger = configure_logging()

@app.command("ingest")
def ingest_pdfs(
    pdf_path: str = typer.Option(None, "--pdf", help="Path to PDF file or directory"),
    collection: str = typer.Option(None, "--collection", help="Weaviate collection name"),
    chunk_size: int = typer.Option(1024, "--chunk-size", help="Number of tokens per chunk"),
    chunk_overlap: int = typer.Option(20, "--chunk-overlap", help="Number of tokens to overlap between chunks"),
    embedding_model: str = typer.Option("BAAI/bge-large-en-v1.5", "--embedding-model", help="Embedding model to use"),
    weaviate_host: str = typer.Option(os.environ.get("WEAVIATE_HTTP_HOST", "weaviate.weaviate"), "--weaviate-host", help="Weaviate HTTP host"),
    weaviate_port: int = typer.Option(os.environ.get("WEAVIATE_HTTP_PORT", 80), "--weaviate-port", help="Weaviate HTTP port"),
    weaviate_grpc_host: str = typer.Option(os.environ.get("WEAVIATE_GRPC_HOST", "weaviate-grpc.weaviate"), "--weaviate-grpc-host", help="Weaviate gRPC host"),
    weaviate_grpc_port: int = typer.Option(os.environ.get("WEAVIATE_GRPC_PORT", 50051), "--weaviate-grpc-port", help="Weaviate gRPC port"),
    weaviate_timeout: int = typer.Option(os.environ.get("WEAVIATE_TIMEOUT", 60), "--weaviate-timeout", help="Weaviate connection timeout in seconds"),
    db_path: str = typer.Option(DefaultConfig().db_path, "--db", help="SQLite database file for logging ingestions"),
    verbose: bool = typer.Option(False, "--verbose", help="Enable verbose logging"),
    run_id: Optional[str] = typer.Option(None, "--run-id", help="Specify a run ID (optional)"),
    run_name: Optional[str] = typer.Option(None, "--run-name", help="Specify a human-readable name for the run"),
    query: Optional[str] = typer.Option(None, "--query", help="Search query to run after processing"),
    ray_address: Optional[str] = typer.Option(None, "--ray-address", help="Ray cluster address (e.g., 'ray://localhost:10001'). If not provided, local Ray will be used."),
    ray_workers: int = typer.Option(1, "--ray-workers", help="Number of Ray workers for embedding model parallelism"),
):
    """Ingest PDFs into Weaviate with analysis and chunking."""
    # Set up logging with proper verbose flag
    if not pdf_path:
        console.print("[bold red]Error: --pdf argument is required for processing files.[/bold red]")
        console.print("Use --list-runs to see all runs or --run-stats <run_id> to see statistics for a specific run.")
        sys.exit(1)
        
    if not collection:
        console.print("[bold red]Error: --collection argument is required for processing files.[/bold red]")
        sys.exit(1)
    
    # Validate collection name format
    if not re.match(r'^[A-Z][a-zA-Z0-9_]*$', collection):
        console.print("[bold red]Error: Collection name must start with a capital letter and contain only letters, numbers, and underscores.[/bold red]")
        sys.exit(1)

    import ray

    # Initialize Ray based on the provided address
    if ray_address:
        console.print(f"Connecting to Ray cluster at: [bold]{ray_address}[/bold]")
        ray.init(address=ray_address)
    else:
        logger.info("Using local Ray cluster")
        ray.init()
        
    # Create configuration
    app_config = AppConfig(
        weaviate=WeaviateConfig(
            http_host=weaviate_host,
            http_port=weaviate_port,
            grpc_host=weaviate_grpc_host,
            grpc_port=weaviate_grpc_port,
            timeout=weaviate_timeout
        ),
        processing=ProcessingConfig(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            embedding_model=embedding_model,
            db_path=db_path,
            ray_workers=ray_workers
        ),
        verbose=verbose,
        collection=collection,
        run_id=run_id,
        run_name=run_name
    )
    
    # Find PDF files
    pdf_files = find_pdf_files(pdf_path)
    if not pdf_files:
        console.print(f"[bold red]Error: No PDF files found at {pdf_path}[/bold red]")
    
    logger.info(f"Found {len(pdf_files)} PDF file(s) to process.")
    
    # Initialize ingestion manager
    ingestion_manager = IngestionManager(app_config)
    
    try:
        # Display connection info
        logger.info(f"Weaviate connection: HTTP {weaviate_host}:{weaviate_port}, gRPC {weaviate_grpc_host}:{weaviate_grpc_port}")
        
        # Verify Weaviate connection before starting the run
        try:
            ingestion_manager.weaviate_manager.connect()
            console.print("[bold green]Successfully connected to Weaviate![/bold green]")
        except ConnectionError as e:
            console.print(f"[bold red]Error: Could not connect to Weaviate: {str(e)}[/bold red]")
            console.print(f"[bold yellow]Please verify that Weaviate is running at {weaviate_host}:{weaviate_port}[/bold yellow]")
            sys.exit(1)
            
        # Start a new run
        ingestion_manager.start_run(total_files=len(pdf_files))
        console.print(f"Started run with ID: [bold]{ingestion_manager.run_manager.run_id}[/bold]")
        if run_name:
            console.print(f"Run name: [bold]{run_name}[/bold]")
        console.print(f"Collection: [bold]{collection}[/bold]")
        console.print(f"Using embedding model: [bold]{embedding_model}[/bold]")

        # For each database record for this colleciton, if there is no pdf_file that matches the file_path, delete it from weaviate
        deleted = []
        for record in ingestion_manager.db_manager.get_all_records(collection):
            if record["file_path"] not in pdf_files:
                ingestion_manager.weaviate_manager.delete(collection, record["file_path"])
                deleted.append(record["file_path"])
                # delete it from the database also
                ingestion_manager.db_manager.delete_record(collection, record["file_path"])
        console.print(f"Deleted {len(deleted)} records from Weaviate")
        
        # Process each PDF file
        for pdf_file in pdf_files:
            status = ingestion_manager.ingest_file(pdf_file)
            status_color = {
                "success": "green",
                "error": "red",
                "skipped": "yellow",
                "already_processed": "blue"
            }.get(status, "white")
            console.print(f"Status: [{status_color}]{status}[/{status_color}]")
        
        # End the run and display statistics
        run_stats = ingestion_manager.end_run()
        console.print("\n[bold green]Run completed.[/bold green]")
        
        # Display run statistics
        _display_run_stats(run_stats, verbose)
        
        # Optionally run search query if provided
        if query:
            console.print(f"\nSearch query not yet implemented.")
            
    except Exception as e:
        console.print(f"[bold red]Error: {str(e)}[/bold red]")
        if ingestion_manager and hasattr(ingestion_manager, 'run_manager') and ingestion_manager.run_manager is not None and ingestion_manager.run_manager.run_id:
            # Mark the run as failed
            try:
                ingestion_manager.run_manager.status = "failed"
                ingestion_manager.run_manager.end_run()
            except Exception as inner_e:
                console.print(f"[bold red]Warning: Failed to mark run as failed: {str(inner_e)}[/bold red]")
        sys.exit(1)
    finally:
        # Ensure all resources are properly cleaned up
        if ingestion_manager:
            try:
                ingestion_manager.close()
            except Exception as cleanup_e:
                console.print(f"[bold red]Warning: Failed to clean up resources: {str(cleanup_e)}[/bold red]")
            
            # Import and clear CUDA cache if appropriate
            try:
                import torch
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
            except Exception:
                pass

@app.command("list-runs")
def list_runs(
    db_path: str = typer.Option(DefaultConfig().db_path, "--db", help="SQLite database file"),
    limit: int = typer.Option(10, "--limit", help="Limit for listing runs"),
):
    """List all ingestion runs."""
    db_manager = DatabaseManager(db_path)
    
    # Get all runs
    runs = db_manager.get_all_runs(limit=limit)
    if not runs:
        console.print("[bold yellow]No runs found in the database.[/bold yellow]")
        return
    
    console.print(f"\n[bold cyan]Recent Runs (showing {len(runs)} of {limit} requested)[/bold cyan]")
    
    # Create a table for run information
    table = Table(show_header=True, header_style="bold magenta", expand=False)
    table.add_column("Run ID", no_wrap=True)
    table.add_column("Run Name")
    table.add_column("Collection", no_wrap=True)
    table.add_column("Start Time")
    table.add_column("Status")
    table.add_column("Files")
    table.add_column("Processing Time")
    
    # Add run information to the table
    for run in runs:
        # Calculate file counts
        total_files = run.get("total_files", 0)
        processed = run.get("processed_files", 0)
        failed = run.get("failed_files", 0)
        skipped = run.get("skipped_files", 0)
        file_str = f"{processed}/{total_files} (F:{failed}, S:{skipped})"
        
        # Calculate processing time
        processing_time = run.get("total_processing_time", 0)
        if processing_time:
            minutes, seconds = divmod(processing_time, 60)
            hours, minutes = divmod(minutes, 60)
            time_str = f"{int(hours)}h {int(minutes)}m {seconds:.2f}s"
        else:
            time_str = "In progress"
        
        # Determine status color
        status = run.get("status", "N/A")
        status_color = {
            "running": "yellow",
            "completed": "green",
            "completed_with_errors": "yellow",
            "failed": "red"
        }.get(status, "white")
        
        # Get values from database columns
        run_name = run.get("run_name", "N/A")
        collection = run.get("collection", "N/A")
        
        table.add_row(
            run.get("run_id", "N/A"),
            run_name,
            collection,
            run.get("start_time", "N/A"),
            f"[{status_color}]{status}[/{status_color}]",
            file_str,
            time_str
        )
    
    console.print(table)
    console.print("\n[italic]To see detailed information for a specific run, use 'gobbler run-stats <run_id>'[/italic]")


@app.command("run-stats")
def run_stats(
    run_id: str = typer.Argument(..., help="Run ID to display statistics for"),
    db_path: str = typer.Option(DefaultConfig().db_path, "--db", help="SQLite database file"),
    verbose: bool = typer.Option(False, "--verbose", help="Display detailed information"),
):
    """Show statistics for a specific ingestion run."""
    db_manager = DatabaseManager(db_path)
    
    # Get run information
    run_info = db_manager.get_run(run_id)
    if not run_info:
        console.print(f"[bold red]Run with ID '{run_id}' not found.[/bold red]")
        return
    
    # Get ingestions for this run
    ingestions = db_manager.get_ingestions_for_run(run_id)
    
    # Get values from database columns
    run_name = run_info.get("run_name", "N/A")
    collection = run_info.get("collection", "N/A")
    
    # Display run information
    if run_name != "N/A":
        console.print(f"\n[bold cyan]Run Information for '{run_id}' (Name: {run_name})[/bold cyan]")
    else:
        console.print(f"\n[bold cyan]Run Information for '{run_id}'[/bold cyan]")
    
    # Create a table for run information
    table = Table(show_header=True, header_style="bold magenta", expand=False)
    table.add_column("Property")
    table.add_column("Value", no_wrap=False)
    
    # Add run information to the table
    if run_name != "N/A":
        table.add_row("Run Name", run_name)
    table.add_row("Collection", collection)
    table.add_row("Start Time", run_info.get("start_time", "N/A"))
    table.add_row("End Time", run_info.get("end_time", "N/A"))
    table.add_row("Status", run_info.get("status", "N/A"))
    table.add_row("Total Files", str(run_info.get("total_files", 0)))
    table.add_row("Processed Files", str(run_info.get("processed_files", 0)))
    table.add_row("Failed Files", str(run_info.get("failed_files", 0)))
    table.add_row("Skipped Files", str(run_info.get("skipped_files", 0)))
    
    # Display current Weaviate connection settings from environment
    weaviate_http_host = os.environ.get("WEAVIATE_HTTP_HOST", "weaviate.weaviate")
    weaviate_http_port = os.environ.get("WEAVIATE_HTTP_PORT", "80")
    table.add_row("Weaviate HTTP", f"{weaviate_http_host}:{weaviate_http_port}")
    
    # Get already processed files from database column
    already_processed = run_info.get("already_processed_files", 0)
    if already_processed > 0:
        table.add_row("Already Processed Files", str(already_processed))
    
    # Calculate processing time
    processing_time = run_info.get("total_processing_time", 0)
    if processing_time:
        minutes, seconds = divmod(processing_time, 60)
        hours, minutes = divmod(minutes, 60)
        time_str = f"{int(hours)}h {int(minutes)}m {seconds:.2f}s"
        table.add_row("Processing Time", time_str)
    
    console.print(table)
    
    # Display ingestion information if verbose
    if verbose and ingestions:
        console.print(f"\n[bold cyan]Ingestion Information ({len(ingestions)} files)[/bold cyan]")
        
        # Create a table for ingestion information
        ingestion_table = Table(show_header=True, header_style="bold magenta", expand=False)
        ingestion_table.add_column("File Path", no_wrap=True)
        ingestion_table.add_column("Status")
        ingestion_table.add_column("Time")
        ingestion_table.add_column("Issues")
        
        # Add ingestion information to the table
        for ingestion in ingestions:
            status = ingestion.get("status", "N/A")
            status_color = {
                "success": "green",
                "error": "red",
                "skipped": "yellow"
            }.get(status, "white")
            
            # Try to parse error message as JSON for more detailed information
            error_msg = ingestion.get("error_message", "None")
            try:
                error_data = json.loads(error_msg)
                if isinstance(error_data, dict) and "message" in error_data:
                    error_msg = error_data["message"]
            except (json.JSONDecodeError, TypeError):
                # Not JSON, use as is
                pass
            
            ingestion_table.add_row(
                os.path.basename(ingestion.get("file_path", "N/A")),
                f"[{status_color}]{status}[/{status_color}]",
                ingestion.get("ingestion_time", "N/A"),
                error_msg[:150] + ("..." if len(error_msg) > 150 else "")
            )
        
        console.print(ingestion_table)


@app.command("query")
def query(
    query_text: str = typer.Argument(..., help="Query text to search for"),
    collection: str = typer.Argument(..., help="Collection to search in"),
    embedding_model: str = typer.Option("BAAI/bge-large-en-v1.5", "--embedding-model", help="Embedding model to use"),
    limit: int = typer.Option(5, "--limit", help="Maximum number of results to return"),
    weaviate_host: str = typer.Option("weaviate.weaviate", "--weaviate-host", help="Weaviate HTTP host"),
    weaviate_port: int = typer.Option(80, "--weaviate-port", help="Weaviate HTTP port"),
    weaviate_grpc_host: str = typer.Option("weaviate-grpc.weaviate", "--weaviate-grpc-host", help="Weaviate gRPC host"),
    weaviate_grpc_port: int = typer.Option(50051, "--weaviate-grpc-port", help="Weaviate gRPC port"),
    weaviate_timeout: int = typer.Option(60, "--weaviate-timeout", help="Weaviate connection timeout in seconds"),
):
    """Search for chunks in a collection."""
    from sentence_transformers import SentenceTransformer
    from cake_gobbler.core.weaviate_manager import WeaviateManager
    
    # Validate collection name format
    if not re.match(r'^[A-Z][a-zA-Z0-9_]*$', collection):
        console.print("[bold red]Error: Collection name must start with a capital letter and contain only letters, numbers, and underscores.[/bold red]")
        sys.exit(1)
    
    # Create Weaviate configuration
    weaviate_config = WeaviateConfig(
        http_host=weaviate_host,
        http_port=weaviate_port,
        grpc_host=weaviate_grpc_host,
        grpc_port=weaviate_grpc_port,
        timeout=weaviate_timeout
    )
    
    # Create Weaviate manager
    weaviate_manager = WeaviateManager(weaviate_config)
    model = None
    
    try:
        # Display connection info
        console.print(f"Weaviate HTTP connection: [bold]{weaviate_host}:{weaviate_port}[/bold]")
        console.print(f"Weaviate gRPC connection: [bold]{weaviate_grpc_host}:{weaviate_grpc_port}[/bold]")
        
        # Connect to Weaviate
        try:
            weaviate_manager.connect()
            console.print("[bold green]Successfully connected to Weaviate![/bold green]")
        except ConnectionError as e:
            console.print(f"[bold red]Error: Could not connect to Weaviate: {str(e)}[/bold red]")
            console.print(f"[bold yellow]Please verify that Weaviate is running at {weaviate_host}:{weaviate_port}[/bold yellow]")
            sys.exit(1)
        
        # Load embedding model
        console.print(f"Loading embedding model: [bold]{embedding_model}[/bold]...")
        model = SentenceTransformer(embedding_model)
        
        # Embed query
        console.print(f"Embedding query: [italic]\"{query_text}\"[/italic]")
        query_embedding = model.encode(query_text).tolist()
        
        # Search
        console.print(f"Searching collection: [bold]{collection}[/bold]")
        results = weaviate_manager.search(collection, query_embedding, limit=limit)
        
        # Display results
        console.print(f"\n[bold green]Search Results:[/bold green]")
        
        for i, result in enumerate(results):
            console.print(f"\n[bold cyan]Result {i+1}:[/bold cyan]")
            score = result.get('score')
            if score is not None:
                console.print(f"[bold]Score:[/bold] {score:.4f}")
            else:
                console.print("[bold]Score:[/bold] N/A")
            console.print(f"[bold]Source:[/bold] {result['metadata'].get('source', 'Unknown')}")
            console.print(f"[bold]Text:[/bold] {result['text'][:300]}...")
    finally:
        # Proper cleanup of resources
        if weaviate_manager:
            try:
                weaviate_manager.close()
            except Exception as e:
                console.print(f"[bold red]Warning: Failed to close Weaviate connection: {str(e)}[/bold red]")
        
        # Clean up any CUDA resources from the model
        if model:
            import torch
            try:
                # Clear CUDA cache
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
            except Exception as e:
                console.print(f"[bold red]Warning: Failed to clear CUDA cache: {str(e)}[/bold red]")


@app.command("list-ingestions")
def list_ingestions(
    db_path: str = typer.Option(DefaultConfig().db_path, "--db", help="SQLite database file"),
    limit: int = typer.Option(20, "--limit", help="Maximum number of ingestions to show"),
):
    """List recent ingestion records."""
    db_manager = DatabaseManager(db_path)
    ingestions = db_manager.get_ingestion_log(limit=limit)
    
    if not ingestions:
        console.print("[bold yellow]No ingestion records found.[/bold yellow]")
        return
    
    console.print(f"\n[bold cyan]Recent Ingestions (showing {len(ingestions)} of {limit} requested)[/bold cyan]")
    
    # Create a table for ingestion information
    table = Table(show_header=True, header_style="bold magenta", expand=False)
    table.add_column("ID", no_wrap=True)
    table.add_column("File", no_wrap=True)
    table.add_column("Status")
    table.add_column("Run ID", no_wrap=True)
    table.add_column("Time")
    table.add_column("Issues/Notes")
    
    # Add ingestion information to the table
    for ingestion in ingestions:
        status = ingestion.get("status", "N/A")
        status_color = {
            "success": "green",
            "error": "red",
            "skipped": "yellow",
            "already_processed": "blue"
        }.get(status, "white")
        
        filename = os.path.basename(ingestion.get("file_path", "N/A"))
        
        notes = ingestion.get("error_message", "")
        # Try to parse as JSON for more detailed information
        try:
            error_data = json.loads(notes)
            if isinstance(error_data, dict) and "message" in error_data:
                notes = error_data["message"]
        except (json.JSONDecodeError, TypeError):
            # Not JSON, use as is
            pass
            
        if not notes and status == "success":
            # Extract some info from analysis_result
            analysis_json = ingestion.get("analysis_result")
            if analysis_json:
                try:
                    analysis = json.loads(analysis_json)
                    num_pages = analysis.get("num_pages", 0)
                    notes = f"Pages: {num_pages}"
                except json.JSONDecodeError:
                    pass
        
        table.add_row(
            str(ingestion.get("id", "-")),
            filename,
            f"[{status_color}]{status}[/{status_color}]",
            ingestion.get("run_id", "N/A"),
            ingestion.get("ingestion_time", "N/A").split("T")[0],  # Just the date part
            notes[:50] + ("..." if len(notes) > 50 else "")
        )
    
    console.print(table)
    console.print("\n[italic]To see detailed information for a specific ingestion, use 'gobbler ingestion-details --id <id>'[/italic]")


@app.command("ingestion-details")
def ingestion_details(
    id: int = typer.Option(None, "--id", help="ID of the ingestion record"),
    file: str = typer.Option(None, "--file", help="Path to the PDF file"),
    db_path: str = typer.Option(DefaultConfig().db_path, "--db", help="SQLite database file"),
):
    """Show detailed information about a specific ingestion."""
    if not id and not file:
        console.print("[bold red]Error: Either --id or --file must be provided.[/bold red]")
        return
    
    db_manager = DatabaseManager(db_path)
    
    # Get ingestion record
    ingestion = None
    if id:
        # This is a placeholder since we don't have a specific method to get by ID
        ingestions = db_manager.get_ingestion_log(limit=1000)
        for record in ingestions:
            if record.get("id") == id:
                ingestion = record
                break
    elif file:
        ingestion = db_manager.get_ingestion_by_file(file)
    
    if not ingestion:
        console.print("[bold red]Ingestion record not found.[/bold red]")
        return
    
    # Display ingestion information
    console.print(f"\n[bold cyan]Ingestion Details for '{os.path.basename(ingestion.get('file_path', 'Unknown'))}'[/bold cyan]")
    
    # Create a table for ingestion information
    table = Table(show_header=True, header_style="bold magenta", expand=False)
    table.add_column("Property")
    table.add_column("Value", no_wrap=False, max_width=100)
    
    # Add basic ingestion information
    table.add_row("ID", str(ingestion.get("id", "-")))
    table.add_row("File Path", ingestion.get("file_path", "N/A"))
    table.add_row("Status", ingestion.get("status", "N/A"))
    table.add_row("Run ID", ingestion.get("run_id", "N/A"))
    table.add_row("Ingestion Time", ingestion.get("ingestion_time", "N/A"))
    
    # Add error message if any
    error_message = ingestion.get("error_message", "")
    if error_message:
        # Try to parse as JSON for detailed error information
        try:
            error_data = json.loads(error_message)
            if isinstance(error_data, dict) and "message" in error_data:
                # This is a structured error message with diagnostics
                table.add_row("Error Message", error_data["message"])
                
                # Add extraction diagnostics if available
                if "extraction_diagnostics" in error_data:
                    extraction = error_data["extraction_diagnostics"]
                    if extraction:
                        table.add_row("Extraction Failure", extraction.get("failure_reason", "Unknown"))
                        table.add_row("Failure Details", extraction.get("failure_details", "No details"))
                        
                        # Add page count if available
                        if "page_count" in extraction:
                            table.add_row("Page Count", str(extraction["page_count"]))
                        
                        # Add extraction attempts if available
                        attempts = extraction.get("extraction_attempts", [])
                        if attempts:
                            attempt_details = []
                            for i, attempt in enumerate(attempts):
                                status = "✓" if attempt.get("success", False) else "✗"
                                encoding = attempt.get("encoding", "unknown")
                                note = f" ({attempt.get('note')})" if "note" in attempt else ""
                                attempt_details.append(f"{status} {encoding}{note}")
                            
                            table.add_row("Extraction Attempts", ", ".join(attempt_details))
                
                # Add analysis diagnostics if available
                if "analysis_diagnostics" in error_data:
                    analysis_diag = error_data["analysis_diagnostics"]
                    if analysis_diag:
                        if "issues_found" in analysis_diag and analysis_diag["issues_found"]:
                            table.add_row("Issues Found", ", ".join(analysis_diag["issues_found"]))
                        
                        if "primary_rejection_reason" in analysis_diag:
                            table.add_row("Primary Rejection Reason", analysis_diag["primary_rejection_reason"] or "None")
            else:
                # Just a simple JSON string or object
                table.add_row("Error Message", error_message)
        except json.JSONDecodeError:
            # Not JSON, just a regular string
            table.add_row("Error Message", error_message)
    
    # Parse analysis_result if available
    analysis_json = ingestion.get("analysis_result")
    if analysis_json:
        try:
            analysis = json.loads(analysis_json)
            table.add_row("Pages", str(analysis.get("num_pages", 0)))
            table.add_row("File Size", f"{analysis.get('filesize', 0) / 1024 / 1024:.2f} MB")
            table.add_row("Encrypted", "Yes" if analysis.get("is_encrypted", False) else "No")
            table.add_row("Damaged", "Yes" if analysis.get("is_damaged", False) else "No")
            
            # Add encoding types
            encoding_types = analysis.get("encoding_types", [])
            if encoding_types:
                table.add_row("Encoding Types", ", ".join(encoding_types))
            
            # Add font information
            fonts = analysis.get("fonts", [])
            if fonts:
                font_types = set(font.get("type", "") for font in fonts)
                table.add_row("Font Types", ", ".join(font_types))
                table.add_row("Fonts Count", str(len(fonts)))
            
            # Add issues
            issues = analysis.get("issues", [])
            if issues:
                issue_types = [issue.get("type", "") for issue in issues]
                table.add_row("Issues", ", ".join(issue_types))
                
                # Add detailed issues
                issues_details = []
                for i, issue in enumerate(issues):
                    issue_text = f"{issue.get('type', '')}: {issue.get('description', '')} (Severity: {issue.get('severity', '')})"
                    issues_details.append(issue_text)
                
                # Instead of adding many rows, add one detailed row that can wrap properly
                if issues_details:
                    table.add_row("Issues Details", "\n".join(issues_details))
            
            # Check for diagnostics in metadata
            if "metadata" in analysis and "diagnostics" in analysis["metadata"]:
                diagnostics = analysis["metadata"]["diagnostics"]
                
                # Add acceptance check diagnostics
                if "acceptance_check" in diagnostics:
                    acceptance = diagnostics["acceptance_check"]
                    if "issues_found" in acceptance and acceptance["issues_found"]:
                        table.add_row("Analysis Issues", ", ".join(acceptance["issues_found"]))
                
                # Add text extraction diagnostics
                if "text_extraction" in diagnostics:
                    extraction = diagnostics["text_extraction"]
                    if extraction:
                        if "failure_reason" in extraction:
                            table.add_row("Extraction Failure", extraction["failure_reason"])
                            table.add_row("Failure Details", extraction.get("failure_details", "No details"))
        except json.JSONDecodeError:
            console.print("[bold yellow]Failed to parse analysis results.[/bold yellow]")
    
    console.print(table)
    
    # If we have detailed diagnostics, show them in a separate section
    if analysis_json:
        try:
            analysis = json.loads(analysis_json)
            if "metadata" in analysis and "diagnostics" in analysis["metadata"]:
                console.print("\n[bold cyan]Detailed Diagnostics[/bold cyan]")
                console.print(json.dumps(analysis["metadata"]["diagnostics"], indent=2))
        except json.JSONDecodeError:
            pass
    
    # If we have a structured error message with diagnostics, show them in a separate section
    if error_message:
        try:
            error_data = json.loads(error_message)
            if isinstance(error_data, dict) and ("extraction_diagnostics" in error_data or "analysis_diagnostics" in error_data):
                console.print("\n[bold cyan]Detailed Error Diagnostics[/bold cyan]")
                
                # Display extraction diagnostics
                if "extraction_diagnostics" in error_data:
                    console.print("\n[bold cyan]Text Extraction Diagnostics[/bold cyan]")
                    extraction_diag = error_data["extraction_diagnostics"]
                    if extraction_diag:
                        # Display page count
                        console.print(f"Page count: {extraction_diag.get('page_count', 0)}")
                        
                        # Display failure reason and details
                        if "failure_reason" in extraction_diag:
                            console.print(f"[bold red]Failure reason: {extraction_diag['failure_reason']}[/bold red]")
                            console.print(f"Failure details: {extraction_diag.get('failure_details', 'No details')}")
                        
                        # Display extraction attempts
                        attempts = extraction_diag.get("extraction_attempts", [])
                        if attempts:
                            console.print("\n[bold]Extraction attempts:[/bold]")
                            attempts_table = Table(show_header=True, header_style="bold blue")
                            attempts_table.add_column("Attempt")
                            attempts_table.add_column("Encoding")
                            attempts_table.add_column("Status")
                            attempts_table.add_column("Notes", no_wrap=False, max_width=60)
                            
                            for i, attempt in enumerate(attempts):
                                status = "✓" if attempt.get("success", False) else "✗"
                                status_color = "green" if attempt.get("success", False) else "red"
                                encoding = attempt.get("encoding", "unknown")
                                note = attempt.get("note", "")
                                error = attempt.get("error", "")
                                if error and not note:
                                    note = error
                                
                                attempts_table.add_row(
                                    str(i+1),
                                    encoding,
                                    f"[{status_color}]{status}[/{status_color}]",
                                    note
                                )
                            
                            console.print(attempts_table)
                
                # Display analysis diagnostics
                if "analysis_diagnostics" in error_data:
                    console.print("\n[bold cyan]Analysis Diagnostics[/bold cyan]")
                    analysis_diag = error_data["analysis_diagnostics"]
                    if analysis_diag:
                        if "issues_found" in analysis_diag and analysis_diag["issues_found"]:
                            console.print("[bold]Issues found:[/bold]", ", ".join(analysis_diag["issues_found"]))
                        
                        if "primary_rejection_reason" in analysis_diag:
                            console.print("[bold]Primary rejection reason:[/bold]", 
                                         analysis_diag["primary_rejection_reason"] or "None")
                        
                        if "all_issues" in analysis_diag and analysis_diag["all_issues"]:
                            console.print("\n[bold]All detected issues:[/bold]")
                            issues_table = Table(show_header=True, header_style="bold blue")
                            issues_table.add_column("Type")
                            issues_table.add_column("Description", no_wrap=False, max_width=60)
                            issues_table.add_column("Severity")
                            issues_table.add_column("Pages")
                            
                            for issue in analysis_diag["all_issues"]:
                                severity_color = {
                                    "high": "red",
                                    "medium": "yellow",
                                    "low": "blue"
                                }.get(issue.get("severity", "").lower(), "white")
                                
                                pages = "N/A"
                                if issue.get("page_numbers"):
                                    if isinstance(issue["page_numbers"], list):
                                        pages = ", ".join(map(str, issue["page_numbers"]))
                                    else:
                                        pages = str(issue["page_numbers"])
                                
                                issues_table.add_row(
                                    issue.get("type", "Unknown"),
                                    issue.get("description", "No description"),
                                    f"[{severity_color}]{issue.get('severity', 'Unknown')}[/{severity_color}]",
                                    pages
                                )
                            
                            console.print(issues_table)
        except json.JSONDecodeError:
            pass


def _display_run_stats(run_stats):
    """Display run statistics in a formatted table."""
    table = Table(show_header=True, header_style="bold magenta", expand=False)
    table.add_column("Property")
    table.add_column("Value", no_wrap=False)
    
    # Add run information to the table
    table.add_row("Run ID", run_stats.run_id)
    
    # Add run name and collection from dedicated fields
    if run_stats.run_name:
        table.add_row("Run Name", run_stats.run_name)
    if run_stats.collection:
        table.add_row("Collection", run_stats.collection)
    
    # Add start time and end time
    if hasattr(run_stats, 'start_time') and run_stats.start_time:
        table.add_row("Started", run_stats.start_time)
    if hasattr(run_stats, 'end_time') and run_stats.end_time:
        table.add_row("Ended", run_stats.end_time)
    
    # Calculate and display duration if both start and end times are available
    if hasattr(run_stats, 'start_time') and hasattr(run_stats, 'end_time') and run_stats.start_time and run_stats.end_time:
        from datetime import datetime
        try:
            # Parse the datetime strings
            start_dt = datetime.fromisoformat(run_stats.start_time.replace('Z', '+00:00'))
            end_dt = datetime.fromisoformat(run_stats.end_time.replace('Z', '+00:00'))
            
            # Calculate duration
            duration = end_dt - start_dt
            
            # Format duration as hours, minutes, seconds
            total_seconds = duration.total_seconds()
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            
            duration_str = f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
            table.add_row("Duration", duration_str)
        except (ValueError, TypeError) as e:
            # If there's an error parsing the dates, just skip the duration
            pass
        
    table.add_row("Status", run_stats.status)
    table.add_row("Total Files", str(run_stats.total_files))
    table.add_row("Processed Files", str(run_stats.processed_files))
    table.add_row("Failed Files", str(run_stats.failed_files))
    table.add_row("Skipped Files", str(run_stats.skipped_files))
    if run_stats.already_processed_files > 0:
        table.add_row("Already Processed Files", str(run_stats.already_processed_files))
    table.add_row("Completion", f"{run_stats.completion_percentage:.2f}%")
    
        
    # Add chunk size and overlap if available
    if run_stats.chunk_size:
        table.add_row("Chunk Size", str(run_stats.chunk_size))
    if run_stats.chunk_overlap:
        table.add_row("Chunk Overlap", str(run_stats.chunk_overlap))
    
    console.print(table)


if __name__ == "__main__":
    app()
