# Cake Gobbler - PDF Ingestion System
![Cake Gobbler Mascot gobbling up documents](https://github.com/user-attachments/assets/a3907897-56b0-4874-ac10-ea0b8d6910c1)

This system analyzes PDF files for encoding issues and other potential problems, extracts text, splits it into chunks, embeds the content, and stores the embedded chunks in Weaviate. It also logs the ingestion process (both successes and errors) in a SQLite database with enhanced analysis data and provides run management capabilities to track processing of multiple files.

## Features

- **PDF Analysis**: Analyzes PDFs for encoding types, fonts, and potential issues
- **Text Extraction**: Extracts text from PDFs using PyMuPDF via external process (AGPL compliant)
- **Intelligent Encoding Detection**: Uses chardet to automatically detect text encoding with confidence scores
- **Chunking**: Splits text into chunks for embedding
- **Embedding**: Embeds chunks using Sentence Transformers
- **Storage**: Stores embedded chunks in Weaviate
- **Logging**: Logs ingestion process with detailed analysis results in SQLite
- **Run Management**: Tracks processing of multiple files and collects run statistics
- **License Compliance**: Uses PyMuPDF as an external process to comply with AGPL licensing requirements

## Enhanced Analysis Data

The system captures and stores detailed analysis data for each PDF, including:

- **Encoding Types**: All detected encoding types (ASCII, UTF8, UTF16, etc.)
  - Uses chardet library for intelligent encoding detection with confidence scores
  - Falls back to a sequence of common encodings if automatic detection fails
  - Tracks all encoding attempts and their success/failure status
  - Records confidence scores for detected encodings
- **Font Information**: Details about all fonts used in the PDF
  - Font name
  - Font type
  - Font encoding
  - Whether the font is embedded
  - Whether the font is subset
- **Issues**: All detected issues with the PDF
  - Issue type
  - Description
  - Severity
  - Affected pages
  - Additional details
- **Metadata**: PDF metadata such as author, title, etc.

## Project Structure

The project now uses a modular structure with Python packages:

```
cake_gobbler/
├── cli/              # Command-line interface
│   ├── __init__.py
│   └── main.py       # Main CLI application
├── core/             # Core functionality
│   ├── __init__.py
│   ├── db_manager.py # Database operations
│   ├── ingestion.py  # Core ingestion logic
│   ├── pdf_analyzer.py # PDF analysis
│   ├── pdf_processor.py # PDF processing
│   ├── run_manager.py # Run management
│   ├── text_processor.py # Text chunking and processing
│   └── weaviate_manager.py # Weaviate operations
├── models/           # Data models
│   ├── __init__.py
│   ├── config.py     # Configuration models
│   ├── db_models.py  # Database models
│   └── pdf_models.py # PDF-related models
└── utils/            # Utility functions
    ├── __init__.py
    ├── file_utils.py # File operations
    └── logging.py    # Logging utilities
```

Legacy scripts:

- `main.py`: Compatibility wrapper for the CLI
- `run.sh`: Shell script that now calls the CLI

## Installation

You can install the package using `uv`:

```bash
# Install as a tool
uv tool install .

# Now you can run the CLI directly
gobbler --help
```

### Uninstallation

```bash
# Uninstall the CLI
uv tool uninstall cake-gobbler
```

### Development Mode

During development, you can run the CLI without installing it:

```bash
# Run directly with uv
uv run cake_gobbler/cli/main.py --help

# Or run the main script
uv run main.py --help
```

## Configuration

You can configure the system using environment variables by creating a `.env` file in the project root. A sample configuration file (`.env.sample`) is provided as a reference.

Key configuration options:

### Weaviate Connection

- `WEAVIATE_HTTP_HOST`: Weaviate HTTP host (default: weaviate.weaviate)
- `WEAVIATE_HTTP_PORT`: Weaviate HTTP port (default: 80)
- `WEAVIATE_GRPC_HOST`: Weaviate gRPC host (default: weaviate-grpc.weaviate)
- `WEAVIATE_GRPC_PORT`: Weaviate gRPC port (default: 50051)
- `WEAVIATE_TIMEOUT`: Weaviate connection timeout in seconds (default: 60)

Note: When using command line flags, use `--weaviate-host` and `--weaviate-port` (without the "HTTP" in the parameter name).

### PDF Processing

- `CHUNK_SIZE`: Number of tokens per chunk (default: 1024)
- `CHUNK_OVERLAP`: Number of tokens to overlap between chunks (default: 20)
- `EMBEDDING_MODEL`: Embedding model to use (default: BAAI/bge-large-en-v1.5)

## Usage

### Using the Convenience Script

The easiest way to use the system is with the `run.sh` script:

```bash
# Run with collection name (required)
./run.sh --collection MyCollection

# Run with a specific PDF file or directory
./run.sh --collection MyCollection /path/to/pdfs

# Run with verbose logging
./run.sh --collection MyCollection --verbose

# Run with a specific run ID
./run.sh --collection MyCollection --run-id my-run-1

# Run with a human-readable name
./run.sh --collection MyCollection --run-name "My PDF Processing Run"

# Override Weaviate connection settings
./run.sh --collection MyCollection --weaviate-host customhost --weaviate-port 9090

# List all runs
./run.sh --list-runs

# Show statistics for a specific run
./run.sh --run-stats run-123

# Skip showing analysis results
./run.sh --no-analysis
```

### Using the CLI Directly

You can use the CLI directly with the `rag` command (if installed) or via the module:

```bash
# Using the installed command
gobbler ingest --pdf <pdf_file_or_directory> --collection <collection_name> [options]

# Using the Python module
python -m cake_gobbler.cli.main ingest --pdf <pdf_file_or_directory> --collection <collection_name> [options]
```

#### Available Commands

- `ingest`: Process PDFs and store them in Weaviate
- `query`: Search for chunks in a collection
- `list-runs`: List all ingestion runs
- `run-stats`: Show detailed statistics for a specific run
- `list-ingestions`: List recent ingestion records
- `ingestion-details`: Show detailed information about a specific ingestion

#### Common Options

- `--pdf`: Path to a PDF file or directory of PDFs (required for ingestion)
- `--collection`: Weaviate collection name (required for ingestion/query)
- `--chunk-size`: Number of tokens per chunk (default: 1024)
- `--chunk-overlap`: Number of tokens to overlap between chunks (default: 20)
- `--embedding-model`: Embedding model to use (default: BAAI/bge-large-en-v1.5)
- `--weaviate-host`: Weaviate HTTP host (default: weaviate.weaviate)
- `--weaviate-port`: Weaviate HTTP port (default: 80)
- `--weaviate-grpc-host`: Weaviate gRPC host (default: weaviate-grpc.weaviate)
- `--weaviate-grpc-port`: Weaviate gRPC port (default: 50051)
- `--db-path`: SQLite database file path (default: cake-gobbler-log.db)
- `--verbose`: Enable verbose logging
- `--run-id`: Specify a run ID (optional, will be generated if not provided)
- `--run-name`: Specify a human-readable name for the run

For more information on each command, use the `--help` flag:

```bash
gobbler --help
gobbler ingest --help
gobbler query --help
# etc.
```

## Database Schema

The database schema includes the following tables:

### Ingestion Log Table

- `id`: Primary key
- `file_path`: Path to the PDF file
- `status`: Status of the ingestion (success, error, skipped)
- `error_message`: Error message if any
- `issues`: JSON string of issues if any
- `ingestion_time`: Timestamp of the ingestion
- `encoding_types`: JSON array of encoding types
- `is_encrypted`: Whether the PDF is encrypted
- `is_damaged`: Whether the PDF is damaged
- `num_pages`: Number of pages in the PDF
- `filesize`: Size of the PDF in bytes
- `fonts`: JSON array of font details
- `analysis_result`: Complete JSON of the analysis result
- `run_id`: Reference to the run that processed this file
- `file_fingerprint`: Unique fingerprint for the file
- `file_mtime`: Last modification time of the file

### Runs Table

- `run_id`: Primary key, unique identifier for the run
- `start_time`: Timestamp when the run started
- `end_time`: Timestamp when the run ended
- `status`: Status of the run (running, completed, completed_with_errors, failed)
- `total_files`: Total number of files to process
- `processed_files`: Number of successfully processed files
- `failed_files`: Number of failed files
- `skipped_files`: Number of skipped files
- `total_processing_time`: Total processing time in seconds
- `metadata`: JSON string of metadata about the run (includes run name if provided)

### Resource Management

All CLI commands properly clean up resources after use:

- Weaviate connections are explicitly closed to prevent leaks
- CUDA memory is cleared after tensor operations with PyTorch
- All database connections are properly managed

Some ResourceWarnings may still appear in logs related to temporary directories and SWIG bindings, but these are handled internally by dependencies and don't affect functionality.

## Dependencies

- Python 3.12+
- PyMuPDF 1.21.1+ (used via external process for AGPL compliance)
- Weaviate Client 4.11.1+
- Sentence Transformers 2.2.2+
- NLTK 3.8.1+
- LangChain 0.1.0+
- LangChain Text Splitters 0.0.1+
- PikePDF 8.0.0+
- Rich 13.0.0+
- Chardet 5.0.0+
- Typer 0.9.0+

## License Compliance

This project uses PyMuPDF (a Python binding for MuPDF), which is licensed under the AGPL-3.0 license. To comply with this license while using the software in a commercial context without the AGPL requirements applying to the rest of the codebase:

1. PyMuPDF is isolated in a separate Python process
2. Communication happens via stdin/stdout with JSON
3. The main application never directly imports or links to PyMuPDF

This approach allows for commercial use of the application without requiring the entire codebase to be licensed under AGPL-3.0.
