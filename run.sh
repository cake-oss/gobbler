#!/bin/bash
# Script to run the PDF ingestion system with enhanced analysis and run management

# Set environment variables to suppress ResourceWarnings
export PYTHONWARNINGS="ignore::ResourceWarning"

# Display usage information
function show_usage {
    echo "Usage: $0 [OPTIONS] [PDF_PATH]"
    echo ""
    echo "Arguments:"
    echo "  PDF_PATH            Path to a PDF file or directory of PDFs (default: sample_pdfs)"
    echo ""
    echo "Options:"
    echo "  --verbose           Enable verbose logging"
    echo "  --collection NAME   Specify a collection name (REQUIRED)"
    echo "  --no-analysis       Skip showing analysis results after processing"
    echo "  --run-id ID         Specify a run ID (optional, will be generated if not provided)"
    echo "  --run-name NAME     Specify a human-readable name for the run"
    echo "  --list-runs         List all runs"
    echo "  --run-stats ID      Show statistics for a specific run"
    echo "  --limit N           Limit for listing runs (default: 10)"
    echo "  --weaviate-host H   Specify Weaviate HTTP host (overrides .env)"
    echo "  --weaviate-port P   Specify Weaviate HTTP port (overrides .env)"
    echo "  --help              Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --collection MyCollection              # Process default directory with required collection name"
    echo "  $0 --collection MyCollection /path/to/pdfs # Process PDFs in the specified directory"
    echo "  $0 --collection MyCollection --run-name \"My Run\" /path/to/pdf # Process with a named run"
    echo "  $0 --list-runs                      # List all runs"
    echo "  $0 --run-stats run-123              # Show statistics for a specific run"
    exit 1
}

# Check if help is requested
if [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
    show_usage
fi

# Handle run management commands that don't require a PDF path
if [ "$1" == "--list-runs" ]; then
    echo "Listing all runs:"
    uv run -m cake_gobbler.cli.main list-runs
    exit 0
fi

if [ "$1" == "--run-stats" ]; then
    if [ -z "$2" ]; then
        echo "Error: Run ID is required for --run-stats"
        show_usage
    fi
    echo "Showing run statistics for $2:"
    uv run -m cake_gobbler.cli.main run-stats "$2" --verbose
    exit 0
fi

# Default values
PDF_PATH="sample_pdfs"
VERBOSE=""
COLLECTION=""  # No default collection - must be specified
SHOW_ANALYSIS=true
RUN_ID=""
RUN_NAME=""
LIMIT="10"
WEAVIATE_HTTP_HOST=""
WEAVIATE_HTTP_PORT=""
WEAVIATE_GRPC_HOST=""
WEAVIATE_GRPC_PORT=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --verbose)
            VERBOSE="--verbose"
            echo "Verbose mode enabled"
            shift
            ;;
        --collection)
            COLLECTION="$2"
            echo "Using collection: $COLLECTION"
            shift 2
            ;;
        --no-analysis)
            SHOW_ANALYSIS=false
            echo "Analysis display disabled"
            shift
            ;;
        --run-id)
            RUN_ID="--run-id $2"
            echo "Using run ID: $2"
            shift 2
            ;;
        --run-name)
            RUN_NAME="--run-name $2"
            echo "Using run name: $2"
            shift 2
            ;;
        --limit)
            LIMIT="$2"
            shift 2
            ;;
        --weaviate-http-host)
            WEAVIATE_HTTP_HOST="--weaviate-http-host $2"
            echo "Using Weaviate HTTP host: $2"
            shift 2
            ;;
        --weaviate-http-port)
            WEAVIATE_HTTP_PORT="--weaviate-http-port $2"
            echo "Using Weaviate HTTP port: $2"
            shift 2
            ;;
        --weaviate-grpc-host)
            WEAVIATE_GRPC_HOST="--weaviate-grpc-host $2"
            echo "Using Weaviate gRPC host: $2"
            shift 2
            ;;
        --weaviate-grpc-port)
            WEAVIATE_GRPC_PORT="--weaviate-grpc-port $2"
            echo "Using Weaviate gRPC port: $2"
            shift 2
            ;;
        --list-runs)
            echo "Listing all runs:"
            uv run -m cake_gobbler.cli.main list-runs --limit "$LIMIT"
            exit 0
            ;;
        --run-stats)
            echo "Showing run statistics for $2:"
            uv run -m cake_gobbler.cli.main run-stats "$2" --verbose
            exit 0
            ;;
        --help)
            show_usage
            ;;
        *)
            # If it's not a flag, assume it's the PDF path
            if [[ ! $1 == --* ]]; then
                PDF_PATH="$1"
            else
                echo "Unknown option: $1"
                show_usage
            fi
            shift
            ;;
    esac
done

# Verify required parameters
if [ -z "$COLLECTION" ]; then
    echo "Error: Collection name is required. Use --collection to specify a collection name."
    show_usage
fi

# Check if the path exists
if [ ! -e "$PDF_PATH" ]; then
    echo "Error: Path '$PDF_PATH' does not exist"
    exit 1
fi

# Create a timestamp for logging
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
if [ ! -d "logs" ]; then
    mkdir logs
fi
LOG_FILE="logs/run_${TIMESTAMP}.log"

# If no run ID is specified, use the timestamp
if [ -z "$RUN_ID" ]; then
    RUN_ID="--run-id run-${TIMESTAMP}"
    echo "Using generated run ID: run-${TIMESTAMP}"
fi

echo "Starting run on: $PDF_PATH"
echo "Collection: $COLLECTION"
echo "Log file: $LOG_FILE"

# Build the command
CMD="uv run -m cake_gobbler.cli.main ingest --pdf \"$PDF_PATH\" --collection \"$COLLECTION\""

# Add optional arguments
if [ ! -z "$VERBOSE" ]; then
  CMD="$CMD $VERBOSE"
fi

if [ ! -z "$RUN_ID" ]; then
  CMD="$CMD $RUN_ID"
fi

if [ ! -z "$RUN_NAME" ]; then
  # Extract the run name from the RUN_NAME variable (remove --run-name)
  NAME="${RUN_NAME#--run-name }"
  CMD="$CMD --run-name \"$NAME\""
fi

if [ ! -z "$WEAVIATE_HTTP_HOST" ]; then
  CMD="$CMD $WEAVIATE_HTTP_HOST"
fi

if [ ! -z "$WEAVIATE_HTTP_PORT" ]; then
  CMD="$CMD $WEAVIATE_HTTP_PORT"
fi

if [ ! -z "$WEAVIATE_GRPC_HOST" ]; then
  CMD="$CMD $WEAVIATE_GRPC_HOST"
fi

if [ ! -z "$WEAVIATE_GRPC_PORT" ]; then
  CMD="$CMD $WEAVIATE_GRPC_PORT"
fi

# Set environment variables to suppress ResourceWarnings
export PYTHONWARNINGS="ignore::ResourceWarning"

echo "Executing command: $CMD"

# Execute the command and capture output
{
    eval $CMD 2>&1 
} | tee "$LOG_FILE"

# Extract the run ID from the log file if it was generated
if [[ "$RUN_ID" == *"run-${TIMESTAMP}"* ]]; then
    ACTUAL_RUN_ID=$(grep "Started run with ID:" "$LOG_FILE" | awk '{print $NF}')
    if [ ! -z "$ACTUAL_RUN_ID" ]; then
        RUN_ID="$ACTUAL_RUN_ID"
    fi
fi

# Check if the processing was successful
if [ ${PIPESTATUS[0]} -eq 0 ]; then
    echo -e "\nPDF processing completed successfully"
    
    # Show run statistics
    echo -e "\nShowing run statistics:"
    uv run -m cake_gobbler.cli.main run-stats "$RUN_ID"
    
    # Show analysis results if enabled
    if [ "$SHOW_ANALYSIS" = true ]; then
        echo -e "\nShowing analysis results:"
        if [ -d "$PDF_PATH" ]; then
            # If it's a directory, list all processed PDFs
            uv run -m cake_gobbler.cli.main list-ingestions
        else
            # If it's a file, show details for that specific file
            uv run -m cake_gobbler.cli.main ingestion-details --file "$PDF_PATH"
        fi
    fi
else
    echo -e "\nPDF processing encountered errors. Check the log file: $LOG_FILE"
fi

echo "Run completed at: $(date)"
