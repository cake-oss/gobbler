# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# Cake Gobbler Commands & Coding Guidelines

## Commands
- **Install**: `uv tool install .`
- **Run application**: `./run.sh --collection CollectionName [PDF_PATH]`
- **Run with Ray**: `./run_ray.sh --collection CollectionName [PDF_PATH]`
- **Dev mode**: `uv run -m cake_gobbler.cli.main [command]`
- **List runs**: `gobbler list-runs`
- **View run stats**: `gobbler run-stats [run-id]`
- **Query DB**: `gobbler query [query_text] [collection]`

## Code Style
- Python 3.12+ with strict type hints (no Any)
- PEP 8 with descriptive docstrings
- Imports: standard lib → third-party → local modules
- Use dataclasses for models
- Naming: snake_case (vars/functions), PascalCase (classes)
- Include SPDX license header on all files
- Error handling: structured reporting with diagnostics
- Return tuple for functions with multiple returns (success, data)
- Use pathlib for file operations
- Rich terminal UI for all user feedback

## Development
- Use `uv` for package management
- Ray parallel options: `--num-processors N`, `--sequential`, `--ray-address`
- Code organization follows domain-driven design principles
- Weaviate used for vector database operations