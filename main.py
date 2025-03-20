#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2025 Cake AI Technologies, Inc.
#
# SPDX-License-Identifier: Apache-2.0

"""
Cake Gobbler - PDF Ingestion System

This script serves as the main entry point for the application.
It uses the centralized resource management system and forwards
all arguments to the modular CLI implementation.
"""

import sys
import warnings

from cake_gobbler.cli.main import app

# Suppress ResourceWarnings about temporary directories
warnings.filterwarnings("ignore", category=ResourceWarning, message="Implicitly cleaning up")

if __name__ == "__main__":
    sys.exit(app())
