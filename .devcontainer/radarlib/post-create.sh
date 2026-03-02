#!/bin/bash
# Install development dependencies
pip install --no-cache-dir -r requirements-dev.txt
# Install pre-commit hooks
pre-commit install
