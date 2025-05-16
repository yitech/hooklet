#!/bin/bash
# Script to run all linters on the ./hooklet directory

set -e  # Exit immediately if a command exits with a non-zero status

echo "Running black..."
black ./hooklet

echo "Running isort..."
isort ./hooklet

echo "Running flake8..."
flake8 ./hooklet

echo "Running pylint..."
pylint ./hooklet

echo "Running mypy..."
mypy ./hooklet

echo "All linting completed successfully!"
