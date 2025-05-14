#!/bin/bash
# Script to run all linters on the ./ems directory

set -e  # Exit immediately if a command exits with a non-zero status

echo "Running black..."
black ./ems

echo "Running isort..."
isort ./ems

echo "Running flake8..."
flake8 ./ems

echo "Running pylint..."
pylint ./ems

echo "Running mypy..."
mypy ./ems

echo "All linting completed successfully!"
