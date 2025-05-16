#!/bin/bash
# Upload the package to PyPI

set -e

# Ensure build and twine are installed
pip install --upgrade build twine

# Clean previous builds
echo "Cleaning previous builds..."
rm -rf dist build *.egg-info

# Build the package
echo "Building the package..."
python -m build

# Upload to PyPI
echo "Uploading to PyPI..."
twine upload dist/*

echo "Upload complete!"
