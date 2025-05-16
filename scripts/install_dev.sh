#!/bin/bash
# Install the package in development mode

set -e

echo "Installing natsio in development mode..."
pip install -e .

echo "Installation complete! You can now import natsio in your Python projects."
