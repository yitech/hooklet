#!/usr/bin/env bash

# Activate virtual environment if it exists
if [ -d "./venv" ]; then
    source ./venv/bin/activate
fi

MODE=${1:-all}

case "$MODE" in
    all)
        python -m pytest -v
        ;;
    unittest)
        python -m pytest -v -m "not integration"
        ;;
    integration)
        python -m pytest -v -m "integration"
        ;;
    *)
        echo "Usage: $0 [all|unittest|integration]"
        echo "  all: Run all tests"
        echo "  unittest: Run only unit tests (exclude integration tests)"
        echo "  integration: Run only integration tests"
        exit 1
        ;;
esac 