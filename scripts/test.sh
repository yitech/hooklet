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
    clean)
        echo "Cleaning test caches..."
        find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
        find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
        find . -name "*.pyc" -delete 2>/dev/null || true
        echo "Test caches cleaned."
        ;;
    *)
        echo "Usage: $0 [all|unittest|integration|clean]"
        echo "  all: Run all tests"
        echo "  unittest: Run only unit tests (exclude integration tests)"
        echo "  integration: Run only integration tests"
        echo "  clean: Clean test caches (__pycache__, .pytest_cache, *.pyc)"
        exit 1
        ;;
esac 