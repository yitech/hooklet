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
        python -m pytest -v tests/ --ignore=tests/integration
        ;;
    integration)
        if [ -d "tests/integration" ]; then
            python -m pytest -v tests/integration
        else
            echo "[WARN] No integration tests directory found. Skipping."
            exit 0
        fi
        ;;
    *)
        echo "Usage: $0 [all|unittest|integration]"
        exit 1
        ;;
esac 