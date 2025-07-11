#!/bin/bash
# Script to run all linters on the ./hooklet directory

# Remove set -e to allow all linters to run
# set -e  # Exit immediately if a command exits with a non-zero status

echo "Running black..."
if ! black ./hooklet; then
    echo "❌ black failed"
    black_exit_code=1
else
    echo "✅ black passed"
    black_exit_code=0
fi

echo "Running isort..."
if ! isort ./hooklet; then
    echo "❌ isort failed"
    isort_exit_code=1
else
    echo "✅ isort passed"
    isort_exit_code=0
fi

echo "Running flake8..."
if ! flake8 ./hooklet; then
    echo "❌ flake8 failed"
    flake8_exit_code=1
else
    echo "✅ flake8 passed"
    flake8_exit_code=0
fi

echo "Running pylint..."
if ! pylint ./hooklet; then
    echo "❌ pylint failed"
    pylint_exit_code=1
else
    echo "✅ pylint passed"
    pylint_exit_code=0
fi

echo "Running mypy..."
if ! mypy ./hooklet; then
    echo "❌ mypy failed"
    mypy_exit_code=1
else
    echo "✅ mypy passed"
    mypy_exit_code=0
fi

# Calculate overall exit code
overall_exit_code=$((black_exit_code + isort_exit_code + flake8_exit_code + pylint_exit_code + mypy_exit_code))

if [ $overall_exit_code -eq 0 ]; then
    echo "🎉 All linting completed successfully!"
else
    echo "⚠️  Some linters failed. Check the output above for details."
    echo "Exit codes: black=$black_exit_code, isort=$isort_exit_code, flake8=$flake8_exit_code, pylint=$pylint_exit_code, mypy=$mypy_exit_code"
fi

exit $overall_exit_code
