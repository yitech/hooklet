"""Example usage of the Hooklet logger.

This example demonstrates:
1. Basic logging setup
2. Different log formats
3. File output
4. Performance logging
"""

import time

from hooklet.logger.hooklet_logger import (
    LogFormat,
    LogLevel,
    get_logger,
    log_performance,
    setup_default_logging,
)


def example_basic_logging():
    """Basic logging example with default settings."""
    # Setup with default settings (INFO level, DETAILED format, stdout only)
    setup_default_logging()

    logger = get_logger()

    # Log messages at different levels
    logger.debug("Debug message - won't show with default INFO level")
    logger.info("Info message - this will show")
    logger.warning("Warning message with some details", extra={"user": "admin", "action": "login"})
    logger.error("Error message")
    logger.critical("Critical message")


def example_with_file_output():
    """Example with both console and file output."""
    # Setup logging with file output
    setup_default_logging(
        level=LogLevel.DEBUG,  # More verbose logging
        log_file="logs/app.log",  # Will create logs directory if it doesn't exist
        format_type=LogFormat.DETAILED,
    )

    logger = get_logger("file_example")

    logger.debug("This debug message will go to both console and file")
    logger.info("File logging is useful for debugging and audit trails")


def example_simple_format():
    """Example using the simple log format."""
    # Setup with simple format
    setup_default_logging(
        level=LogLevel.INFO,
        format_type=LogFormat.SIMPLE,
    )

    logger = get_logger("simple")

    logger.info("A clean, simple log message")
    logger.warning("Warning - Simple but effective")


def example_json_format():
    """Example using JSON format for structured logging."""
    # Setup with JSON format
    setup_default_logging(
        level=LogLevel.INFO,
        format_type=LogFormat.JSON,
    )

    logger = get_logger("json")

    # Log with extra fields for structured data
    logger.info(
        "User action completed",
        extra={"user_id": 123, "action": "checkout", "items": 5, "total": 99.99},
    )


def example_performance_logging():
    """Example using performance logging decorator."""
    # Setup logging with performance logging enabled
    setup_default_logging(
        level=LogLevel.INFO, format_type=LogFormat.DETAILED, enable_performance_logging=True
    )

    # Get the root logger
    logger = get_logger()
    logger.info("Starting performance logging example...")

    @log_performance("expensive_operation")
    def do_expensive_work():
        """Simulate some expensive work."""
        logger.info("Doing expensive work...")
        time.sleep(1)
        return "operation complete"

    # Run the operation and show the result
    result = do_expensive_work()
    logger.info(f"Operation completed with result: {result}")


def run_all_examples():
    """Run all logging examples."""
    # Reset logging configuration before each example
    print("\n=== Basic Logging Example ===")
    setup_default_logging()
    example_basic_logging()

    print("\n=== File Output Example ===")
    setup_default_logging(
        level=LogLevel.DEBUG,
        log_file="logs/app.log",
        format_type=LogFormat.DETAILED,
    )
    example_with_file_output()

    print("\n=== Simple Format Example ===")
    setup_default_logging(
        level=LogLevel.INFO,
        format_type=LogFormat.SIMPLE,
    )
    example_simple_format()

    print("\n=== JSON Format Example ===")
    setup_default_logging(
        level=LogLevel.INFO,
        format_type=LogFormat.JSON,
    )
    example_json_format()

    print("\n=== Performance Logging Example ===")
    example_performance_logging()


if __name__ == "__main__":
    run_all_examples()
