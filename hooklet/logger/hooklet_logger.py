"""
Hooklet Logger Module

A simple logging system for the Hooklet framework that provides:
- Configurable log levels
- Multiple output formats (simple text, detailed text, or JSON)
- Always logs to stdout with optional file output
- Performance metrics and structured logging
"""

import asyncio
import json
import logging
import logging.handlers
import sys
import threading
from contextlib import contextmanager
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, Union


class LogLevel(Enum):
    """Enumeration for log levels."""

    CRITICAL = logging.CRITICAL
    ERROR = logging.ERROR
    WARNING = logging.WARNING
    INFO = logging.INFO
    DEBUG = logging.DEBUG
    NOTSET = logging.NOTSET


class LogFormat(Enum):
    """Enumeration for log output formats."""

    SIMPLE = "simple"
    DETAILED = "detailed"
    JSON = "json"


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add exception information if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Add extra fields if present
        if hasattr(record, "extra_fields"):
            log_entry.update(record.extra_fields)

        return json.dumps(log_entry, ensure_ascii=False)


class PerformanceFormatter(logging.Formatter):
    """Formatter for performance logging messages."""

    def __init__(self, base_formatter: logging.Formatter):
        """Initialize with a base formatter for standard log formatting.

        Args:
            base_formatter: The base formatter to use for standard log fields
        """
        super().__init__()
        self.base_formatter = base_formatter

    def format(self, record: logging.LogRecord) -> str:
        """Format performance log record."""
        # Create performance-specific message
        if getattr(record, "event", None) == "start":
            perf_message = f"⏱️  Starting operation: {record.operation}"
        elif getattr(record, "event", None) == "complete":
            perf_message = (
                f"✅ Completed operation: {record.operation} (took {record.duration:.3f}s)"
            )
        elif getattr(record, "event", None) == "error":
            perf_message = (
                f"❌ Failed operation: {record.operation} "
                f"(failed after {record.duration:.3f}s) - {record.error}"
            )
        else:
            # Fallback to base formatter for non-performance logs
            return self.base_formatter.format(record)

        # Replace the message with our performance message
        original_message = record.getMessage()
        record.msg = perf_message
        record.args = ()

        # Format using the base formatter
        formatted = self.base_formatter.format(record)

        # Restore original message
        record.msg = original_message

        return formatted


class HookletLoggerConfig:
    """Configuration class for Hooklet Logger."""

    def __init__(
        self,
        level: Union[LogLevel, str, int] = LogLevel.INFO,
        format_type: LogFormat = LogFormat.DETAILED,
        log_file: Optional[str] = None,
        enable_async_context: bool = True,
        enable_performance_logging: bool = False,
        extra_fields: Optional[Dict[str, Any]] = None,
    ):
        """Initialize logger configuration.

        Args:
            level: Logging level (LogLevel enum, string, or int)
            format_type: Format type for log messages
            log_file: Optional path to log file for additional file output
            enable_async_context: Include async context in logs
            enable_performance_logging: Enable performance metrics
            extra_fields: Additional fields to include in all log messages
        """
        self.level = self._normalize_level(level)
        self.format_type = format_type
        self.log_file = log_file
        self.enable_async_context = enable_async_context
        self.enable_performance_logging = enable_performance_logging
        self.extra_fields = extra_fields or {}

        # Validate configuration
        self._validate()

    def _normalize_level(self, level: Union[LogLevel, str, int]) -> int:
        """Normalize log level to integer."""
        if isinstance(level, LogLevel):
            return level.value
        elif isinstance(level, str):
            return getattr(logging, level.upper())
        elif isinstance(level, int):
            return level
        else:
            raise ValueError(f"Invalid log level: {level}")

    def _validate(self):
        """Validate configuration parameters."""
        if self.log_file:
            log_path = Path(self.log_file)
            if not log_path.parent.exists():
                raise ValueError(f"Log directory does not exist: {log_path.parent}")


class HookletLogger:
    """Main logger class for the Hooklet framework."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        """Singleton pattern implementation."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config: Optional[HookletLoggerConfig] = None):
        """Initialize the Hooklet logger.

        Args:
            config: Logger configuration. If None, uses default configuration.
        """
        # Prevent re-initialization
        if hasattr(self, "_initialized"):
            return

        self.config = config or HookletLoggerConfig()
        self.logger = logging.getLogger("hooklet")
        self.performance_logger = logging.getLogger("hooklet.performance")
        self._setup_logger()
        self._initialized = True

    def _setup_logger(self):
        """Set up the logger with the specified configuration."""
        # Clear existing handlers
        self.logger.handlers.clear()

        # Set log level
        self.logger.setLevel(self.config.level)

        # Create formatters
        standard_formatter = self._create_formatter()
        performance_formatter = PerformanceFormatter(standard_formatter)

        # Create handlers
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(standard_formatter)

        perf_handler = logging.StreamHandler(sys.stdout)
        perf_handler.setFormatter(performance_formatter)

        # Set filters to separate performance logs
        def is_performance_log(record):
            return hasattr(record, "event")

        def is_standard_log(record):
            return not hasattr(record, "event")

        console_handler.addFilter(is_standard_log)
        perf_handler.addFilter(is_performance_log)

        # Add handlers
        self.logger.addHandler(console_handler)
        self.logger.addHandler(perf_handler)

        # Add file handler if log_file is specified
        if self.config.log_file:
            file_handler = self._create_file_handler(standard_formatter)
            self.logger.addHandler(file_handler)

        # Disable propagation to avoid duplicate logs
        self.logger.propagate = False

    def _create_formatter(self) -> logging.Formatter:
        """Create log formatter based on configuration."""
        if self.config.format_type == LogFormat.JSON:
            return JSONFormatter()

        # Define format strings
        formats = {
            LogFormat.SIMPLE: "%(levelname)s - %(message)s",
            LogFormat.DETAILED: (
                "%(asctime)s - %(name)s - %(levelname)s - "
                "%(module)s:%(funcName)s:%(lineno)d - %(message)s"
            ),
        }

        format_string = formats[self.config.format_type]
        return logging.Formatter(format_string)

    def _create_console_handler(self, formatter: logging.Formatter) -> logging.Handler:
        """Create console handler."""
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        return handler

    def _create_file_handler(self, formatter: logging.Formatter) -> logging.Handler:
        """Create file handler."""
        # Ensure log directory exists
        log_path = Path(self.config.log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        handler = logging.FileHandler(self.config.log_file, encoding="utf-8")
        handler.setFormatter(formatter)
        return handler

    def get_logger(self, name: str = None) -> logging.Logger:
        """Get a logger instance.

        Args:
            name: Logger name. If None, returns the root hooklet logger.

        Returns:
            Logger instance.
        """
        if name is None:
            return self.logger
        return logging.getLogger(f"hooklet.{name}")

    def log_with_extra(self, level: int, message: str, **extra_fields):
        """Log message with extra fields.

        Args:
            level: Log level
            message: Log message
            **extra_fields: Additional fields to include
        """
        record = self.logger.makeRecord(self.logger.name, level, "", 0, message, (), None)
        if extra_fields:
            record.extra_fields = {**self.config.extra_fields, **extra_fields}
        else:
            record.extra_fields = self.config.extra_fields
        self.logger.handle(record)

    @contextmanager
    def performance_context(self, operation: str, **extra_fields):
        """Context manager for performance logging.

        Args:
            operation: Name of the operation being measured
            **extra_fields: Additional fields to log
        """
        if not self.config.enable_performance_logging:
            yield
            return

        start_time = datetime.now()

        # Create a record for start event
        start_record = self.logger.makeRecord(
            self.logger.name, logging.INFO, "performance", 0, "Performance measurement", (), None
        )
        start_record.funcName = "performance_context"
        start_record.operation = operation
        start_record.event = "start"
        for key, value in extra_fields.items():
            setattr(start_record, key, value)
        self.logger.handle(start_record)

        try:
            yield
            duration = (datetime.now() - start_time).total_seconds()

            # Create a record for completion event
            complete_record = self.logger.makeRecord(
                self.logger.name,
                logging.INFO,
                "performance",
                0,
                "Performance measurement",
                (),
                None,
            )
            complete_record.funcName = "performance_context"
            complete_record.operation = operation
            complete_record.event = "complete"
            complete_record.duration = duration
            for key, value in extra_fields.items():
                setattr(complete_record, key, value)
            self.logger.handle(complete_record)

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()

            # Create a record for error event
            error_record = self.logger.makeRecord(
                self.logger.name,
                logging.ERROR,
                "performance",
                0,
                "Performance measurement",
                (),
                None,
            )
            error_record.funcName = "performance_context"
            error_record.operation = operation
            error_record.event = "error"
            error_record.duration = duration
            error_record.error = str(e)
            for key, value in extra_fields.items():
                setattr(error_record, key, value)
            self.logger.handle(error_record)
            raise

    def update_config(self, new_config: HookletLoggerConfig):
        """Update logger configuration.

        Args:
            new_config: New configuration to apply
        """
        self.config = new_config
        self._setup_logger()

    def add_handler(self, handler: logging.Handler):
        """Add a custom handler to the logger.

        Args:
            handler: Custom logging handler
        """
        self.logger.addHandler(handler)
        self.performance_logger.addHandler(handler)

    def remove_handler(self, handler: logging.Handler):
        """Remove a handler from the logger.

        Args:
            handler: Handler to remove
        """
        self.logger.removeHandler(handler)
        self.performance_logger.removeHandler(handler)


# Convenience functions for easy logging
def get_logger(name: str = None) -> logging.Logger:
    """Get a logger instance.

    Args:
        name: Logger name. If None, returns the root hooklet logger.

    Returns:
        Logger instance.
    """
    return HookletLogger().get_logger(name)


def configure_logging(config: HookletLoggerConfig):
    """Configure the Hooklet logging system.

    Args:
        config: Logger configuration
    """
    # Get or create the logger instance
    logger = HookletLogger()
    # Update configuration
    logger.update_config(config)


def setup_default_logging(
    level: Union[LogLevel, str, int] = LogLevel.INFO,
    log_file: Optional[str] = None,
    format_type: LogFormat = LogFormat.DETAILED,
    enable_performance_logging: bool = False,
):
    """Set up default logging configuration.

    Args:
        level: Logging level
        log_file: Optional path to log file
        format_type: Format type for log messages
        enable_performance_logging: Whether to enable performance logging
    """
    config = HookletLoggerConfig(
        level=level,
        log_file=log_file,
        format_type=format_type,
        enable_performance_logging=enable_performance_logging,
    )
    configure_logging(config)


# Performance logging decorator
def log_performance(operation: str = None):
    """Decorator for automatic performance logging.

    Args:
        operation: Operation name (defaults to function name)
    """

    def decorator(func):
        def sync_wrapper(*args, **kwargs):
            op_name = operation or func.__name__
            # Get the singleton instance that was configured
            logger = HookletLogger()._instance
            with logger.performance_context(op_name):
                return func(*args, **kwargs)

        async def async_wrapper(*args, **kwargs):
            op_name = operation or func.__name__
            # Get the singleton instance that was configured
            logger = HookletLogger()._instance
            with logger.performance_context(op_name):
                return await func(*args, **kwargs)

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator
