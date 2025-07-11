"""
Hooklet Logger Module

A simple logging system for the Hooklet framework that provides:
- Configurable log levels
- Multiple output formats (simple text, detailed text, or JSON)
- Always logs to stdout with optional file output
- Structured logging
"""

import json
import logging
import logging.handlers
import sys
import threading
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
        extra_fields = getattr(record, "extra_fields", None)
        if extra_fields:
            log_entry.update(extra_fields)

        return json.dumps(log_entry, ensure_ascii=False)


class HookletLoggerConfig:
    """Configuration class for Hooklet Logger."""

    def __init__(
        self,
        level: Union[LogLevel, str, int] = LogLevel.INFO,
        format_type: LogFormat = LogFormat.DETAILED,
        log_file: Optional[str] = None,
        rotation: bool = False,
        max_backup: int = 5,
        extra_fields: Optional[Dict[str, Any]] = None,
    ):
        """Initialize logger configuration.

        Args:
            level: Logging level (LogLevel enum, string, or int)
            format_type: Format type for log messages
            log_file: Optional path to log file for additional file output
            rotation: Enable log rotation at midnight
            max_backup: Maximum number of backup files to keep
            extra_fields: Additional fields to include in all log messages
        """
        self.level = self._normalize_level(level)
        self.format_type = format_type
        self.log_file = log_file
        self.rotation = rotation
        self.max_backup = max_backup
        self.extra_fields = extra_fields or {}

    def _normalize_level(self, level: Union[LogLevel, str, int]) -> int:
        """Normalize log level to integer."""
        if isinstance(level, LogLevel):
            return level.value
        if isinstance(level, str):
            return getattr(logging, level.upper(), logging.INFO)
        if isinstance(level, int):
            return level
        raise ValueError(f"Invalid log level: {level}")


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
        # If already initialized, update config if provided
        if hasattr(self, "_initialized"):
            if config is not None:
                self.update_config(config)
            return

        self.config = config or HookletLoggerConfig()
        self.logger = logging.getLogger("hooklet")
        # Store original rotation settings for child logger copying
        self._original_rotation_when = "midnight"
        self._original_rotation_interval = 1
        self._setup_logger()
        self._initialized = True

    def _setup_logger(self):
        """Set up the logger with the specified configuration."""
        # Clear existing handlers
        self.logger.handlers.clear()

        # Set log level
        self.logger.setLevel(self.config.level)

        # Create formatter
        formatter = self._create_formatter()

        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)

        # Add console handler
        self.logger.addHandler(console_handler)

        # Add file handler if log_file is specified
        if self.config.log_file:
            file_handler = self._create_file_handler(formatter)
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
        if self.config.log_file is None:
            raise ValueError("log_file cannot be None when creating file handler")

        log_path = Path(self.config.log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        if self.config.rotation:
            # Use TimedRotatingFileHandler for midnight rotation
            handler: logging.Handler = logging.handlers.TimedRotatingFileHandler(
                self.config.log_file,
                when="midnight",
                interval=1,
                backupCount=self.config.max_backup,
                encoding="utf-8",
            )

        else:
            # Use regular FileHandler for no rotation
            handler = logging.FileHandler(self.config.log_file, encoding="utf-8")

        handler.setFormatter(formatter)
        return handler

    def get_logger(self, name: str | None = None) -> logging.Logger:
        """Get a logger instance.

        Args:
            name: Logger name. If None, returns the root hooklet logger.

        Returns:
            Logger instance.
        """
        if name is None:
            return self.logger

        # Get the child logger
        child_logger = logging.getLogger(f"hooklet.{name}")

        # Ensure child logger inherits handlers from parent
        if not child_logger.handlers:
            # Copy handlers from the main logger
            for handler in self.logger.handlers:
                # Create a copy of the handler to avoid sharing the same instance
                if isinstance(handler, logging.StreamHandler) and not isinstance(
                    handler, logging.FileHandler
                ):
                    new_handler: logging.Handler = logging.StreamHandler(handler.stream)
                elif isinstance(handler, logging.handlers.TimedRotatingFileHandler):
                    # Use original configuration values to avoid double conversion
                    when = self._original_rotation_when
                    interval = self._original_rotation_interval
                    new_handler = logging.handlers.TimedRotatingFileHandler(
                        handler.baseFilename,
                        when=when,
                        interval=interval,
                        backupCount=handler.backupCount,
                        encoding=handler.encoding,
                    )
                elif isinstance(handler, logging.FileHandler):
                    new_handler = logging.FileHandler(
                        handler.baseFilename, encoding=handler.encoding
                    )
                else:
                    # For other handler types, try to copy them
                    new_handler = type(handler)()

                # Copy formatter and filters
                if handler.formatter:
                    new_handler.setFormatter(handler.formatter)
                for filter_obj in handler.filters:
                    new_handler.addFilter(filter_obj)

                child_logger.addHandler(new_handler)

            # Set the same level as parent
            child_logger.setLevel(self.logger.level)
            child_logger.propagate = False

        return child_logger

    def log_with_extra(self, level: int, message: str, **extra_fields):
        """Log message with extra fields.

        Args:
            level: Log level
            message: Log message
            **extra_fields: Additional fields to include
        """
        record = self.logger.makeRecord(self.logger.name, level, "", 0, message, (), exc_info=None)
        if extra_fields:
            setattr(record, "extra_fields", {**self.config.extra_fields, **extra_fields})
        else:
            setattr(record, "extra_fields", self.config.extra_fields)
        self.logger.handle(record)

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

    def remove_handler(self, handler: logging.Handler):
        """Remove a handler from the logger.

        Args:
            handler: Handler to remove
        """
        self.logger.removeHandler(handler)


# Convenience functions for easy logging
def get_logger(name: str | None = None) -> logging.Logger:
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
    rotation: bool = False,
    max_backup: int = 5,
):
    """Set up default logging configuration.

    Args:
        level: Logging level
        log_file: Optional path to log file
        format_type: Format type for log messages
        rotation: Enable log rotation at midnight
        max_backup: Maximum number of backup files to keep
    """
    config = HookletLoggerConfig(
        level=level,
        log_file=log_file,
        format_type=format_type,
        rotation=rotation,
        max_backup=max_backup,
    )
    configure_logging(config)
