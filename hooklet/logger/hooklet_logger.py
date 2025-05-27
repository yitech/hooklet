"""
Hooklet Logger Module

A comprehensive logging system for the Hooklet framework that provides:
- Configurable log levels
- Multiple output destinations (console, file, or both)
- Customizable log formats
- JSON and standard text formatting
- Async-safe logging operations
- Performance metrics and structured logging
"""

import logging
import logging.handlers
import sys
import os
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Union, TextIO
from enum import Enum
import asyncio
import threading
from contextlib import contextmanager


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
    CUSTOM = "custom"


class LogDestination(Enum):
    """Enumeration for log output destinations."""
    CONSOLE = "console"
    FILE = "file"
    BOTH = "both"
    SYSLOG = "syslog"


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
        if hasattr(record, 'extra_fields'):
            log_entry.update(record.extra_fields)
            
        return json.dumps(log_entry, ensure_ascii=False)


class AsyncSafeFormatter(logging.Formatter):
    """Thread-safe formatter that includes async context information."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record with async context information."""
        # Add async context information
        try:
            loop = asyncio.get_running_loop()
            record.task_name = getattr(asyncio.current_task(loop), 'get_name', lambda: 'unknown')()
        except RuntimeError:
            record.task_name = 'no-loop'
            
        record.thread_name = threading.current_thread().name
        record.thread_id = threading.get_ident()
        
        return super().format(record)


class HookletLoggerConfig:
    """Configuration class for Hooklet Logger."""
    
    def __init__(
        self,
        level: Union[LogLevel, str, int] = LogLevel.INFO,
        format_type: LogFormat = LogFormat.DETAILED,
        destination: LogDestination = LogDestination.CONSOLE,
        log_file: Optional[str] = None,
        max_file_size: int = 10 * 1024 * 1024,  # 10MB
        backup_count: int = 5,
        enable_async_context: bool = True,
        enable_performance_logging: bool = False,
        custom_format: Optional[str] = None,
        extra_fields: Optional[Dict[str, Any]] = None,
        disable_existing_loggers: bool = False,
    ):
        """Initialize logger configuration.
        
        Args:
            level: Logging level (LogLevel enum, string, or int)
            format_type: Format type for log messages
            destination: Where to output logs
            log_file: Path to log file (required if destination includes file)
            max_file_size: Maximum size of log file before rotation
            backup_count: Number of backup files to keep
            enable_async_context: Include async context in logs
            enable_performance_logging: Enable performance metrics
            custom_format: Custom format string (used when format_type is CUSTOM)
            extra_fields: Additional fields to include in all log messages
            disable_existing_loggers: Whether to disable existing loggers
        """
        self.level = self._normalize_level(level)
        self.format_type = format_type
        self.destination = destination
        self.log_file = log_file
        self.max_file_size = max_file_size
        self.backup_count = backup_count
        self.enable_async_context = enable_async_context
        self.enable_performance_logging = enable_performance_logging
        self.custom_format = custom_format
        self.extra_fields = extra_fields or {}
        self.disable_existing_loggers = disable_existing_loggers
        
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
        if self.destination in (LogDestination.FILE, LogDestination.BOTH) and not self.log_file:
            raise ValueError("log_file must be specified when destination includes file output")
        
        if self.format_type == LogFormat.CUSTOM and not self.custom_format:
            raise ValueError("custom_format must be specified when format_type is CUSTOM")


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
        if hasattr(self, '_initialized'):
            return
            
        self.config = config or HookletLoggerConfig()
        self.logger = logging.getLogger('hooklet')
        self.performance_logger = logging.getLogger('hooklet.performance')
        self._setup_logger()
        self._initialized = True
    
    def _setup_logger(self):
        """Set up the logger with the specified configuration."""
        # Clear existing handlers
        self.logger.handlers.clear()
        self.performance_logger.handlers.clear()
        
        # Set log level
        self.logger.setLevel(self.config.level)
        self.performance_logger.setLevel(self.config.level)
        
        # Create formatter
        formatter = self._create_formatter()
        
        # Set up handlers based on destination
        if self.config.destination in (LogDestination.CONSOLE, LogDestination.BOTH):
            console_handler = self._create_console_handler(formatter)
            self.logger.addHandler(console_handler)
            self.performance_logger.addHandler(console_handler)
        
        if self.config.destination in (LogDestination.FILE, LogDestination.BOTH):
            file_handler = self._create_file_handler(formatter)
            self.logger.addHandler(file_handler)
            self.performance_logger.addHandler(file_handler)
        
        if self.config.destination == LogDestination.SYSLOG:
            syslog_handler = self._create_syslog_handler(formatter)
            self.logger.addHandler(syslog_handler)
            self.performance_logger.addHandler(syslog_handler)
        
        # Disable propagation to avoid duplicate logs
        self.logger.propagate = False
        self.performance_logger.propagate = False
    
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
            LogFormat.CUSTOM: self.config.custom_format,
        }
        
        if self.config.enable_async_context:
            formats[LogFormat.DETAILED] += " - [Task: %(task_name)s, Thread: %(thread_name)s]"
        
        format_string = formats[self.config.format_type]
        
        if self.config.enable_async_context:
            return AsyncSafeFormatter(format_string)
        else:
            return logging.Formatter(format_string)
    
    def _create_console_handler(self, formatter: logging.Formatter) -> logging.Handler:
        """Create console handler."""
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        return handler
    
    def _create_file_handler(self, formatter: logging.Formatter) -> logging.Handler:
        """Create rotating file handler."""
        # Ensure log directory exists
        log_path = Path(self.config.log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        handler = logging.handlers.RotatingFileHandler(
            self.config.log_file,
            maxBytes=self.config.max_file_size,
            backupCount=self.config.backup_count,
            encoding='utf-8'
        )
        handler.setFormatter(formatter)
        return handler
    
    def _create_syslog_handler(self, formatter: logging.Formatter) -> logging.Handler:
        """Create syslog handler."""
        handler = logging.handlers.SysLogHandler(address='/dev/log')
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
        return logging.getLogger(f'hooklet.{name}')
    
    def log_with_extra(self, level: int, message: str, **extra_fields):
        """Log message with extra fields.
        
        Args:
            level: Log level
            message: Log message
            **extra_fields: Additional fields to include
        """
        record = self.logger.makeRecord(
            self.logger.name, level, '', 0, message, (), None
        )
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
        self.performance_logger.info(
            f"Starting operation: {operation}",
            extra={'operation': operation, 'event': 'start', **extra_fields}
        )
        
        try:
            yield
            duration = (datetime.now() - start_time).total_seconds()
            self.performance_logger.info(
                f"Completed operation: {operation} in {duration:.3f}s",
                extra={'operation': operation, 'event': 'complete', 'duration': duration, **extra_fields}
            )
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.performance_logger.error(
                f"Failed operation: {operation} after {duration:.3f}s - {str(e)}",
                extra={'operation': operation, 'event': 'error', 'duration': duration, 'error': str(e), **extra_fields}
            )
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
    HookletLogger(config)


def setup_default_logging(
    level: Union[LogLevel, str, int] = LogLevel.INFO,
    log_file: Optional[str] = None,
    format_type: LogFormat = LogFormat.DETAILED,
    destination: LogDestination = LogDestination.CONSOLE
):
    """Set up default logging configuration.
    
    Args:
        level: Logging level
        log_file: Path to log file (optional)
        format_type: Format type for log messages
        destination: Where to output logs
    """
    config = HookletLoggerConfig(
        level=level,
        log_file=log_file,
        format_type=format_type,
        destination=destination
    )
    configure_logging(config)


# Performance logging decorator
def log_performance(operation: str = None, logger_name: str = None):
    """Decorator for automatic performance logging.
    
    Args:
        operation: Operation name (defaults to function name)
        logger_name: Logger name to use
    """
    def decorator(func):
        def sync_wrapper(*args, **kwargs):
            op_name = operation or func.__name__
            hooklet_logger = HookletLogger()
            with hooklet_logger.performance_context(op_name):
                return func(*args, **kwargs)
        
        async def async_wrapper(*args, **kwargs):
            op_name = operation or func.__name__
            hooklet_logger = HookletLogger()
            with hooklet_logger.performance_context(op_name):
                return await func(*args, **kwargs)
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator
