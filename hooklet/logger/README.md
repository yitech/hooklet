# Hooklet Logger

A comprehensive, configurable logging system designed for the Hooklet framework. This logger provides extensive customization options, async-safe operations, performance monitoring, and structured logging capabilities.

## Features

- **Multiple Log Levels**: Support for all standard logging levels (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- **Flexible Output Destinations**: Console, file, both, or syslog
- **Multiple Format Options**: Simple, detailed, JSON, or custom formats
- **File Rotation**: Automatic log file rotation with configurable size limits
- **Async-Safe**: Thread-safe and async-context aware logging
- **Performance Monitoring**: Built-in performance tracking with context managers and decorators
- **Structured Logging**: JSON format support with custom fields
- **Singleton Pattern**: Centralized logger configuration across the application
- **Dynamic Reconfiguration**: Change logger settings at runtime

## Quick Start

### Basic Usage

```python
from hooklet.logger import setup_default_logging, get_logger, LogLevel

# Set up default logging
setup_default_logging(level=LogLevel.INFO)

# Get a logger
logger = get_logger("my_module")

# Log messages
logger.info("Application started")
logger.warning("This is a warning")
logger.error("An error occurred")
```

### File Logging

```python
from hooklet.logger import HookletLoggerConfig, HookletLogger, LogLevel, LogFormat, LogDestination

# Configure file logging
config = HookletLoggerConfig(
    level=LogLevel.DEBUG,
    format_type=LogFormat.DETAILED,
    destination=LogDestination.FILE,
    log_file="logs/application.log",
    max_file_size=10 * 1024 * 1024,  # 10MB
    backup_count=5
)

# Initialize logger
hooklet_logger = HookletLogger(config)
logger = hooklet_logger.get_logger("app")

logger.info("This message goes to the file")
```

### JSON Logging

```python
from hooklet.logger import HookletLoggerConfig, HookletLogger, LogFormat

config = HookletLoggerConfig(
    format_type=LogFormat.JSON,
    extra_fields={"service": "my-service", "version": "1.0.0"}
)

hooklet_logger = HookletLogger(config)
logger = hooklet_logger.get_logger("json_example")

logger.info("JSON formatted message")

# Log with additional fields
hooklet_logger.log_with_extra(
    LogLevel.INFO.value,
    "User action performed",
    user_id=12345,
    action="login",
    ip_address="192.168.1.1"
)
```

## Configuration Options

### HookletLoggerConfig Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `level` | `LogLevel \| str \| int` | `LogLevel.INFO` | Minimum log level to capture |
| `format_type` | `LogFormat` | `LogFormat.DETAILED` | Log message format |
| `destination` | `LogDestination` | `LogDestination.CONSOLE` | Where to output logs |
| `log_file` | `str \| None` | `None` | Path to log file (required for file destinations) |
| `max_file_size` | `int` | `10MB` | Maximum log file size before rotation |
| `backup_count` | `int` | `5` | Number of backup files to keep |
| `enable_performance_logging` | `bool` | `False` | Enable performance monitoring |
| `custom_format` | `str \| None` | `None` | Custom format string (when format_type is CUSTOM) |
| `extra_fields` | `dict \| None` | `None` | Additional fields to include in all log messages |

### Log Levels

```python
from hooklet.logger import LogLevel

LogLevel.CRITICAL  # 50
LogLevel.ERROR     # 40
LogLevel.WARNING   # 30
LogLevel.INFO      # 20
LogLevel.DEBUG     # 10
LogLevel.NOTSET    # 0
```

### Log Formats

```python
from hooklet.logger import LogFormat

LogFormat.SIMPLE    # "LEVEL - MESSAGE"
LogFormat.DETAILED  # Full format with timestamp, module, function, line number
LogFormat.JSON      # Structured JSON format
LogFormat.CUSTOM    # User-defined format string
```

### Log Destinations

```python
from hooklet.logger import LogDestination

LogDestination.CONSOLE  # Output to console/stdout
LogDestination.FILE     # Output to file
LogDestination.BOTH     # Output to both console and file
LogDestination.SYSLOG   # Output to system log
```

## Performance Monitoring

### Context Manager

```python
from hooklet.logger import HookletLoggerConfig, HookletLogger

config = HookletLoggerConfig(enable_performance_logging=True)
hooklet_logger = HookletLogger(config)

# Monitor operation performance
with hooklet_logger.performance_context("database_query", table="users"):
    # Your code here
    result = perform_database_query()
```

### Decorator

```python
from hooklet.logger import log_performance

@log_performance("api_endpoint")
def handle_request():
    # Function code here
    return response

@log_performance("async_operation") 
async def async_task():
    # Async function code here
    await some_async_operation()
```

## Async Support

The logger is fully async-aware and thread-safe:

```python
import asyncio
from hooklet.logger import HookletLoggerConfig, HookletLogger

async def async_worker(worker_id):
    config = HookletLoggerConfig()
    hooklet_logger = HookletLogger(config)
    logger = hooklet_logger.get_logger("worker")
    
    logger.info(f"Worker {worker_id} starting")
    await asyncio.sleep(1)
    logger.info(f"Worker {worker_id} completed")

# Run multiple workers concurrently
async def main():
    tasks = [async_worker(i) for i in range(3)]
    await asyncio.gather(*tasks)

asyncio.run(main())
```

## Dynamic Reconfiguration

```python
from hooklet.logger import HookletLogger, HookletLoggerConfig, LogFormat

# Initial configuration
config1 = HookletLoggerConfig(format_type=LogFormat.SIMPLE)
hooklet_logger = HookletLogger(config1)

# Change configuration at runtime
config2 = HookletLoggerConfig(
    format_type=LogFormat.JSON,
    log_file="logs/updated.log",
    destination=LogDestination.BOTH
)
hooklet_logger.update_config(config2)
```

## Multiple Named Loggers

```python
from hooklet.logger import get_logger, setup_default_logging

# Set up default configuration
setup_default_logging()

# Create specialized loggers for different components
auth_logger = get_logger("auth")
db_logger = get_logger("database")
api_logger = get_logger("api")

auth_logger.info("User authenticated successfully")
db_logger.warning("Database connection pool at 80% capacity")
api_logger.error("API rate limit exceeded")
```

## Custom Handlers

```python
import logging
from hooklet.logger import HookletLogger

# Create custom handler
custom_handler = logging.handlers.SMTPHandler(
    mailhost='localhost',
    fromaddr='app@example.com',
    toaddrs=['admin@example.com'],
    subject='Application Error'
)
custom_handler.setLevel(logging.ERROR)

# Add to logger
hooklet_logger = HookletLogger()
hooklet_logger.add_handler(custom_handler)
```

## Best Practices

1. **Initialize Early**: Set up logging configuration early in your application lifecycle
2. **Use Named Loggers**: Create specific loggers for different modules/components
3. **Appropriate Log Levels**: Use DEBUG for detailed debugging, INFO for general flow, WARNING for potential issues, ERROR for errors, CRITICAL for severe problems
4. **Structured Logging**: Use JSON format and extra fields for machine-readable logs
5. **Performance Monitoring**: Enable performance logging for critical operations
6. **File Rotation**: Configure appropriate file sizes and backup counts for file logging

## Examples

See `examples.py` for comprehensive usage examples demonstrating all features.

To run the examples:

```python
from hooklet.logger.examples import run_all_examples
run_all_examples()
```

## Error Handling

The logger handles various error conditions gracefully:

- **Invalid Configuration**: Raises `ValueError` with descriptive messages
- **File Permission Issues**: Falls back to console logging if file is not writable
- **Missing Directories**: Automatically creates log directories
- **Async Context Errors**: Gracefully handles cases where no event loop is running

## Thread Safety

The logger is fully thread-safe and uses appropriate locking mechanisms to ensure safe concurrent access across multiple threads and async tasks.

## Integration with Hooklet Framework

The logger is designed to integrate seamlessly with the Hooklet framework's event-driven architecture and can be used across all framework components for consistent logging behavior. 