# Header Validator System

The Header Validator system provides a comprehensive way to handle message headers with validation, forwarding, and processing capabilities.

## Overview

The header validator system consists of two main classes:
- `HeaderValidator`: Validates and processes headers with forwarding capabilities
- `HeaderBuilder`: Helper class for creating common header patterns

## Standard Headers

The system defines several standard header keys:

| Header Key | Description | Type | Default |
|------------|-------------|------|---------|
| `Hooklet-Forward-To` | Target topics for forwarding | String (comma-separated) | - |
| `Hooklet-Forward-Delay` | Delay before forwarding (seconds) | Float | 0 |
| `Hooklet-Forward-Retry` | Number of retry attempts | Integer | 0 |
| `Hooklet-Forward-Timeout` | Timeout for forwarding (seconds) | Float | - |
| `Hooklet-Processing-Mode` | Processing mode (sync/async) | String | "sync" |
| `Hooklet-Correlation-ID` | Correlation ID for tracing | String | - |
| `Hooklet-Trace-ID` | Trace ID for distributed tracing | String | - |
| `Hooklet-Validation-Rules` | Custom validation rules | Dict | - |

## Usage Examples

### Basic Forwarding

```python
from hooklet.utils import HeaderBuilder

# Forward to a single topic
headers = HeaderBuilder.forward_to("next.topic")

# Forward to multiple topics
headers = HeaderBuilder.forward_to(["topic1", "topic2", "topic3"])

# Forward with delay and retry
headers = HeaderBuilder.forward_to("next.topic", delay=1.0, retry=3, timeout=5.0)
```

### Async Processing

```python
# Enable async processing
headers = HeaderBuilder.async_processing()

# Combine with forwarding
headers = HeaderBuilder.forward_to("next.topic")
headers.update(HeaderBuilder.async_processing())
```

### Tracing and Correlation

```python
import uuid

correlation_id = str(uuid.uuid4())
trace_id = str(uuid.uuid4())

headers = HeaderBuilder.with_correlation(correlation_id, trace_id)
```

### Direct Header Validator Usage

```python
from hooklet.utils import HeaderValidator

async with HeaderValidator(headers, pilot) as validator:
    # Check if headers are valid
    if validator.is_valid():
        # Get forward targets
        targets = validator.get_forward_targets()
        
        # Process forwarding
        if validator.has_forward_headers():
            await validator.process_forwarding(data, pilot)
```

## Integration with Pilots

The header validator is automatically integrated with pilots. When you publish a message with headers, the pilot will:

1. Validate all headers
2. Process any forwarding logic
3. Handle retries and timeouts
4. Support async processing

```python
from hooklet.pilot import InprocPilot
from hooklet.utils import HeaderBuilder

pilot = InprocPilot()
await pilot.connect()

# Subscribe to topics
pilot.pubsub().subscribe("step1", step1_handler)
pilot.pubsub().subscribe("step2", step2_handler)

# Publish with forwarding headers
headers = HeaderBuilder.forward_to("step2", delay=0.5, retry=2)
data = {"message": "Hello", "id": "123"}

await pilot.pubsub().publish("step1", data, headers)
```

## Forwarding Behavior

### Synchronous Forwarding
- Waits for forwarding to complete before returning
- Good for ensuring message delivery
- Blocks until all targets receive the message

### Asynchronous Forwarding
- Queues forwarding tasks in background
- Returns immediately after processing
- Good for high-throughput scenarios

### Retry Logic
- Exponential backoff between retries
- Configurable retry count
- Timeout support for each attempt

### Multiple Targets
- Supports comma-separated target lists
- Processes all targets in parallel
- Fails if any target fails (unless async)

## Validation Rules

The header validator automatically validates:

- `Hooklet-Forward-To`: Must be non-empty string
- `Hooklet-Forward-Delay`: Must be non-negative number
- `Hooklet-Forward-Retry`: Must be non-negative integer
- `Hooklet-Forward-Timeout`: Must be positive number

Invalid headers will be logged and the message processing may be skipped.

## Error Handling

The system provides comprehensive error handling:

- Validation errors are collected and logged
- Forwarding failures trigger retry logic
- Timeout handling for long-running operations
- Graceful cleanup of pending tasks

## Best Practices

1. **Use HeaderBuilder**: Prefer using `HeaderBuilder` methods over manual header creation
2. **Validate Early**: Check `validator.is_valid()` before processing
3. **Handle Errors**: Always handle validation and forwarding errors
4. **Use Correlation IDs**: Include correlation IDs for tracing
5. **Choose Processing Mode**: Use async for high-throughput, sync for critical messages
6. **Set Timeouts**: Always set appropriate timeouts for external dependencies

## Example Workflow

```python
# 1. Create headers for a multi-step workflow
headers = HeaderBuilder.forward_to("step2,step3", delay=0.1, retry=1)
headers.update(HeaderBuilder.async_processing())
headers.update(HeaderBuilder.with_correlation(str(uuid.uuid4())))

# 2. Publish message
await pilot.pubsub().publish("step1", data, headers)

# 3. Message flows through the system:
#    step1 -> (delay) -> step2, step3 (parallel)
#    Each step can add its own forwarding headers
```

This system provides a powerful and flexible way to handle message routing, validation, and processing in your Hooklet applications. 