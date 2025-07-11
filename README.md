# Hooklet
[![codecov](https://codecov.io/gh/yitech/hooklet/graph/badge.svg?token=3kf2NZNMKe)](https://codecov.io/gh/yitech/hooklet)


Hooklet is an event-driven framework that enables building distributed systems using message broker architecture. The v3 version centralize different data transmission models (pub/sub, req/reply) with respect to different underlying implementations. (e.g. NATS, ~~ZeroMQ~~, native Python asyncio)

## Architecture

### Core Components

1. **Pilot**
   - Abstract message broker layer
   - Predefined/Customized header support
   - Topic based data routing

2. **Node**
    - High level data transmission model base on Pilot

## Testing

Hooklet includes both unit tests and integration tests. Integration tests require a NATS server with JetStream support.

### Running Tests

```bash
# Run all tests (unit + integration, auto-starts NATS with Docker)
./scripts/test.sh all

# Run only unit tests
./scripts/test.sh unittest

# Run only integration tests (auto-starts NATS with Docker)
./scripts/test.sh integration

# Clean test caches
./scripts/test.sh clean
```

### Integration Test Requirements

Integration tests need a NATS server running on `localhost:4222` with JetStream enabled. The test script will automatically:

1. Check if NATS is already running
2. If not, start NATS using `docker-compose up -d nats`
3. Run the integration tests
4. Clean up the Docker container if it was started by the script

### Manual NATS Setup

If you prefer to manage NATS manually:

```bash
# Option 2: Using Docker directly
docker run -d --name nats-server -p 4222:4222 -p 8222:8222 nats:latest -js
```

### CI/CD

Integration tests run automatically in GitHub Actions with a hosted NATS instance for every push and pull request to the `main` branch.


