# Hooklet v3

Hooklet is an event-driven framework that enables building distributed systems using message broker architecture. The v3 version centralize different data transmission models (pub/sub, req/reply) with respect to different underlying implementations. (e.g. NATS, ZeroMQ, native Python asyncio)

## Architecture

### Core Components

1. **Pilot**
   - Abstract message broker layer
   - Predefined/Customized header support
   - Topic based data routing

2. **Node**
    - High level data transmission model base on Pilot


