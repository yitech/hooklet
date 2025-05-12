# Crypto Algorithmic Trading Framework

An asynchronous, event-driven Python framework for developing cryptocurrency trading strategies. This framework leverages Python's async/await features and NATS messaging system to create a flexible, extensible platform for algorithmic trading.

## Overview

This framework provides a foundation for building trading strategies with these key features:

- **Event-driven architecture** using NATS pub/sub messaging
- **Asynchronous design** for high performance
- **Modular components** for strategy development, execution, and market data
- **Extensible** to support multiple exchanges and asset types

## System Architecture

The system is built around several core components:

- **Strategy Module**: Base class for implementing trading algorithms
- **NATS Manager**: Handles communication between components
- **Configuration Manager**: Manages exchange credentials and system settings

## NATS Topic Structure

The framework uses a hierarchical topic naming convention:

```
<service>.<exchange>.<event>.<attribute1>.<attribute2>...
```

Examples:
- `mms.binancefuture.orderbook.btc.usdt.perp.5` - Binance perpetual BTCUSDT 5-level orderbook
- `pms.binance.orderupdate.<account>` - New order requests

## Getting Started

### Prerequisites

- Python 3.10+
- NATS server
- Required packages (see `requirements.txt`)

### Installation

1. Clone the repository
```bash
git clone https://github.com/yourusername/ems.git
cd ems
```

2. Install requirements
```bash
pip install -r requirements.txt
```

3. Create your configuration file
```bash
cp config.example.yml config.yml
# Edit config.yml with your exchange API credentials
```

4. Start a NATS server (using Docker)
```bash
docker-compose up -d
```

### Running a Sample Strategy

```bash
# Start the market data simulator in one terminal
python scripts/market_data_simulator.py

# Start the sample strategy in another terminal
python scripts/strategy_example.py
```

## Creating Your Own Strategy

Implement your custom strategy by extending the `Strategy` base class:

```python
from ems import NatsManager
from ems.strategy import Strategy

class MyCustomStrategy(Strategy):
    def __init__(self, nats_manager, symbol):
        super().__init__(nats_manager, strategy_id=f"custom_{symbol}")
        self.symbol = symbol
        
    def get_handlers(self):
        return {
            f"market.price.{self.symbol}": self.handle_price_update,
            "ems.orders.filled": self.handle_order_filled
        }
        
    async def handle_price_update(self, data):
        # Implement your strategy logic here
        pass
        
    async def handle_order_filled(self, data):
        # Handle order fills
        pass
```

## Configuration

Exchange credentials and other settings are stored in `config.yml`:

```yaml
accounts:
  - name: "binance-main"
    exchange: "binance"
    api_key: "your_api_key"
    api_secret: "your_api_secret"
```

## Key Components

### Strategy Base Class

The [`Strategy`](ems/strategy.py) class provides the foundation for building trading strategies with event handling capabilities.

### NATS Manager

The [`NatsManager`](ems/nats_manager.py) handles communication between system components using the NATS messaging system.

### Configuration

The [`ConfigManager`](ems/config.py) provides access to exchange credentials and other configuration settings.

## Examples

Several example scripts are provided to demonstrate how to use the framework:

- [`strategy_example.py`](scripts/strategy_example.py): A simple moving average strategy
- [`market_data_simulator.py`](scripts/market_data_simulator.py): Simulates market data for testing
- [`nats_manager_example.py`](scripts/nats_manager_example.py): Demonstrates NATS messaging

## Testing

Run the test suite:

```bash
pytest
```

## Project Structure

```
ems/
├── __init__.py         # Package initialization
├── config.py           # Configuration management
├── nats_manager.py     # NATS communication
├── strategy.py         # Base strategy class
├── execution/          # Order execution functionality
│   └── create_oco_order.py
└── asset/              # Asset management
scripts/
├── market_data_simulator.py  # Simulates market data
├── nats_example.py           # Basic NATS usage
├── nats_manager_example.py   # NATS manager examples
└── strategy_example.py       # Example trading strategy
tests/
├── __init__.py
├── test_config.py      # Config tests
└── test_nats_manager.py # NATS manager tests
```

## License

MIT License
