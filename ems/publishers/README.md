# EMS Publishers

This package contains publishers for the EMS (Exchange Management System) that can be used to publish various types of data to NATS topics.

## Base Publishers

The base publisher classes are defined in `ems.publisher`:

### Publisher (Abstract Base Class)

The core publisher that all other publishers inherit from. It provides common functionality like:
- Connection management
- Start/stop methods
- Publish method

### IntervalPublisher (Abstract Class)

Publisher that publishes data at regular intervals. Developers should implement the `get_data()` method to provide data to publish.

### StreamPublisher (Abstract Class)

Publisher that connects to an external data stream and publishes data from it. Developers should implement:
- `connect_to_stream()` - Establish connection to the data source
- `get_stream_data()` - Get data from the stream
- `disconnect_from_stream()` - Clean up when done

## Market Publishers

The `ems.publishers.market` module provides publishers for market data:

### MarketPricePublisher

Publishes simulated price data for a trading symbol.

```python
from ems import NatsManager
from ems.publishers import MarketPricePublisher

# Create a price publisher
publisher = MarketPricePublisher(
    symbol="BTC/USDT",
    initial_price=30000.0,
    volatility=50.0,
    interval_seconds=1.0
)

# Start publishing
await publisher.start()
```

### OrderbookPublisher

Publishes simulated orderbook data for a trading symbol.

```python
from ems.publishers import OrderbookPublisher

# Create an orderbook publisher
publisher = OrderbookPublisher(
    symbol="BTC/USDT",
    depth=10,       # Number of price levels
    base_price=30000.0,
    spread=5.0      # Spread between best bid and ask
)

# Start publishing
await publisher.start()
```

### TradePublisher

Publishes simulated trade data at random intervals.

## Binance Publishers

The `ems.publishers.binance` module provides publishers for Binance API:

### BinanceUserDataPublisher

Connects to Binance user data stream (account updates, order updates).

```python
from binance import AsyncClient
from ems.publishers import BinanceUserDataPublisher

# Create Binance client
client = await AsyncClient.create(API_KEY, API_SECRET)

# Create user data publisher
publisher = BinanceUserDataPublisher(
    client=client,
    subject="ems.orders"
)

# Start publishing
await publisher.start()
```

### BinanceMarketStreamPublisher

Connects to various Binance market data streams (trades, klines, orderbook, etc.).

```python
from binance import AsyncClient
from ems.publishers import BinanceMarketStreamPublisher

# Create Binance client
client = await AsyncClient.create(API_KEY, API_SECRET)

# Create market stream publisher
publisher = BinanceMarketStreamPublisher(
    client=client,
    symbol="BTCUSDT",
    stream_type="trade"  # "trade", "kline", "depth", "ticker"
)

# Start publishing
await publisher.start()
```

## External API Publishers

The `ems.publishers.external` module provides publishers for external APIs:

### RestApiPublisher

Makes periodic REST API calls and publishes the responses.

```python
from ems.publishers import RestApiPublisher

# Create REST API publisher
publisher = RestApiPublisher(
    endpoint="https://api.example.com/data",
    subject="external.api.data",
    interval_seconds=60.0,
    auth_headers={"Authorization": "Bearer token123"}
)

# Start publishing
await publisher.start()
```

### WebsocketPublisher

Connects to a websocket endpoint and publishes received messages.

```python
from ems.publishers import WebsocketPublisher

# Create websocket publisher
publisher = WebsocketPublisher(
    endpoint="wss://ws.example.com/stream",
    subject="external.ws.data",
    subscription_message={"type": "subscribe", "channels": ["ticker"]}
)

# Start publishing
await publisher.start()
```

## Advanced Publishers

The `ems.publishers.advanced` module provides more sophisticated publishers:

### FilterPublisher

Filters data from a source publisher before republishing.

```python
from ems.publishers import MarketPricePublisher, FilterPublisher

# Create source publisher
price_publisher = MarketPricePublisher(symbol="BTC/USDT")

# Create filter publisher (only publishes when price > 30000)
filter_publisher = FilterPublisher(
    source_publisher=price_publisher,
    filter_func=lambda data: data["price"] > 30000,
    subject="market.price.high.btc-usdt"
)

# Start the filter publisher (it will start the source publisher too)
await filter_publisher.start()
```

### TransformPublisher

Transforms data from a source publisher before republishing.

```python
from ems.publishers import MarketPricePublisher, TransformPublisher

# Create source publisher
price_publisher = MarketPricePublisher(symbol="BTC/USDT")

# Create transform publisher (adds timestamp and formatted price)
transform_publisher = TransformPublisher(
    source_publisher=price_publisher,
    transform_func=lambda data: {
        **data,
        "timestamp": time.time(),
        "price_formatted": f"${data['price']:,.2f}"
    }
)

# Start the transform publisher
await transform_publisher.start()
```

### MergePublisher

Combines data from multiple publishers into a single data stream.

```python
from ems.publishers import MarketPricePublisher, MergePublisher

# Create source publishers
btc_publisher = MarketPricePublisher(symbol="BTC/USDT")
eth_publisher = MarketPricePublisher(symbol="ETH/USDT")

# Create merge publisher (combines latest prices)
merge_publisher = MergePublisher(
    publishers=[btc_publisher, eth_publisher],
    subject="market.prices.combined",
    interval_seconds=1.0
)

# Start the merge publisher
await merge_publisher.start()
```

### BroadcastPublisher

Broadcasts the same data to multiple subjects.

```python
from ems.publishers import MarketPricePublisher, BroadcastPublisher

# Create source publisher
price_publisher = MarketPricePublisher(symbol="BTC/USDT")

# Create broadcast publisher
broadcast_publisher = BroadcastPublisher(
    source_publisher=price_publisher,
    subjects=[
        "market.price.btc-usdt",
        "analytics.price.btc",
        "ui.price.btc"
    ]
)

# Start the broadcast publisher
await broadcast_publisher.start()
```

## Creating Custom Publishers

You can create your own publishers by inheriting from the base classes. See the examples in `scripts/custom_publisher_implementations.py`.
