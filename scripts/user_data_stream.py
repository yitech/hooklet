import asyncio
from binance import AsyncClient, BinanceSocketManager
import logging
from ems import NatsManager
from ems.config import ConfigManager

logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load configuration from config.yml
config = ConfigManager().load()
account = config.get_account("SCYLLA")  # Using the SCYLLA account from config.yml
API_KEY = account.api_key
API_SECRET = account.api_secret
SUBJECT = "ems.orders"

async def main():
    client = await AsyncClient.create(API_KEY, API_SECRET, testnet=False)
    nats_manager = NatsManager()
    await nats_manager.connect()
    try:
        # Get the Futures listenKey (user data stream)
        listen_key = await client.futures_stream_get_listen_key()
        logger.info(f"Obtained futures listen key: {listen_key[:5]}...")

        # Use socket manager for Futures
        bsm = BinanceSocketManager(client)

        # Get the user data socket for Futures
        socket = bsm.futures_user_socket()

        logger.info("Listening to Futures user data stream...")
        async with socket as stream:
            while True:
                msg = await stream.recv()
                # logger.info(f"User Event: {msg}")
                await nats_manager.publish(SUBJECT, msg)
                logger.info(f"Published: {msg} to {SUBJECT}")
    except Exception as e:
        logger.error(f"Error occurred: {e}")
    finally:
        await client.close_connection()
        await nats_manager.close()
        logger.info("Connection closed")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
