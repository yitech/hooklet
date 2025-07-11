from hooklet.logger import get_logger, HookletLogger, HookletLoggerConfig
import logging
import time

from hooklet.logger.node_logger import get_node_logger
config = HookletLoggerConfig(
    level=logging.DEBUG,
    log_file="logs/hooklet.log",
    rotation=True,
    max_backup=10
)

HookletLogger(config)

logger = get_node_logger("test")



for i in range(20):
    logger.info(f"Hello, world! {i}")
    logger.warning(f"This is a warning {i}")
    logger.error(f"This is an error {i}")
    logger.critical(f"This is a critical error {i}")
    logger.debug(f"This is a debug message {i}")
    time.sleep(5)