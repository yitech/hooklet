#!/usr/bin/env python3
# filepath: /home/yite/Projects/ems/scripts/create_oco_order.py

import argparse
import logging
import ccxt

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def parse_arguments():
    parser = argparse.ArgumentParser(description='Create OCO (One-Cancels-the-Other) order')
    parser.add_argument('--api-name', type=str, required=True, 
                        help='Name of the API to use')
    parser.add_argument('--exchange', type=str, required=True, choices=['binance'],
                        help='Exchange to place the order on (e.g., binance)')
    parser.add_argument('--stop-px', type=float, required=True,
                        help='Stop price for the OCO order')
    parser.add_argument('--profit', type=float, required=True,
                        help='Profit price for the OCO order')
    
    return parser.parse_args()

def main():
    args = parse_arguments()
    logger.info("Creating OCO order with:")
    logger.info(f"API Name: {args.api_name}")
    logger.info(f"Exchange: {args.exchange}")
    logger.info(f"Stop Price: {args.stop_px}")
    logger.info(f"Profit Price: {args.profit}")
    
    # TODO: Implement the actual OCO order creation logic here

if __name__ == "__main__":
    main()