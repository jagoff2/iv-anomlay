"""
Options Data Feed using YFinance

This module provides a real-time options data feed using the yfinance library.
It fetches options data for a list of tickers and publishes it to a Kafka topic
for consumption by the anomaly detection system.

Usage:
    python yfinance_data_feed.py [--config CONFIG_PATH] [--debug]
"""

import os
import json
import time
import logging
import datetime
import argparse
import uuid
import threading
import queue
import traceback
from typing import Dict, List, Optional, Union, Any
from dataclasses import dataclass

import numpy as np
import pandas as pd
import yfinance as yf
from confluent_kafka import Producer, KafkaException

# Setup logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("yfinance_data_feed.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("yfinance_data_feed")

# Default configuration
DEFAULT_CONFIG = {
    "kafka": {
        "bootstrap_servers": "localhost:9092",
        "topic": "options_data",
        "batch_size": 100,
        "queue_buffer_size": 10000,
        "linger_ms": 100,
        "compression_type": "lz4"
    },
    "yfinance": {
        "tickers": ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "SPY", "QQQ", "IWM"],
        "fetch_interval": 60,  # seconds
        "max_expiry_days": 90,  # Only fetch options expiring within this many days
        "risk_free_rate": 0.05
    },
    "general": {
        "debug": False,
        "num_workers": 4
    }
}


class KafkaProducerWrapper:
    """Wrapper for Kafka producer with error handling"""
    
    def __init__(self, bootstrap_servers: str, topic: str, linger_ms: int = 100, 
                 compression_type: str = "lz4"):
        """
        Initialize the Kafka producer
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Topic to produce to
            linger_ms: Time to wait in ms for more messages before sending a batch
            compression_type: Compression type (none, gzip, snappy, lz4)
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': f'yfinance-data-feed-{uuid.uuid4()}',
            'linger.ms': linger_ms,
            'queue.buffering.max.messages': 100000,
            'queue.buffering.max.kbytes': 100000,
            'compression.type': compression_type,
            'batch.num.messages': 1000
        })
        
        logger.info(f"Kafka producer initialized for topic {topic}")
    
    def delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')
    
    def produce(self, key: str, value: Dict) -> None:
        """
        Produce a message to Kafka
        
        Args:
            key: Message key
            value: Message value (will be JSON serialized)
        """
        try:
            self.producer.produce(
                self.topic,
                key=key.encode('utf-8'),
                value=json.dumps(value).encode('utf-8'),
                callback=self.delivery_report
            )
            # Serve delivery callbacks from previous produce calls
            self.producer.poll(0)
        
        except BufferError:
            logger.warning('Local buffer full, waiting for free space...')
            self.producer.poll(0.5)
            self.produce(key, value)  # Retry
        
        except Exception as e:
            logger.error(f'Error producing message: {str(e)}')
    
    def flush(self) -> None:
        """Flush the producer"""
        self.producer.flush()
    
    def close(self) -> None:
        """Close the producer"""
        self.flush()


class YFinanceOptionsFetcher:
    """Fetch options data from Yahoo Finance using yfinance"""
    
    def __init__(self, risk_free_rate: float = 0.05, max_expiry_days: int = 90):
        """
        Initialize the options fetcher
        
        Args:
            risk_free_rate: Risk-free interest rate for option price calculations
            max_expiry_days: Maximum days to expiration to fetch
        """
        self.risk_free_rate = risk_free_rate
        self.max_expiry_days = max_expiry_days
        self.ticker_data = {}  # Cache for ticker objects
        self.ticker_cache_time = {}  # Track when each ticker was last fetched
        self.ticker_cache_duration = 300  # Cache tickers for 5 minutes
    
    def get_ticker(self, ticker_symbol: str) -> Optional[yf.Ticker]:
        """
        Get a ticker object, using cache if available
        
        Args:
            ticker_symbol: Ticker symbol (e.g., 'AAPL')
            
        Returns:
            Ticker object or None if fetching failed
        """
        current_time = time.time()
        
        # Check if ticker is in cache and cache is still valid
        if (ticker_symbol in self.ticker_data and 
            current_time - self.ticker_cache_time.get(ticker_symbol, 0) < self.ticker_cache_duration):
            return self.ticker_data[ticker_symbol]
        
        # Fetch new ticker data
        try:
            ticker = yf.Ticker(ticker_symbol)
            self.ticker_data[ticker_symbol] = ticker
            self.ticker_cache_time[ticker_symbol] = current_time
            return ticker
        except Exception as e:
            logger.error(f"Error fetching ticker {ticker_symbol}: {str(e)}")
            logger.debug(traceback.format_exc())
            return None
    
    def get_options_for_ticker(self, ticker_symbol: str) -> List[Dict]:
        """
        Fetch all options data for a ticker
        
        Args:
            ticker_symbol: Ticker symbol (e.g., 'AAPL')
            
        Returns:
            List of option data records
        """
        ticker = self.get_ticker(ticker_symbol)
        if not ticker:
            logger.warning(f"Could not fetch ticker {ticker_symbol}")
            return []
        
        # Get underlying price
        try:
            underlying_price = ticker.info.get('regularMarketPrice')
            if not underlying_price:
                underlying_price = ticker.history(period="1d")['Close'].iloc[-1]
            
            logger.debug(f"Underlying price for {ticker_symbol}: {underlying_price}")
        except Exception as e:
            logger.error(f"Error getting price for {ticker_symbol}: {str(e)}")
            logger.debug(traceback.format_exc())
            return []
        
        # Get expiration dates
        try:
            expiration_dates = ticker.options
            
            if not expiration_dates:
                logger.warning(f"No expiration dates found for {ticker_symbol}")
                return []
            
            logger.debug(f"Found {len(expiration_dates)} expiration dates for {ticker_symbol}")
        except Exception as e:
            logger.error(f"Error getting expiration dates for {ticker_symbol}: {str(e)}")
            logger.debug(traceback.format_exc())
            return []
        
        # Filter expiration dates based on max_expiry_days
        current_date = datetime.datetime.now().date()
        filtered_dates = []
        
        for date_str in expiration_dates:
            try:
                exp_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
                days_to_expiry = (exp_date - current_date).days
                
                if days_to_expiry <= self.max_expiry_days:
                    filtered_dates.append(date_str)
            except Exception as e:
                logger.warning(f"Could not parse expiration date {date_str}: {str(e)}")
        
        logger.debug(f"Filtered to {len(filtered_dates)} expiration dates within {self.max_expiry_days} days")
        
        # Get option chains for each expiration date
        all_options = []
        
        for exp_date in filtered_dates:
            try:
                option_chain = ticker.option_chain(exp_date)
                
                # Process calls
                for _, row in option_chain.calls.iterrows():
                    all_options.append(self._process_option_row(
                        ticker_symbol, underlying_price, exp_date, 'call', row
                    ))
                
                # Process puts
                for _, row in option_chain.puts.iterrows():
                    all_options.append(self._process_option_row(
                        ticker_symbol, underlying_price, exp_date, 'put', row
                    ))
                
            except Exception as e:
                logger.error(f"Error getting option chain for {ticker_symbol} ({exp_date}): {str(e)}")
                logger.debug(traceback.format_exc())
        
        # Remove None values (failed conversions)
        all_options = [opt for opt in all_options if opt is not None]
        logger.info(f"Fetched {len(all_options)} options for {ticker_symbol}")
        
        return all_options
    
# Fix for the NaN conversion error in the _process_option_row method

    def _process_option_row(self, ticker_symbol: str, underlying_price: float, 
                            exp_date: str, option_type: str, row: pd.Series) -> Optional[Dict]:
        """
        Process an option row from the option chain with improved error handling
        
        Args:
            ticker_symbol: Ticker symbol (e.g., 'AAPL')
            underlying_price: Current price of the underlying
            exp_date: Expiration date string ('YYYY-MM-DD')
            option_type: 'call' or 'put'
            row: Row from the option chain DataFrame
            
        Returns:
            Option data dict or None if processing failed
        """
        try:
            # Calculate days to expiration
            current_date = datetime.datetime.now().date()
            exp_datetime = datetime.datetime.strptime(exp_date, '%Y-%m-%d').date()
            days_to_expiry = (exp_datetime - current_date).days
            
            # Handle edge case where market closed but date is today
            if days_to_expiry < 0:
                days_to_expiry = 0
            
            # Time to expiration in years
            t_expiry = days_to_expiry / 365.0
            
            # Symbol
            contract_symbol = row.get('contractSymbol', f"{ticker_symbol}{exp_date.replace('-', '')}{option_type[0].upper()}{int(row['strike']*1000):08d}")
            
            # Handle NaN values properly
            volume = row.get('volume', 0)
            if pd.isna(volume):
                volume = 0
            else:
                volume = int(volume)
                
            open_interest = row.get('openInterest', 0)
            if pd.isna(open_interest):
                open_interest = 0
            else:
                open_interest = int(open_interest)
            
            # Ensure positive prices
            bid = float(row['bid']) if not pd.isna(row['bid']) else 0.0
            ask = float(row['ask']) if not pd.isna(row['ask']) else 0.0
            
            # Ensure bid <= ask
            if bid > ask and ask > 0:
                # Swap if reversed
                bid, ask = ask, bid
            
            # Make sure strike is sensible
            strike = float(row['strike'])
            if strike <= 0:
                logger.warning(f"Invalid strike price for {contract_symbol}: {strike}")
                return None
            
            # Make sure implied volatility is valid or provide a reasonable default
            if 'impliedVolatility' in row and not pd.isna(row['impliedVolatility']):
                # Use provided IV if available
                implied_volatility = float(row['impliedVolatility'])
                if implied_volatility <= 0 or implied_volatility > 10:  # IV over 1000% is suspect
                    # Override with reasonable default
                    moneyness = strike / underlying_price
                    implied_volatility = self._default_iv(option_type, moneyness, t_expiry)
            else:
                # Calculate a reasonable default IV
                moneyness = strike / underlying_price
                implied_volatility = self._default_iv(option_type, moneyness, t_expiry)
            
            # Delta if available
            delta = None
            if 'delta' in row and not pd.isna(row['delta']):
                delta = float(row['delta'])
                
            # Convert to our format
            option_data = {
                "timestamp": time.time(),
                "symbol": contract_symbol,
                "underlying": ticker_symbol,
                "option_type": option_type,
                "strike": strike,
                "expiration": t_expiry,
                "bid": bid,
                "ask": ask,
                "volume": volume,
                "open_interest": open_interest,
                "implied_volatility": implied_volatility,
                "underlying_price": underlying_price,
                "delta": delta
            }
            
            return option_data
        
        except Exception as e:
            logger.warning(f"Error processing option row: {str(e)}")
            logger.debug(f"Row data: {row}")
            logger.debug(traceback.format_exc())
            return None

    def _default_iv(self, option_type: str, moneyness: float, t_expiry: float) -> float:
        """Calculate a reasonable default IV based on option characteristics"""
        # Base IV starts at 30%
        base_iv = 0.3
        
        # Adjust for moneyness (further OTM = higher IV)
        if option_type == 'call':
            if moneyness < 0.85:  # ITM
                moneyness_adj = 0.8
            elif moneyness > 1.15:  # OTM
                moneyness_adj = 1.0 + 0.2 * (moneyness - 1.15)  # Higher IV for OTM
            else:  # Near the money
                moneyness_adj = 1.0
        else:  # Put
            if moneyness > 1.15:  # ITM
                moneyness_adj = 0.8
            elif moneyness < 0.85:  # OTM
                moneyness_adj = 1.0 + 0.2 * (0.85 - moneyness)  # Higher IV for OTM
            else:  # Near the money
                moneyness_adj = 1.0
        
        # Adjust for time to expiration (shorter time = higher IV)
        if t_expiry < 1/52:  # Less than a week
            time_adj = 1.5
        elif t_expiry < 1/12:  # Less than a month
            time_adj = 1.2
        elif t_expiry > 1.0:  # More than a year
            time_adj = 0.8
        else:
            time_adj = 1.0
        
        # Combine adjustments
        iv = base_iv * moneyness_adj * time_adj
        
        # Ensure IV is within reasonable bounds
        return max(0.05, min(iv, 2.0))

class YFinanceDataFeed:
    """
    Main class for fetching options data from Yahoo Finance and 
    publishing to Kafka for the anomaly detection system
    """
    
    def __init__(self, config: Dict):
        """
        Initialize the data feed
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.running = False
        
        # Initialize Kafka producer
        self.producer = KafkaProducerWrapper(
            bootstrap_servers=config["kafka"]["bootstrap_servers"],
            topic=config["kafka"]["topic"],
            linger_ms=config["kafka"]["linger_ms"],
            compression_type=config["kafka"]["compression_type"]
        )
        
        # Initialize options fetcher
        self.fetcher = YFinanceOptionsFetcher(
            risk_free_rate=config["yfinance"]["risk_free_rate"],
            max_expiry_days=config["yfinance"]["max_expiry_days"]
        )
        
        # Setup worker threads and queue
        self.num_workers = config["general"]["num_workers"]
        self.fetch_queue = queue.Queue(maxsize=config["kafka"]["queue_buffer_size"])
        self.workers = []
        
        # Set log level based on debug flag
        if config["general"]["debug"]:
            logger.setLevel(logging.DEBUG)
        
        logger.info("YFinance data feed initialized")
    
    def _worker_thread(self):
        """Worker thread for fetching options data"""
        while self.running:
            ticker = None
            try:
                # Get ticker from queue
                try:
                    ticker = self.fetch_queue.get(timeout=1)
                except queue.Empty:
                    # No tickers in queue, just continue
                    continue
                
                # Fetch options data
                options = self.fetcher.get_options_for_ticker(ticker)
                
                # Publish to Kafka
                for option in options:
                    self.producer.produce(
                        key=option["symbol"],
                        value=option
                    )
                
                # Track progress
                logger.info(f"Published {len(options)} options for {ticker} to Kafka")
                
                # Mark task as done
                self.fetch_queue.task_done()
                
            except Exception as e:
                logger.error(f"Error in worker thread: {str(e)}")
                logger.debug(traceback.format_exc())
                
                # Make sure to mark the task as done even if processing failed
                if ticker is not None:
                    try:
                        self.fetch_queue.task_done()
                    except ValueError:
                        # Task may already be marked as done
                        pass

    
    def _fetch_cycle(self):
        """Run a full fetch cycle for all tickers"""
        tickers = self.config["yfinance"]["tickers"]
        logger.info(f"Starting fetch cycle for {len(tickers)} tickers")
        
        # Add tickers to fetch queue
        for ticker in tickers:
            self.fetch_queue.put(ticker)
        
        # Wait for all fetches to complete
        self.fetch_queue.join()
        
        # Flush the producer
        self.producer.flush()
        logger.info("Fetch cycle completed")
    
    def _main_loop(self):
        """Main loop for the data feed"""
        while self.running:
            try:
                # Run a fetch cycle
                start_time = time.time()
                self._fetch_cycle()
                
                # Calculate time spent and sleep if needed
                elapsed = time.time() - start_time
                interval = self.config["yfinance"]["fetch_interval"]
                
                if elapsed < interval:
                    sleep_time = interval - elapsed
                    logger.debug(f"Sleeping for {sleep_time:.2f} seconds until next fetch cycle")
                    
                    # Sleep in small increments to check running flag
                    for _ in range(int(sleep_time)):
                        if not self.running:
                            break
                        time.sleep(1)
                    
                    # Sleep remaining fraction
                    if self.running and sleep_time % 1 > 0:
                        time.sleep(sleep_time % 1)
            
            except Exception as e:
                logger.error(f"Error in main loop: {str(e)}")
                logger.debug(traceback.format_exc())
                
                # Sleep a bit before retrying
                time.sleep(5)
    
    def start(self):
        """Start the data feed"""
        if self.running:
            logger.warning("Data feed already running")
            return
        
        self.running = True
        
        # Start worker threads
        self.workers = []
        for i in range(self.num_workers):
            worker = threading.Thread(
                target=self._worker_thread,
                daemon=True,
                name=f"worker-{i}"
            )
            self.workers.append(worker)
            worker.start()
        
        # Start main thread
        self.main_thread = threading.Thread(
            target=self._main_loop,
            daemon=True,
            name="main-loop"
        )
        self.main_thread.start()
        
        logger.info(f"YFinance data feed started with {self.num_workers} workers")
    
    def stop(self):
        """Stop the data feed"""
        if not self.running:
            logger.warning("Data feed not running")
            return
        
        logger.info("Stopping YFinance data feed")
        self.running = False
        
        # Wait for main thread to finish
        if hasattr(self, 'main_thread') and self.main_thread.is_alive():
            self.main_thread.join(timeout=10)
        
        # Wait for worker threads to finish
        for worker in self.workers:
            worker.join(timeout=5)
        
        # Close Kafka producer
        self.producer.close()
        
        logger.info("YFinance data feed stopped")


def load_config(config_path: Optional[str] = None) -> Dict:
    """
    Load configuration from file or use defaults
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Configuration dict
    """
    config = DEFAULT_CONFIG.copy()
    
    if config_path:
        try:
            with open(config_path, 'r') as f:
                file_config = json.load(f)
                
                # Update config with file values (support nested dicts)
                for section, section_config in file_config.items():
                    if section in config:
                        if isinstance(section_config, dict) and isinstance(config[section], dict):
                            config[section].update(section_config)
                        else:
                            config[section] = section_config
                    else:
                        config[section] = section_config
                
                logger.info(f"Loaded configuration from {config_path}")
        
        except Exception as e:
            logger.error(f"Error loading configuration from {config_path}: {str(e)}")
            logger.debug(traceback.format_exc())
    
    return config


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='YFinance Options Data Feed')
    
    parser.add_argument(
        '--config',
        type=str,
        help='Path to configuration file'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    parser.add_argument(
        '--tickers',
        type=str,
        help='Comma-separated list of ticker symbols to fetch (overrides config)'
    )
    
    return parser.parse_args()


def main():
    """Main entry point"""
    args = parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Update config based on command line arguments
    if args.debug:
        config["general"]["debug"] = True
        logger.setLevel(logging.DEBUG)
    
    if args.tickers:
        config["yfinance"]["tickers"] = [t.strip() for t in args.tickers.split(',')]
    
    # Create and start data feed
    data_feed = YFinanceDataFeed(config)
    
    try:
        data_feed.start()
        
        # Keep the main thread alive until interrupted
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    
    finally:
        data_feed.stop()


if __name__ == "__main__":
    main()
