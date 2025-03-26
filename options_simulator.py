"""
Options Data Simulator

This script generates simulated options data to test the anomaly detection system.
It produces both normal and anomalous options data patterns.

Usage:
    python options_simulator.py --kafka_server localhost:9092 --topic options_data --rate 10
"""

import argparse
import json
import random
import time
import uuid
import datetime
import numpy as np
import logging
from typing import Dict, List, Tuple, Optional, Union
from dataclasses import dataclass
from confluent_kafka import Producer
import math
# Setup logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("options_simulator")

# Constants
DEFAULT_UNDERLYINGS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "SPY", "QQQ", "IWM"]
RISK_FREE_RATE = 0.05  # 5% annual rate


class BlackScholesMerton:
    """Black-Scholes-Merton option pricing model implementation"""
    
    @staticmethod
    def d1(S: float, K: float, T: float, r: float, sigma: float, q: float = 0.0) -> float:
        """Calculate d1 component of Black-Scholes formula with dividend yield"""
        return (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    
    @staticmethod
    def d2(S: float, K: float, T: float, r: float, sigma: float, q: float = 0.0) -> float:
        """Calculate d2 component of Black-Scholes formula with dividend yield"""
        return BlackScholesMerton.d1(S, K, T, r, sigma, q) - sigma * np.sqrt(T)
    
    @staticmethod
    def call_price(S: float, K: float, T: float, r: float, sigma: float, q: float = 0.0) -> float:
        """Calculate the price of a European call option with dividend yield"""
        if T <= 0 or sigma <= 0:
            return max(0, S - K)
        
        d1 = BlackScholesMerton.d1(S, K, T, r, sigma, q)
        d2 = BlackScholesMerton.d2(S, K, T, r, sigma, q)
        
        return S * np.exp(-q * T) * norm_cdf(d1) - K * np.exp(-r * T) * norm_cdf(d2)
    
    @staticmethod
    def put_price(S: float, K: float, T: float, r: float, sigma: float, q: float = 0.0) -> float:
        """Calculate the price of a European put option with dividend yield"""
        if T <= 0 or sigma <= 0:
            return max(0, K - S)
        
        d1 = BlackScholesMerton.d1(S, K, T, r, sigma, q)
        d2 = BlackScholesMerton.d2(S, K, T, r, sigma, q)
        
        return K * np.exp(-r * T) * norm_cdf(-d2) - S * np.exp(-q * T) * norm_cdf(-d1)


def norm_cdf(x: float) -> float:
    """Standard normal cumulative distribution function"""
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def norm_pdf(x: float) -> float:
    """Standard normal probability density function"""
    return np.exp(-0.5 * x ** 2) / np.sqrt(2 * np.pi)


class StockPriceSimulator:
    """Simulate stock price movements using Geometric Brownian Motion (GBM)"""
    
    def __init__(self, initial_prices: Dict[str, float], 
                 volatilities: Dict[str, float], 
                 drift: float = 0.0):
        """
        Initialize stock price simulator
        
        Args:
            initial_prices: Dict mapping stock symbols to initial prices
            volatilities: Dict mapping stock symbols to volatilities
            drift: Annual drift (expected return)
        """
        self.prices = initial_prices.copy()
        self.volatilities = volatilities
        self.drift = drift
        self.last_update_time = time.time()
    
    def update_prices(self) -> Dict[str, float]:
        """
        Update stock prices using GBM
        
        Returns:
            Dict mapping stock symbols to updated prices
        """
        current_time = time.time()
        dt = (current_time - self.last_update_time) / (365 * 24 * 60 * 60)  # Time in years
        self.last_update_time = current_time
        
        for symbol in self.prices:
            # Get volatility for this symbol
            vol = self.volatilities.get(symbol, 0.2)  # Default 20% volatility
            
            # Generate random normal variable
            z = np.random.normal(0, 1)
            
            # GBM formula: S(t+dt) = S(t) * exp((mu - sigma^2/2)*dt + sigma*sqrt(dt)*z)
            self.prices[symbol] *= np.exp((self.drift - 0.5 * vol ** 2) * dt + vol * np.sqrt(dt) * z)
        
        return self.prices


class OptionGenerator:
    """Generate realistic option data for a given underlying"""
    
    def __init__(self, 
                 underlying: str, 
                 underlying_price: float, 
                 implied_volatility: float, 
                 risk_free_rate: float = RISK_FREE_RATE):
        """
        Initialize option generator
        
        Args:
            underlying: Underlying symbol
            underlying_price: Current price of the underlying
            implied_volatility: Base implied volatility (ATM) for the underlying
            risk_free_rate: Annual risk-free rate
        """
        self.underlying = underlying
        self.underlying_price = underlying_price
        self.implied_volatility = implied_volatility
        self.risk_free_rate = risk_free_rate
        
        # Standard expiration periods in days
        self.expirations = [1, 2, 3, 7, 14, 30, 60, 90, 180, 270, 365]
        
        # Generate strikes based on current price
        self.strikes = self._generate_strikes()
    
    def _generate_strikes(self) -> List[float]:
        """Generate realistic strike prices based on underlying price"""
        price = self.underlying_price
        
        # Round price to appropriate precision
        if price < 10:
            tick_size = 0.5
        elif price < 50:
            tick_size = 1.0
        elif price < 100:
            tick_size = 2.5
        elif price < 200:
            tick_size = 5.0
        else:
            tick_size = 10.0
        
        base_price = round(price / tick_size) * tick_size
        
        # Generate strikes around the base price
        strikes = []
        
        # OTM strikes
        strike = base_price
        while strike <= price * 1.5:
            strikes.append(strike)
            strike += tick_size
        
        # ITM strikes
        strike = base_price - tick_size
        while strike >= price * 0.5:
            strikes.insert(0, strike)
            strike -= tick_size
        
        return strikes
    
    def _calculate_option_prices(self, strike: float, expiration_days: int) -> Dict:
        """
        Calculate option prices and Greeks for a given strike and expiration
        
        Args:
            strike: Strike price
            expiration_days: Days to expiration
            
        Returns:
            Dict with call and put prices, IVs, and Greeks
        """
        T = expiration_days / 365.0  # Time to expiration in years
        S = self.underlying_price
        K = strike
        r = self.risk_free_rate
        
        # Calculate IV (varies by moneyness: higher for OTM options - volatility smile)
        moneyness = K / S
        
        # Simple model for volatility smile
        if moneyness < 0.8:  # Deep ITM call
            iv_adjustment = 0.2
        elif moneyness < 0.9:  # ITM call
            iv_adjustment = 0.1
        elif moneyness < 1.1:  # ATM
            iv_adjustment = 0.0
        elif moneyness < 1.2:  # OTM call
            iv_adjustment = 0.1
        else:  # Deep OTM call
            iv_adjustment = 0.2
        
        # Also adjust IV for expiration (term structure)
        if T < 7/365:  # Less than a week
            term_adjustment = 0.15
        elif T < 30/365:  # Less than a month
            term_adjustment = 0.05
        elif T < 90/365:  # Less than 3 months
            term_adjustment = 0.0
        else:  # Longer dated
            term_adjustment = -0.05
        
        iv = max(0.05, self.implied_volatility + iv_adjustment + term_adjustment)
        
        # Calculate prices
        call_price = BlackScholesMerton.call_price(S, K, T, r, iv)
        put_price = BlackScholesMerton.put_price(S, K, T, r, iv)
        
        # Add bid-ask spread
        spread_pct = 0.05 + (0.2 * abs(1 - moneyness)) + (0.1 * (1/(T+0.1)))
        call_bid = max(0.01, call_price * (1 - spread_pct/2))
        call_ask = call_price * (1 + spread_pct/2)
        put_bid = max(0.01, put_price * (1 - spread_pct/2))
        put_ask = put_price * (1 + spread_pct/2)
        
        # Calculate volume and open interest (higher for ATM options, lower for far OTM/ITM)
        atm_factor = np.exp(-10 * (moneyness - 1) ** 2)
        time_factor = np.exp(-2 * T)
        
        base_volume = int(1000 * atm_factor * (1 - 0.5 * time_factor))
        base_oi = int(5000 * atm_factor * (1 - 0.3 * time_factor))
        
        # Add randomness
        call_volume = max(0, int(base_volume * (0.5 + random.random())))
        put_volume = max(0, int(base_volume * (0.5 + random.random())))
        call_oi = max(0, int(base_oi * (0.5 + random.random())))
        put_oi = max(0, int(base_oi * (0.5 + random.random())))
        
        return {
            "call": {
                "price": call_price,
                "bid": call_bid,
                "ask": call_ask,
                "iv": iv,
                "volume": call_volume,
                "open_interest": call_oi
            },
            "put": {
                "price": put_price,
                "bid": put_bid,
                "ask": put_ask,
                "iv": iv,
                "volume": put_volume,
                "open_interest": put_oi
            }
        }
    
    def generate_option(self, introduce_anomaly: bool = False) -> Dict:
        """
        Generate a random option contract data
        
        Args:
            introduce_anomaly: Whether to introduce pricing anomalies
            
        Returns:
            Dict with option data
        """
        # Randomly select strike and expiration
        strike = random.choice(self.strikes)
        expiration_days = random.choice(self.expirations)
        option_type = random.choice(["call", "put"])
        
        # Create option symbol
        expiry_date = datetime.datetime.now() + datetime.timedelta(days=expiration_days)
        expiry_str = expiry_date.strftime('%y%m%d')
        symbol = f"{self.underlying}{expiry_str}{option_type[0].upper()}{int(strike):05d}"
        
        # Calculate normal prices
        option_data = self._calculate_option_prices(strike, expiration_days)
        
        # Extract data for selected option type
        data = option_data[option_type]
        
        # Introduce anomaly if requested
        if introduce_anomaly:
            anomaly_type = random.choice(["price_spike", "vol_spike", "spread_anomaly"])
            
            if anomaly_type == "price_spike":
                # Significant price spike without corresponding IV change
                factor = random.uniform(1.3, 2.0)
                data["bid"] *= factor
                data["ask"] *= factor
                # IV remains unchanged to create a distortion
                
            elif anomaly_type == "vol_spike":
                # Implied volatility spike
                data["iv"] *= random.uniform(1.5, 3.0)
                # Recalculate prices based on new IV
                T = expiration_days / 365.0
                if option_type == "call":
                    new_price = BlackScholesMerton.call_price(
                        self.underlying_price, strike, T, self.risk_free_rate, data["iv"]
                    )
                else:
                    new_price = BlackScholesMerton.put_price(
                        self.underlying_price, strike, T, self.risk_free_rate, data["iv"]
                    )
                
                spread_pct = (data["ask"] - data["bid"]) / ((data["ask"] + data["bid"]) / 2)
                data["bid"] = new_price * (1 - spread_pct/2)
                data["ask"] = new_price * (1 + spread_pct/2)
                
            elif anomaly_type == "spread_anomaly":
                # Abnormal bid-ask spread
                mid_price = (data["bid"] + data["ask"]) / 2
                data["bid"] = mid_price * 0.5
                data["ask"] = mid_price * 1.5
        
        # Create output format matching the anomaly detection system
        option_dict = {
            "timestamp": time.time(),
            "symbol": symbol,
            "underlying": self.underlying,
            "option_type": option_type,
            "strike": strike,
            "expiration": expiration_days / 365.0,  # Convert to years
            "bid": data["bid"],
            "ask": data["ask"],
            "volume": data["volume"],
            "open_interest": data["open_interest"],
            "implied_volatility": data["iv"],
            "underlying_price": self.underlying_price
        }
        
        return option_dict


class KafkaProducer:
    """Helper class for producing messages to Kafka"""
    
    def __init__(self, bootstrap_servers: str):
        """
        Initialize Kafka producer
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
        """
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': f'options-simulator-{uuid.uuid4()}',
            'queue.buffering.max.messages': 10000,
            'queue.buffering.max.kbytes': 10000,
            'compression.type': 'lz4'
        })
        
        logger.info(f"Kafka producer initialized: {bootstrap_servers}")
    
    def send(self, topic: str, key: str, value: Dict) -> None:
        """
        Send message to Kafka
        
        Args:
            topic: Kafka topic
            key: Message key
            value: Message value (will be serialized to JSON)
        """
        try:
            value_json = json.dumps(value)
            self.producer.produce(
                topic,
                key=key.encode('utf-8'),
                value=value_json.encode('utf-8'),
                callback=self._delivery_report
            )
            # Trigger message delivery
            self.producer.poll(0)
        
        except Exception as e:
            logger.error(f"Error sending message: {str(e)}")
    
    def _delivery_report(self, err, msg):
        """Callback for delivery reports"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')
    
    def flush(self) -> None:
        """Flush producer"""
        self.producer.flush()


def main():
    """Main function for option data simulation"""
    parser = argparse.ArgumentParser(description='Options Data Simulator')
    
    parser.add_argument(
        '--kafka_server',
        type=str,
        default='localhost:9092',
        help='Kafka bootstrap server'
    )
    
    parser.add_argument(
        '--topic',
        type=str,
        default='options_data',
        help='Kafka topic to produce to'
    )
    
    parser.add_argument(
        '--rate',
        type=int,
        default=10,
        help='Number of option records to generate per second'
    )
    
    parser.add_argument(
        '--anomaly_rate',
        type=float,
        default=0.02,
        help='Probability of generating an anomalous option (0-1)'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    args = parser.parse_args()
    
    # Set log level
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    # Initialize Kafka producer
    kafka_producer = KafkaProducer(args.kafka_server)
    
    # Initialize stock simulator with some sample stocks
    # Initial prices and volatilities
    initial_prices = {
        "AAPL": 180.0,
        "MSFT": 345.0,
        "GOOGL": 150.0,
        "AMZN": 170.0,
        "META": 480.0,
        "TSLA": 180.0,
        "SPY": 515.0,
        "QQQ": 430.0,
        "IWM": 205.0
    }
    
    volatilities = {
        "AAPL": 0.25,
        "MSFT": 0.22,
        "GOOGL": 0.28,
        "AMZN": 0.30,
        "META": 0.35,
        "TSLA": 0.55,
        "SPY": 0.15,
        "QQQ": 0.18,
        "IWM": 0.20
    }
    
    stock_simulator = StockPriceSimulator(
        initial_prices=initial_prices,
        volatilities=volatilities,
        drift=0.05  # 5% annual expected return
    )
    
    # Map to store option generators for each underlying
    option_generators = {}
    
    # Main simulation loop
    try:
        logger.info(f"Starting options data simulation. Press Ctrl+C to stop.")
        logger.info(f"Producing to topic: {args.topic} at rate: {args.rate} options/sec")
        
        while True:
            # Update stock prices every second
            if int(time.time()) % 1 == 0:
                updated_prices = stock_simulator.update_prices()
                
                # Update or create option generators with new prices
                for symbol, price in updated_prices.items():
                    option_generators[symbol] = OptionGenerator(
                        underlying=symbol,
                        underlying_price=price,
                        implied_volatility=volatilities.get(symbol, 0.2)
                    )
            
            # Generate options based on rate
            for _ in range(args.rate):
                # Select random underlying
                underlying = random.choice(list(option_generators.keys()))
                generator = option_generators[underlying]
                
                # Determine if this option should be anomalous
                is_anomaly = random.random() < args.anomaly_rate
                
                # Generate option data
                option_data = generator.generate_option(introduce_anomaly=is_anomaly)
                
                # Log anomalies
                if is_anomaly:
                    logger.info(f"Generated anomalous option: {option_data['symbol']}")
                
                # Send to Kafka
                kafka_producer.send(
                    topic=args.topic,
                    key=option_data['symbol'],
                    value=option_data
                )
            
            # Sleep to achieve desired rate
            time.sleep(1.0)
    
    except KeyboardInterrupt:
        logger.info("Simulation stopped by user")
    
    finally:
        # Flush producer before exit
        kafka_producer.flush()
        logger.info("Simulation ended")


if __name__ == "__main__":
    main()
