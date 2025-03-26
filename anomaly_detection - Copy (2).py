"""
Options Pricing Anomaly Detection System

A production-ready implementation of a dual-layer anomaly detection system for options:
1. Layer 1: Implied Volatility Surface Anomaly Alerts
2. Layer 2: Unsupervised Greed-Based Anomaly Detection with Isolation Forest

This system is designed for options traders to detect pricing irregularities in options,
particularly for short-dated (0DTE) contracts.
"""
from american_options_pricing import AmericanOptionPricing
import os
import json
import time
import logging
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.interpolate import griddata, SmoothBivariateSpline
from scipy import stats
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
from concurrent.futures import ThreadPoolExecutor
import threading
import queue
import requests
import uuid
import warnings
import traceback
import argparse
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional, Union, Any, Callable
from abc import ABC, abstractmethod
from collections import Counter
import threading
from collections import Counter, deque
import threading
import time
import os
import json
import queue
import logging
import datetime
import traceback
from typing import Dict, List, Tuple, Optional, Union, Any, Callable



class AnomalySystem:
    """
    Thread-safe system for tracking statistics and storing detected anomalies
    """
    
    def __init__(self, max_stored_anomalies: int = 10000):
        """
        Initialize the anomaly system
        
        Args:
            max_stored_anomalies: Maximum number of anomalies to store in memory
        """
        # Statistics
        self.stats = {
            "detected_anomalies": 0,
            "processed_options": 0,
            "start_time": time.time()
        }
        
        # Recent anomalies storage - thread-safe deque with max size
        self.anomalies = deque(maxlen=max_stored_anomalies)
        
        # Thread safety
        self.lock = threading.Lock()
    
    def get_all_option_data(self) -> List[Dict]:
        """
        Get all stored option data
        
        Returns:
            List of all option data records
        """
        with self.lock:
            # Make a copy to avoid locking during processing
            anomalies_copy = list(self.anomalies)
        
        return anomalies_copy    

    def increment_processed(self, count: int = 1) -> None:
        """
        Increment processed options count
        
        Args:
            count: Number to increment by
        """
        with self.lock:
            self.stats["processed_options"] += count
    
    def add_anomaly(self, anomaly: Dict) -> None:
        """
        Add a detected anomaly to storage and update statistics
        
        Args:
            anomaly: Anomaly data dictionary
        """
        with self.lock:
            # Add to storage
            self.anomalies.appendleft(anomaly)
            
            # Increment counter
            self.stats["detected_anomalies"] += 1
            
            # Log the addition
            logger.info(f"Added anomaly to storage. Type: {anomaly.get('anomaly_type')}, " + 
                    f"Symbol: {anomaly.get('option_data', {}).get('symbol')}, " +
                    f"Total count: {self.stats['detected_anomalies']}")

    
    def get_stats(self) -> Dict:
        """
        Get current statistics
        
        Returns:
            Statistics dictionary
        """
        with self.lock:
            # Calculate uptime
            uptime = time.time() - self.stats["start_time"]
            
            # Return copy of stats
            result = self.stats.copy()
            result["uptime"] = int(uptime)
            
            return result
    
    def get_anomalies(self, limit: int = 100, 
                     underlying_filter: Optional[List[str]] = None,
                     option_type_filter: Optional[List[str]] = None,
                     anomaly_type_filter: Optional[List[str]] = None,
                     confidence_threshold: float = 0.0,
                     time_range: int = 0) -> List[Dict]:
        """
        Get recent anomalies with optional filtering
        
        Args:
            limit: Maximum number of anomalies to return
            underlying_filter: List of underlyings to include
            option_type_filter: List of option types to include
            anomaly_type_filter: List of anomaly types to include
            confidence_threshold: Minimum confidence threshold
            time_range: Time range in minutes (0 = all time)
            
        Returns:
            List of anomaly dictionaries
        """
        result = []
        
        with self.lock:
            # Make a copy to avoid locking during filtering
            anomalies_copy = list(self.anomalies)
        
        # Apply filters
        for anomaly in anomalies_copy:
            # Skip if we've reached the limit
            if len(result) >= limit:
                break
            
            option_data = anomaly.get("option_data", {})
            
            # Apply underlying filter
            if underlying_filter and option_data.get("underlying") not in underlying_filter:
                continue
                
            # Apply option type filter
            if option_type_filter and option_data.get("option_type") not in option_type_filter:
                continue
                
            # Apply anomaly type filter
            if anomaly_type_filter and anomaly.get("anomaly_type") not in anomaly_type_filter:
                continue
                
            # Apply confidence filter
            if anomaly.get("confidence", 0) < confidence_threshold:
                continue
                
            # Apply time range filter
            if time_range > 0:
                anomaly_time = anomaly.get("timestamp", 0)
                now = time.time()
                if now - anomaly_time > time_range * 60:  # Convert to seconds
                    continue
            
            # Add to result
            result.append(anomaly)
            
            # Check limit
            if len(result) >= limit:
                break
        
        return result

# Create a global instance
anomaly_system = AnomalySystem()




class StatsTracker:
    """Thread-safe statistics tracker for anomaly detection system"""
    
    def __init__(self):
        """Initialize the statistics tracker"""
        self.stats = {
            "detected_anomalies": 0,
            "processed_options": 0,
            "start_time": time.time()
        }
        self.lock = threading.Lock()
    
    def increment_processed(self, count: int = 1) -> None:
        """Increment processed options count"""
        with self.lock:
            self.stats["processed_options"] += count
    
    def increment_anomalies(self, count: int = 1) -> None:
        """Increment detected anomalies count"""
        with self.lock:
            self.stats["detected_anomalies"] += count
    
    def get_stats(self) -> Dict:
        """Get current statistics"""
        with self.lock:
            # Calculate uptime
            uptime = time.time() - self.stats["start_time"]
            
            # Return copy of stats
            result = self.stats.copy()
            result["uptime"] = int(uptime)
            
            return result

# Create a global instance of the stats tracker
global_stats = StatsTracker()


# Setup logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("anomaly_detection.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("anomaly_detection")

# Suppress specific warnings
warnings.filterwarnings("ignore", category=UserWarning, 
                      message="The frame.append method is deprecated")

# Constants
DEFAULT_CONFIG = {
    "kafka": {
        "bootstrap_servers": "localhost:9092",
        "group_id": "options_anomaly_detection",
        "input_topic": "options_data",
        "output_topic": "options_anomalies",
        "consumer_poll_timeout": 1.0
    },
    "anomaly_detection": {
        "iv_surface": {
            "enabled": True,
            "threshold_stddev": 1.0,
            "rolling_window": 120,  # in minutes
            "min_samples": 5
        },
        "isolation_forest": {
            "enabled": True,
            "contamination": 0.01,
            "n_estimators": 100,
            "max_samples": "auto",
            "random_state": 42,
            "model_update_interval": 60  # in minutes
        }
    },
    "dashboard": {
        "update_interval": 5,  # in seconds
        "api_port": 8000
    },
    "general": {
        "debug": False,
        "num_workers": 4
    }
}



#########################################
# Domain Model and Data Structures
#########################################

@dataclass
class OptionData:
    """Class for holding option contract data"""
    timestamp: float
    symbol: str
    underlying: str
    option_type: str  # 'call' or 'put'
    strike: float
    expiration: float  # time to expiration in years
    bid: float
    ask: float
    volume: int
    open_interest: int
    implied_volatility: Optional[float] = None
    underlying_price: Optional[float] = None
    delta: Optional[float] = None
    gamma: Optional[float] = None
    theta: Optional[float] = None
    vega: Optional[float] = None
    
    @property
    def mid_price(self) -> float:
        """Calculate the mid price of the option"""
        return (self.bid + self.ask) / 2.0
    
    @property
    def moneyness(self) -> float:
        """Calculate moneyness (K/S for options)"""
        if self.underlying_price is None or self.underlying_price <= 0:
            return float('nan')
        return self.strike / self.underlying_price
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return {
            "timestamp": self.timestamp,
            "symbol": self.symbol,
            "underlying": self.underlying,
            "option_type": self.option_type,
            "strike": self.strike,
            "expiration": self.expiration,
            "bid": self.bid,
            "ask": self.ask,
            "volume": self.volume,
            "open_interest": self.open_interest,
            "implied_volatility": self.implied_volatility,
            "underlying_price": self.underlying_price,
            "delta": self.delta,
            "gamma": self.gamma,
            "theta": self.theta,
            "vega": self.vega
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'OptionData':
        """Create an OptionData instance from dictionary"""
        return cls(**data)


@dataclass
class AnomalyResult:
    """Class for holding anomaly detection results"""
    option_data: OptionData
    anomaly_score: float
    anomaly_type: str  # 'iv_surface', 'isolation_forest', 'combined'
    description: str
    timestamp: float = field(default_factory=lambda: time.time())
    confidence: float = 1.0
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        result = {
            "option_data": self.option_data.to_dict(),
            "anomaly_score": self.anomaly_score,
            "anomaly_type": self.anomaly_type,
            "description": self.description,
            "timestamp": self.timestamp,
            "confidence": self.confidence
        }
        return result
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'AnomalyResult':
        """Create an AnomalyResult instance from dictionary"""
        option_data = OptionData.from_dict(data.pop("option_data"))
        return cls(option_data=option_data, **data)


#########################################
# Black-Scholes Model Implementation
#########################################

# Create a class with the same interface as the original BlackScholes class
class BlackScholes:
    """
    Replacement for the original BlackScholes class that uses American option pricing
    
    This class maintains the same interface as the original for backward compatibility,
    but uses American option pricing methods internally.
    """
    
    @staticmethod
    def _d1(S: float, K: float, T: float, r: float, sigma: float) -> float:
        """Calculate d1 component of Black-Scholes formula (for compatibility)"""
        if T <= 0 or sigma <= 0:
            return float('nan')
        return (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    
    @staticmethod
    def _d2(S: float, K: float, T: float, r: float, sigma: float) -> float:
        """Calculate d2 component of Black-Scholes formula (for compatibility)"""
        if T <= 0 or sigma <= 0:
            return float('nan')
        return BlackScholes._d1(S, K, T, r, sigma) - sigma * np.sqrt(T)
    
    @staticmethod
    def call_price(S: float, K: float, T: float, r: float, sigma: float, dividend_yield: float = 0.0) -> float:
        """Calculate the price of an American call option"""
        return AmericanOptionPricing.option_price(
            S=S, K=K, T=T, r=r, sigma=sigma, 
            option_type='call', dividend_yield=dividend_yield,
            method='binomial'
        )
    
    @staticmethod
    def put_price(S: float, K: float, T: float, r: float, sigma: float, dividend_yield: float = 0.0) -> float:
        """Calculate the price of an American put option"""
        return AmericanOptionPricing.option_price(
            S=S, K=K, T=T, r=r, sigma=sigma, 
            option_type='put', dividend_yield=dividend_yield,
            method='binomial'
        )
    
    @staticmethod
    def option_price(S: float, K: float, T: float, r: float, sigma: float, option_type: str, dividend_yield: float = 0.0) -> float:
        """Calculate option price based on type"""
        if option_type.lower() == 'call':
            return BlackScholes.call_price(S, K, T, r, sigma, dividend_yield)
        elif option_type.lower() == 'put':
            return BlackScholes.put_price(S, K, T, r, sigma, dividend_yield)
        else:
            raise ValueError(f"Invalid option type: {option_type}. Must be 'call' or 'put'.")
    
    @staticmethod
    def vega(S: float, K: float, T: float, r: float, sigma: float, dividend_yield: float = 0.0) -> float:
        """Calculate the vega of an option (sensitivity to volatility)"""
        # Vega is the same for calls and puts, so we'll use calls
        return AmericanOptionPricing.vega(
            S=S, K=K, T=T, r=r, sigma=sigma, 
            option_type='call', dividend_yield=dividend_yield,
            method='binomial'
        )
    
    @staticmethod
    def delta(S: float, K: float, T: float, r: float, sigma: float, option_type: str, dividend_yield: float = 0.0) -> float:
        """Calculate the delta of an option (sensitivity to underlying price)"""
        return AmericanOptionPricing.delta(
            S=S, K=K, T=T, r=r, sigma=sigma, 
            option_type=option_type, dividend_yield=dividend_yield,
            method='binomial'
        )
    
    @staticmethod
    def gamma(S: float, K: float, T: float, r: float, sigma: float, dividend_yield: float = 0.0) -> float:
        """Calculate the gamma of an option (second derivative to underlying price)"""
        # Gamma is the same for calls and puts, so we'll use calls
        return AmericanOptionPricing.gamma(
            S=S, K=K, T=T, r=r, sigma=sigma, 
            option_type='call', dividend_yield=dividend_yield,
            method='binomial'
        )
    
    @staticmethod
    def theta(S: float, K: float, T: float, r: float, sigma: float, option_type: str, dividend_yield: float = 0.0) -> float:
        """Calculate the theta of an option (sensitivity to time decay)"""
        return AmericanOptionPricing.theta(
            S=S, K=K, T=T, r=r, sigma=sigma, 
            option_type=option_type, dividend_yield=dividend_yield,
            method='binomial'
        )
    
    @staticmethod
    def rho(S: float, K: float, T: float, r: float, sigma: float, option_type: str, dividend_yield: float = 0.0) -> float:
        """Calculate the rho of an option (sensitivity to interest rate)"""
        return AmericanOptionPricing.rho(
            S=S, K=K, T=T, r=r, sigma=sigma, 
            option_type=option_type, dividend_yield=dividend_yield,
            method='binomial'
        )
    
    @staticmethod
    def implied_volatility(S: float, K: float, T: float, r: float, market_price: float, option_type: str) -> float:
        """
        Calculate implied volatility using Newton-Raphson method
        
        Args:
            S: Underlying price
            K: Strike price
            T: Time to expiration in years
            r: Risk-free rate
            market_price: Observed market price of the option
            option_type: 'call' or 'put'
            
        Returns:
            Implied volatility or NaN if the calculation fails
        """
        if T <= 0:
            return float('nan')
        
        # Initial guesses based on whether the option is in, at, or out of the money
        moneyness = K / S
        
        if moneyness < 0.8:  # Deep ITM
            sigma = 0.5
        elif moneyness > 1.2:  # Deep OTM
            sigma = 0.3
        else:  # Near the money
            sigma = 0.2
        
        # Handle boundary conditions
        # Ensure the market price is within theoretical bounds
        if option_type.lower() == 'call':
            intrinsic = max(0, S - K * np.exp(-r * T))
            if market_price < intrinsic:
                return float('nan')  # Price is below intrinsic value
            if market_price >= S:
                return float('inf')  # Price is too high
        else:  # Put
            intrinsic = max(0, K * np.exp(-r * T) - S)
            if market_price < intrinsic:
                return float('nan')  # Price is below intrinsic value
            if market_price >= K:
                return float('inf')  # Price is too high
                
        max_iterations = 100
        precision = 1.0e-6
        
        for i in range(max_iterations):
            # Calculate option price and vega with current sigma
            if option_type.lower() == 'call':
                price = BlackScholes.call_price(S, K, T, r, sigma)
            else:
                price = BlackScholes.put_price(S, K, T, r, sigma)
                
            vega = BlackScholes.vega(S, K, T, r, sigma)
            
            # Check for convergence
            price_diff = market_price - price
            
            if abs(price_diff) < precision:
                return sigma
            
            # Avoid division by zero
            if abs(vega) < 1e-10:
                return float('nan')
            
            # Update sigma using Newton-Raphson step
            sigma = sigma + price_diff / vega
            
            # Check if sigma is within reasonable bounds
            if sigma <= 0.001:
                sigma = 0.001
            elif sigma > 5.0:
                return float('nan')  # Implausibly high volatility
                
        # If we reached max iterations without converging
        return float('nan')


#########################################
# Anomaly Detection Algorithms
#########################################

class AnomalyDetector(ABC):
    """Abstract base class for anomaly detection algorithms"""
    
    @abstractmethod
    def fit(self, data: List[OptionData]) -> None:
        """Fit the model on the provided data"""
        pass
    
    @abstractmethod
    def detect(self, option_data: OptionData) -> Optional[AnomalyResult]:
        """Detect if the provided option data is anomalous"""
        pass
    
    @abstractmethod
    def name(self) -> str:
        """Return the name of the detector"""
        pass


class IVSurfaceAnomalyDetector(AnomalyDetector):
    """
    Implied Volatility Surface Anomaly Detector
    
    Detects anomalies by comparing observed implied volatility to predicted
    implied volatility from a fitted surface
    """
    
    def __init__(self, threshold_stddev: float = 2.0, rolling_window: int = 60, min_samples: int = 10):
        """
        Initialize the IV Surface anomaly detector
        
        Args:
            threshold_stddev: Number of standard deviations for anomaly threshold
            rolling_window: Rolling window size in minutes
            min_samples: Minimum number of samples required for fitting
        """
        self.threshold_stddev = threshold_stddev
        self.rolling_window = rolling_window * 60  # Convert to seconds
        self.min_samples = min_samples
        self.surfaces = {}  # Dict to store surfaces for each underlying
        self.residuals = {}  # Dict to store residuals for each underlying
        self.last_fit_time = {}  # Dict to track when each surface was last fitted
        
    def name(self) -> str:
        return "IV Surface Anomaly Detector"
    
    def _fit_surface(self, data: List[OptionData], underlying: str) -> None:
        """Fit an implied volatility surface for a specific underlying"""
        # Filter data for the specific underlying
        underlying_data = [d for d in data if d.underlying == underlying]
        
        # Filter out options with invalid or missing IVs
        valid_data = [d for d in underlying_data 
                     if d.implied_volatility is not None and not np.isnan(d.implied_volatility)
                     and d.moneyness > 0 and not np.isnan(d.moneyness)
                     and d.expiration > 0 and not np.isnan(d.expiration)]
        
        total_options = len(underlying_data)
        valid_options = len(valid_data)
        
        logger.info(f"Fitting IV surface for {underlying} with {valid_options}/{total_options} valid options")
        
        if len(valid_data) < self.min_samples:
            logger.warning(f"Not enough valid data to fit IV surface for {underlying}. Need at least {self.min_samples} samples.")
            # Don't return early - we'll use a simple fallback model if we don't have enough data
            if len(valid_data) < 3:  # Absolute minimum for any kind of surface
                return
        
        try:
            # Extract features and target for surface fitting
            X = np.array([(d.moneyness, d.expiration) for d in valid_data])
            y = np.array([d.implied_volatility for d in valid_data])
            
            # Log the data distribution to help with debugging
            logger.debug(f"Moneyness range: {np.min(X[:, 0]):.2f} - {np.max(X[:, 0]):.2f}")
            logger.debug(f"Expiration range: {np.min(X[:, 1]):.2f} - {np.max(X[:, 1]):.2f}")
            logger.debug(f"IV range: {np.min(y):.2f} - {np.max(y):.2f}")
            
            # Check if we have enough unique values for spline
            unique_moneyness = len(np.unique(X[:, 0]))
            unique_expiry = len(np.unique(X[:, 1]))
            
            if len(valid_data) >= self.min_samples and unique_moneyness >= 3 and unique_expiry >= 3:
                # Use SmoothBivariateSpline if we have enough data points
                try:
                    self.surfaces[underlying] = SmoothBivariateSpline(
                        X[:, 0], X[:, 1], y, 
                        kx=min(3, unique_moneyness-1), 
                        ky=min(3, unique_expiry-1),
                        s=len(y)  # Smoothing parameter - allow some flexibility
                    )
                    logger.info(f"Fitted SmoothBivariateSpline for {underlying}")
                except Exception as e:
                    logger.warning(f"Failed to fit SmoothBivariateSpline for {underlying}: {str(e)}")
                    # Fall back to griddata
                    self.surfaces[underlying] = (X, y, 'grid')
                    logger.info(f"Fallback to griddata interpolation for {underlying}")
            else:
                # For small datasets, use griddata interpolation
                self.surfaces[underlying] = (X, y, 'grid')
                logger.info(f"Using griddata interpolation for {underlying} due to limited data")
                
            # Calculate and store residuals using K-fold cross-validation to avoid overfitting
            from sklearn.model_selection import KFold
            
            if len(valid_data) >= 10:  # Only do cross-validation with enough data
                kf = KFold(n_splits=min(5, len(valid_data) // 2), shuffle=True, random_state=42)
                residuals = []
                
                for train_idx, test_idx in kf.split(X):
                    X_train, X_test = X[train_idx], X[test_idx]
                    y_train, y_test = y[train_idx], y[test_idx]
                    
                    # Fit on training data
                    if len(X_train) >= 10 and len(np.unique(X_train[:, 0])) >= 3 and len(np.unique(X_train[:, 1])) >= 3:
                        try:
                            fold_spline = SmoothBivariateSpline(
                                X_train[:, 0], X_train[:, 1], y_train,
                                kx=min(3, len(np.unique(X_train[:, 0]))-1),
                                ky=min(3, len(np.unique(X_train[:, 1]))-1)
                            )
                            # Predict on test data
                            y_pred = fold_spline.ev(X_test[:, 0], X_test[:, 1])
                            fold_residuals = y_test - y_pred
                            residuals.extend(fold_residuals)
                        except Exception as e:
                            logger.debug(f"Error in fold CV: {str(e)}")
                            continue
                    else:
                        # Not enough data for spline, use simple average
                        y_pred = np.mean(y_train) * np.ones_like(y_test)
                        fold_residuals = y_test - y_pred
                        residuals.extend(fold_residuals)
                
                if residuals:
                    mean_residual = np.mean(residuals)
                    std_residual = max(0.01, np.std(residuals))  # Ensure non-zero std
                    self.residuals[underlying] = (mean_residual, std_residual)
                    logger.info(f"CV residuals for {underlying}: mean={mean_residual:.4f}, std={std_residual:.4f}")
                else:
                    # Fallback if CV failed
                    self.residuals[underlying] = (0.0, 0.05) 
                    logger.warning(f"Using default residuals for {underlying} due to CV failure")
            else:
                # Not enough data for CV, use simple standard deviation
                if len(y) >= 2:
                    mean_residual = 0.0  # Assume zero mean for small datasets
                    std_residual = max(0.01, np.std(y) * 0.2)  # Use fraction of IV std
                    self.residuals[underlying] = (mean_residual, std_residual)
                    logger.info(f"Simple residuals for {underlying}: std={std_residual:.4f}")
                else:
                    # Default values for very small datasets
                    self.residuals[underlying] = (0.0, 0.05)
                    logger.warning(f"Using default residuals for {underlying} due to insufficient data")
                
            # Update last fit time
            self.last_fit_time[underlying] = time.time()
            
        except Exception as e:
            logger.error(f"Error fitting IV surface for {underlying}: {str(e)}")
            logger.debug(traceback.format_exc())
    
    def _predict_iv(self, data: List[OptionData], underlying: str) -> List[float]:
        """Predict implied volatility using the fitted surface with robust error handling"""
        if underlying not in self.surfaces:
            logger.debug(f"No surface found for {underlying}")
            return [float('nan')] * len(data)
        
        features = np.array([(d.moneyness, d.expiration) for d in data])
        
        # Check for NaN values in features
        valid_indices = ~np.isnan(features).any(axis=1)
        if not np.any(valid_indices):
            logger.debug(f"No valid features for {underlying}")
            return [float('nan')] * len(data)
        
        # Initialize predictions with NaNs
        predictions = np.full(len(data), np.nan)
        valid_idx = np.where(valid_indices)[0]
        
        if len(valid_idx) == 0:
            return predictions.tolist()
        
        # Split the calculation based on surface type
        surface = self.surfaces[underlying]
        
        try:
            if isinstance(surface, tuple):
                # This is a griddata surface
                X, y, method_type = surface if len(surface) == 3 else (*surface, 'grid')
                
                if method_type == 'grid':
                    # Use griddata for interpolation with robust error handling
                    valid_features = features[valid_indices]
                    try:
                        # Try linear interpolation first
                        valid_predictions = griddata(
                            X, y, valid_features, method='linear', rescale=True
                        )
                        
                        # For points outside the convex hull, use nearest
                        nan_mask = np.isnan(valid_predictions)
                        if np.any(nan_mask):
                            nearest_pred = griddata(
                                X, y, valid_features[nan_mask], method='nearest', rescale=True
                            )
                            valid_predictions[nan_mask] = nearest_pred
                            
                    except Exception as e:
                        logger.warning(f"Griddata interpolation failed for {underlying}: {str(e)}")
                        # Use simple nearest neighbor as fallback
                        try:
                            valid_predictions = griddata(
                                X, y, valid_features, method='nearest', rescale=True
                            )
                        except Exception as e2:
                            logger.error(f"All griddata methods failed for {underlying}: {str(e2)}")
                            # Last resort: use average IV for each moneyness range
                            valid_predictions = np.zeros(len(valid_features))
                            for i, (m, _) in enumerate(valid_features):
                                # Find similar moneyness points
                                similar_idx = np.abs(X[:, 0] - m) < 0.1
                                if np.any(similar_idx):
                                    valid_predictions[i] = np.mean(y[similar_idx])
                                else:
                                    valid_predictions[i] = np.mean(y)  # Global average
                    
                    predictions[valid_idx] = valid_predictions
                    
            else:
                # This is a SmoothBivariateSpline surface
                try:
                    spline = self.surfaces[underlying]
                    valid_features = features[valid_indices]
                    valid_predictions = spline.ev(
                        valid_features[:, 0], valid_features[:, 1]
                    )
                    predictions[valid_idx] = valid_predictions
                    
                except Exception as e:
                    logger.error(f"Error predicting IV with spline for {underlying}: {str(e)}")
                    # Fallback to simple averaging based on moneyness
                    moneyness_values = features[valid_indices, 0]
                    expiration_values = features[valid_indices, 1]
                    
                    # Create rough estimate based on typical IV patterns
                    for i, (m, t) in enumerate(zip(moneyness_values, expiration_values)):
                        # Base IV - ATM options typically have IV around 0.25-0.30
                        base_iv = 0.25
                        
                        # Adjust for moneyness (smile effect)
                        moneyness_factor = 1.0 + 0.5 * abs(m - 1.0)
                        
                        # Adjust for time (term structure)
                        time_factor = 1.0
                        if t < 0.1:  # short dated
                            time_factor = 1.2
                        elif t > 0.5:  # long dated
                            time_factor = 0.9
                            
                        predictions[valid_idx[i]] = base_iv * moneyness_factor * time_factor
                        
                    logger.debug(f"Used fallback IV estimation for {underlying}")
            
            # Apply reasonable bounds to predictions
            min_iv = 0.05  # 5% minimum IV
            max_iv = 2.0   # 200% maximum IV
            
            valid_preds = ~np.isnan(predictions)
            predictions[valid_preds & (predictions < min_iv)] = min_iv
            predictions[valid_preds & (predictions > max_iv)] = max_iv
            
            return predictions.tolist()
            
        except Exception as e:
            logger.error(f"Error predicting IV with surface for {underlying}: {str(e)}")
            logger.debug(traceback.format_exc())
            # Return valid IV estimates for each option based on simple heuristics
            for i in valid_idx:
                m = features[i, 0]  # moneyness
                t = features[i, 1]  # time to expiration
                
                # Basic IV estimate based on moneyness and time
                if m < 0.85:
                    base_iv = 0.35  # ITM
                elif m > 1.15:
                    base_iv = 0.40  # OTM
                else:
                    base_iv = 0.30  # ATM
                    
                # Time adjustment
                if t < 0.05:
                    time_adj = 1.3  # Short term
                elif t > 0.5:
                    time_adj = 0.9  # Long term
                else:
                    time_adj = 1.0  # Medium term
                    
                predictions[i] = base_iv * time_adj
                
            return predictions.tolist()
    
    def fit(self, data: List[OptionData]) -> None:
        """
        Fit IV surfaces for each underlying in the data
        
        Args:
            data: List of option data records
        """
        # Group data by underlying
        underlyings = set(d.underlying for d in data)
        
        for underlying in underlyings:
            # Check if we need to update the surface
            current_time = time.time()
            last_fit = self.last_fit_time.get(underlying, 0)
            
            # Update every 5 minutes or if no surface exists
            if current_time - last_fit > 300 or underlying not in self.surfaces:
                try:
                    self._fit_surface(data, underlying)
                except Exception as e:
                    logger.error(f"Error in fit_surface for {underlying}: {str(e)}")
                    logger.debug(traceback.format_exc())
    
    def detect(self, option_data: OptionData) -> Optional[AnomalyResult]:
        """
        Detect if an option's implied volatility is anomalous
        
        Args:
            option_data: Option data record to check
            
        Returns:
            AnomalyResult if anomalous, None otherwise
        """
        # Extract key data for easier reference and debugging
        underlying = option_data.underlying
        symbol = option_data.symbol
        iv = option_data.implied_volatility
        
        # Debug log the incoming data
        logger.debug(f"Checking for IV anomaly: {symbol}, underlying={underlying}, IV={iv}")
        
        # Check prerequisites
        has_surface = underlying in self.surfaces
        has_residuals = underlying in self.residuals
        
        if not has_surface or not has_residuals:
            logger.debug(f"No surface/residuals for {underlying}, has_surface={has_surface}, has_residuals={has_residuals}")
            return None
        
        # Skip if IV is not available
        if iv is None or np.isnan(iv):
            logger.debug(f"Missing IV for {symbol}")
            return None
        
        # Skip if moneyness or expiration is invalid
        if np.isnan(option_data.moneyness) or option_data.moneyness <= 0 or np.isnan(option_data.expiration) or option_data.expiration <= 0:
            logger.debug(f"Invalid moneyness/expiration for {symbol}: moneyness={option_data.moneyness}, expiration={option_data.expiration}")
            return None
        
        # Predict IV for this option
        predicted_iv = self._predict_iv([option_data], underlying)[0]
        
        # Skip if prediction failed
        if np.isnan(predicted_iv):
            logger.debug(f"IV prediction failed for {symbol}")
            return None
        
        # Calculate residual
        residual = iv - predicted_iv
        
        # Get distribution parameters for this underlying
        mean_residual, std_residual = self.residuals[underlying]
        
        # Calculate Z-score
        if std_residual > 0:
            z_score = (residual - mean_residual) / std_residual
        else:
            z_score = 0
            logger.warning(f"Zero std_residual for {underlying}, cannot calculate z-score properly")
        
        # Log all options for debugging during development
        logger.debug(f"IV check for {symbol}: observed={iv:.4f}, predicted={predicted_iv:.4f}, z_score={z_score:.2f}, threshold={self.threshold_stddev}")
        
        # Check if anomalous
        if abs(z_score) > self.threshold_stddev:
            # Calculate mispricing percentage
            pct_diff = (iv - predicted_iv) / predicted_iv * 100 if predicted_iv > 0 else 0
            direction = "higher" if iv > predicted_iv else "lower"
            
            description = (
                f"IV anomaly detected for {symbol}. "
                f"Observed IV: {iv:.4f}, "
                f"Predicted IV: {predicted_iv:.4f}, "
                f"Z-score: {z_score:.2f}, "
                f"Observed IV is {abs(pct_diff):.1f}% {direction} than predicted."
            )
            
            logger.info(description)
            
            return AnomalyResult(
                option_data=option_data,
                anomaly_score=abs(z_score),
                anomaly_type="iv_surface",
                description=description,
                confidence=min(1.0, abs(z_score) / (self.threshold_stddev * 2))
            )
        
        return None


class IsolationForestAnomalyDetector(AnomalyDetector):
    """
    Isolation Forest anomaly detector for options data
    
    Uses multiple features from options data to detect anomalies
    """
    
    def __init__(self, contamination: float = 0.01, n_estimators: int = 100, 
                 max_samples: str = "auto", random_state: int = 42,
                 model_update_interval: int = 60):
        """
        Initialize the Isolation Forest detector
        
        Args:
            contamination: Expected proportion of anomalies
            n_estimators: Number of base estimators
            max_samples: Number of samples to draw for each estimator
            random_state: Random state for reproducibility
            model_update_interval: Interval in minutes to update the model
        """
        self.contamination = contamination
        self.n_estimators = n_estimators
        self.max_samples = max_samples
        self.random_state = random_state
        self.model_update_interval = model_update_interval * 60  # Convert to seconds
        
        self.models = {}  # Dict to store models for each underlying and option type
        self.scalers = {}  # Dict to store scalers for each underlying and option type
        self.feature_names = []  # Names of features used in the model
        self.last_fit_time = {}  # Dict to track when each model was last fitted
        
    def name(self) -> str:
        return "Isolation Forest Anomaly Detector"
    
    def _extract_features(self, option_data: OptionData) -> np.ndarray:
        """Extract feature vector from option data with improved handling of missing values"""
        # Start with complete set of features with NaN values
        features = [
            option_data.moneyness,
            option_data.expiration,
            option_data.implied_volatility if option_data.implied_volatility else np.nan,
            option_data.delta if option_data.delta else np.nan,
            option_data.gamma if option_data.gamma else np.nan,
            option_data.theta if option_data.theta else np.nan,
            option_data.vega if option_data.vega else np.nan,
            option_data.volume if option_data.volume > 0 else 0,
            option_data.open_interest if option_data.open_interest > 0 else 0
        ]
        
        # Calculate relative spread safely
        if option_data.bid > 0 and option_data.ask > option_data.bid and option_data.mid_price > 0:
            relative_spread = (option_data.ask - option_data.bid) / option_data.mid_price
            # Cap extremely large spreads
            relative_spread = min(relative_spread, 2.0)
        else:
            relative_spread = np.nan
        
        features.append(relative_spread)
        
        # Create additional derived features that may help with anomaly detection
        if option_data.strike > 0 and option_data.underlying_price > 0:
            # Strike distance from underlying price in percentage
            strike_distance = abs(option_data.strike - option_data.underlying_price) / option_data.underlying_price
            features.append(strike_distance)
        else:
            features.append(np.nan)
        
        # Add option type as a numerical feature (0 for put, 1 for call)
        if option_data.option_type and option_data.option_type.lower() in ('call', 'put'):
            option_type_num = 1.0 if option_data.option_type.lower() == 'call' else 0.0
            features.append(option_type_num)
        else:
            features.append(np.nan)
        
        return np.array(features).reshape(1, -1)
    
    def _feature_names(self) -> List[str]:
        """Return the names of features used in the model"""
        return [
            "moneyness",
            "expiration",
            "implied_volatility",
            "delta",
            "gamma",
            "theta",
            "vega",
            "volume",
            "open_interest",
            "relative_spread",
            "strike_distance",
            "option_type"
        ]
    
    def _impute_features(self, features: np.ndarray) -> np.ndarray:
        """Impute missing values in features to allow model usage with incomplete data"""
        # Create a copy to avoid modifying the original
        imputed = features.copy()
        
        # Default values for imputation based on column index
        defaults = [
            1.0,     # moneyness - ATM
            0.1,     # expiration - ~1 month
            0.3,     # implied_volatility - moderate IV
            0.5,     # delta - ATM
            0.05,    # gamma
            -0.01,   # theta
            0.1,     # vega
            0,       # volume
            0,       # open_interest
            0.05,    # relative_spread
            0.1,     # strike_distance
            0.5      # option_type
        ]
        
        # Impute each feature
        for i in range(imputed.shape[1]):
            mask = np.isnan(imputed[:, i])
            if np.any(mask):
                imputed[mask, i] = defaults[i]
        
        return imputed
    
    def fit(self, data: List[OptionData]) -> None:
        """
        Fit Isolation Forest models for each underlying and option type
        
        Args:
            data: List of option data records
        """
        # Group data by underlying and option type
        grouped_data = {}
        for d in data:
            if not d.underlying or not d.option_type:
                continue
                
            key = (d.underlying, d.option_type)
            if key not in grouped_data:
                grouped_data[key] = []
            grouped_data[key].append(d)
        
        for (underlying, option_type), group_data in grouped_data.items():
            # Skip if not enough data
            if len(group_data) < 10:
                logger.debug(f"Skipping model fitting for {underlying} {option_type}: insufficient data ({len(group_data)} samples)")
                continue
                
            # Check if we need to update the model
            key = (underlying, option_type)
            current_time = time.time()
            last_fit = self.last_fit_time.get(key, 0)
            
            # Update model if it's time or if we don't have a model yet
            if current_time - last_fit > self.model_update_interval or key not in self.models:
                try:
                    logger.info(f"Fitting Isolation Forest model for {underlying} {option_type} with {len(group_data)} samples")
                    
                    # Extract features for all options in the group
                    features_list = []
                    for d in group_data:
                        features = self._extract_features(d)[0]
                        features_list.append(features)
                    
                    if not features_list:
                        logger.warning(f"No valid features extracted for {underlying} {option_type}")
                        continue
                    
                    # Convert to numpy array
                    features = np.array(features_list)
                    
                    # Count valid samples (rows with no NaN)
                    valid_samples = np.sum(~np.isnan(features).any(axis=1))
                    logger.debug(f"Valid complete samples for {underlying} {option_type}: {valid_samples}/{len(features)}")
                    
                    # Impute missing values
                    imputed_features = self._impute_features(features)
                    
                    # Create and fit scaler
                    scaler = StandardScaler()
                    scaled_features = scaler.fit_transform(imputed_features)
                    
                    # Create and fit model with adjusted contamination based on data size
                    # Use lower contamination for smaller datasets to reduce false positives
                    effective_contamination = min(self.contamination, 1.0/len(imputed_features))
                    
                    model = IsolationForest(
                        contamination=effective_contamination,
                        n_estimators=self.n_estimators,
                        max_samples=self.max_samples,
                        random_state=self.random_state,
                        n_jobs=-1
                    )
                    model.fit(scaled_features)
                    
                    # Store model, scaler, and feature names
                    self.models[key] = model
                    self.scalers[key] = scaler
                    self.feature_names = self._feature_names()
                    
                    # Update last fit time
                    self.last_fit_time[key] = current_time
                    
                    logger.info(f"Successfully fitted Isolation Forest for {underlying} {option_type} with contamination {effective_contamination:.4f}")
                
                except Exception as e:
                    logger.error(f"Error fitting Isolation Forest for {underlying} {option_type}: {str(e)}")
                    logger.debug(traceback.format_exc())
    
    def detect(self, option_data: OptionData) -> Optional[AnomalyResult]:
        """
        Detect if an option is anomalous using Isolation Forest
        
        Args:
            option_data: Option data record to check
            
        Returns:
            AnomalyResult if anomalous, None otherwise
        """
        # Extract key data for easier reference and debugging
        underlying = option_data.underlying
        symbol = option_data.symbol
        option_type = option_data.option_type
        
        if not underlying or not option_type:
            logger.debug(f"Missing underlying or option_type for {symbol}")
            return None
            
        key = (underlying, option_type)
        
        # Check if we have a model for this underlying and option type
        if key not in self.models or key not in self.scalers:
            logger.debug(f"No model available for {underlying} {option_type}")
            return None
        
        # Extract features
        features = self._extract_features(option_data)
        
        # Check for too many NaN values (basic features must be present)
        essential_features = features[0, :2]  # moneyness and expiration
        if np.isnan(essential_features).any():
            logger.debug(f"Missing essential features for {symbol}")
            return None
        
        try:
            # Impute missing values
            imputed_features = self._impute_features(features)
            
            # Scale features
            scaled_features = self.scalers[key].transform(imputed_features)
            
            # Get anomaly score
            # Note: decision_function returns a score where negative values are anomalies
            # and the lower the value, the more anomalous the observation
            raw_score = self.models[key].decision_function(scaled_features)[0]
            
            # Invert score so that higher values indicate more anomalous observations
            anomaly_score = -raw_score
            
            # Debug log scores for all options
            logger.debug(f"Isolation Forest score for {symbol}: {anomaly_score:.4f}")
            
            # Check if anomalous (score < 0 in the original scale means anomalous)
            if raw_score < 0:
                # Try to determine why it's anomalous by comparing features to typical values
                reason = self._explain_anomaly(option_data, imputed_features[0], key)
                
                description = (
                    f"Anomaly detected by Isolation Forest for {symbol}. "
                    f"Anomaly score: {anomaly_score:.4f}. {reason}"
                )
                
                # Scale confidence based on anomaly score
                # Typically scores are in range [0, 0.5] after inversion
                confidence = min(1.0, anomaly_score / 0.5)
                
                logger.info(description)
                
                return AnomalyResult(
                    option_data=option_data,
                    anomaly_score=anomaly_score,
                    anomaly_type="isolation_forest",
                    description=description,
                    confidence=confidence
                )
        
        except Exception as e:
            logger.error(f"Error during anomaly detection with Isolation Forest for {symbol}: {str(e)}")
            logger.debug(traceback.format_exc())
        
        return None
    
    def _explain_anomaly(self, option_data: OptionData, features: np.ndarray, key: Tuple[str, str]) -> str:
        """Attempt to explain why an option was flagged as anomalous"""
        try:
            # Get feature distribution statistics from the fitted scaler
            feature_means = self.scalers[key].mean_
            feature_stds = np.sqrt(self.scalers[key].var_)
            
            # Calculate z-scores for this option's features
            z_scores = (features - feature_means) / feature_stds
            
            # Find the most anomalous features (highest absolute z-scores)
            feature_names = self._feature_names()
            anomalous_features = []
            
            for i, (name, z_score) in enumerate(zip(feature_names, z_scores)):
                if abs(z_score) > 2.0:  # More than 2 standard deviations from mean
                    direction = "high" if z_score > 0 else "low"
                    anomalous_features.append(f"{name} ({direction}, {abs(z_score):.1f}σ)")
            
            if anomalous_features:
                return f"Unusual features: {', '.join(anomalous_features[:3])}"
            
            # If no specific feature stands out, check for unusual combinations
            if option_data.implied_volatility and option_data.delta:
                if option_data.implied_volatility > 1.0 and abs(option_data.delta) < 0.1:
                    return "Unusual combination: high IV with low delta"
                if option_data.implied_volatility < 0.1 and abs(option_data.delta) > 0.9:
                    return "Unusual combination: low IV with high delta"
            
            # Default explanation if nothing specific found
            return "Unusual combination of multiple features"
            
        except Exception as e:
            logger.debug(f"Error explaining anomaly: {str(e)}")
            return "Unusual feature pattern detected"


class CombinedAnomalyDetector(AnomalyDetector):
    """
    Combined anomaly detector that integrates results from multiple detectors
    """
    
    def __init__(self, detectors: List[AnomalyDetector]):
        """
        Initialize the combined detector
        
        Args:
            detectors: List of anomaly detectors to combine
        """
        self.detectors = detectors
    
    def name(self) -> str:
        return "Combined Anomaly Detector"
    
    def fit(self, data: List[OptionData]) -> None:
        """
        Fit all underlying detectors
        
        Args:
            data: List of option data records
        """
        for detector in self.detectors:
            try:
                detector.fit(data)
            except Exception as e:
                logger.error(f"Error fitting {detector.name()}: {str(e)}")
                logger.debug(traceback.format_exc())
    
    def detect(self, option_data: OptionData) -> Optional[AnomalyResult]:
        """
        Detect anomalies using all underlying detectors
        
        Returns a combined result if multiple detectors flag the same option
        
        Args:
            option_data: Option data record to check
            
        Returns:
            AnomalyResult if anomalous, None otherwise
        """
        results = []
        
        for detector in self.detectors:
            try:
                result = detector.detect(option_data)
                if result is not None:
                    results.append(result)
            except Exception as e:
                logger.error(f"Error in {detector.name()} detect: {str(e)}")
                logger.debug(traceback.format_exc())
        
        if len(results) > 1:
            # If multiple detectors found anomalies, create a combined result
            avg_score = np.mean([r.anomaly_score for r in results])
            avg_confidence = np.mean([r.confidence for r in results])
            
            description = (
                f"Multiple anomaly detectors flagged {option_data.symbol}. "
                f"Detectors: {', '.join([r.anomaly_type for r in results])}. "
                f"Combined score: {avg_score:.4f}"
            )
            
            return AnomalyResult(
                option_data=option_data,
                anomaly_score=avg_score,
                anomaly_type="combined",
                description=description,
                confidence=avg_confidence
            )
        elif len(results) == 1:
            # If only one detector found an anomaly, return its result
            return results[0]
        
        return None


#########################################
# Data Processing and Pipeline
#########################################

class DataProcessor:
    """
    Data processor for option data
    
    Handles data cleaning, feature calculation, and preparation for anomaly detection
    """
    
    def __init__(self, risk_free_rate: float = 0.05):
        """
        Initialize the data processor
        
        Args:
            risk_free_rate: Risk-free interest rate for option pricing
        """
        self.risk_free_rate = risk_free_rate
    
    def process(self, option_data: OptionData) -> OptionData:
        """
        Process option data by computing implied volatility and Greeks
        
        Args:
            option_data: Option data record to process
            
        Returns:
            Processed option data record
        """
        # Skip processing if already processed
        if option_data.implied_volatility is not None:
            return option_data
        
        # Skip if missing required data
        if (option_data.underlying_price is None or
            option_data.strike is None or
            option_data.expiration is None or
            option_data.bid is None or
            option_data.ask is None):
            logger.debug(f"Skipping processing for {option_data.symbol}: missing required data")
            return option_data
        
        # Validate data before processing
        if (option_data.underlying_price <= 0 or
            option_data.strike <= 0 or
            option_data.expiration < 0):
            logger.debug(f"Skipping processing for {option_data.symbol}: invalid data values")
            return option_data
        
        # Calculate mid price with safeguards
        if option_data.bid >= 0 and option_data.ask >= option_data.bid:
            mid_price = option_data.mid_price
        else:
            # Fallback if bid/ask are suspicious
            mid_price = max(option_data.bid, option_data.ask/2)
        
        # Ensure mid price is positive to avoid IV calculation issues
        mid_price = max(0.01, mid_price)
        
        # Default dividend yield (can be enhanced to get actual dividends)
        dividend_yield = 0.0
        
        # Option type must be lowercase for BlackScholes methods
        option_type = option_data.option_type.lower() if option_data.option_type else 'call'
        if option_type not in ('call', 'put'):
            option_type = 'call'  # Default to call if unknown
        
        # Calculate implied volatility
        try:
            # Log the inputs for debugging
            logger.debug(f"Calculating IV for {option_data.symbol}: S={option_data.underlying_price}, K={option_data.strike}, T={option_data.expiration}, r={self.risk_free_rate}, price={mid_price}")
            
            iv = BlackScholes.implied_volatility(
                S=option_data.underlying_price,
                K=option_data.strike,
                T=max(0.001, option_data.expiration),  # Ensure minimum expiration time
                r=self.risk_free_rate,
                market_price=mid_price,
                option_type=option_type,
                dividend_yield=dividend_yield
            )
            
            # Validate IV result
            if iv is not None and not np.isnan(iv) and 0.001 <= iv <= 5.0:  # IV between 0.1% and 500%
                option_data.implied_volatility = iv
                logger.debug(f"Calculated IV for {option_data.symbol}: {iv:.4f}")
            else:
                logger.debug(f"Invalid IV calculated for {option_data.symbol}: {iv}")
                
                # Use a reasonable default IV based on moneyness and expiration
                moneyness = option_data.strike / option_data.underlying_price
                if moneyness > 1.5 or moneyness < 0.5:
                    # Deep OTM or ITM - higher IV
                    default_iv = 0.4
                else:
                    # Near the money - moderate IV
                    default_iv = 0.25
                    
                # Adjust for time to expiration
                if option_data.expiration < 0.02:  # < 1 week
                    default_iv *= 1.5  # Higher IV for short-dated options
                elif option_data.expiration > 0.5:  # > 6 months
                    default_iv *= 0.8  # Lower IV for long-dated options
                
                option_data.implied_volatility = default_iv
                logger.debug(f"Using default IV for {option_data.symbol}: {default_iv:.4f}")
        except Exception as e:
            logger.error(f"Error calculating implied volatility for {option_data.symbol}: {str(e)}")
            option_data.implied_volatility = None
        
        # Only proceed with Greek calculations if we have a valid IV
        if option_data.implied_volatility is not None and not np.isnan(option_data.implied_volatility):
            iv = option_data.implied_volatility
            
            # Calculate delta
            try:
                delta = BlackScholes.delta(
                    S=option_data.underlying_price,
                    K=option_data.strike,
                    T=max(0.001, option_data.expiration),
                    r=self.risk_free_rate,
                    sigma=iv,
                    option_type=option_type,
                    dividend_yield=dividend_yield
                )
                
                # Validate delta result
                if not np.isnan(delta) and -1.0 <= delta <= 1.0:
                    option_data.delta = delta
                    logger.debug(f"Calculated delta for {option_data.symbol}: {delta:.4f}")
            except Exception as e:
                logger.error(f"Error calculating delta for {option_data.symbol}: {str(e)}")
            
            # Calculate gamma
            try:
                gamma = BlackScholes.gamma(
                    S=option_data.underlying_price,
                    K=option_data.strike,
                    T=max(0.001, option_data.expiration),
                    r=self.risk_free_rate,
                    sigma=iv,
                    dividend_yield=dividend_yield
                )
                
                # Validate gamma result
                if not np.isnan(gamma) and 0.0 <= gamma <= 2.0:  # Reasonable bounds for gamma
                    option_data.gamma = gamma
                    logger.debug(f"Calculated gamma for {option_data.symbol}: {gamma:.4f}")
            except Exception as e:
                logger.error(f"Error calculating gamma for {option_data.symbol}: {str(e)}")
            
            # Calculate theta
            try:
                theta = BlackScholes.theta(
                    S=option_data.underlying_price,
                    K=option_data.strike,
                    T=max(0.001, option_data.expiration),
                    r=self.risk_free_rate,
                    sigma=iv,
                    option_type=option_type,
                    dividend_yield=dividend_yield
                )
                
                # Validate theta result (theta is typically negative)
                if not np.isnan(theta) and -1000.0 <= theta <= 0.1:  # Wide bounds for theta
                    option_data.theta = theta
                    logger.debug(f"Calculated theta for {option_data.symbol}: {theta:.4f}")
            except Exception as e:
                logger.error(f"Error calculating theta for {option_data.symbol}: {str(e)}")
            
            # Calculate vega
            try:
                vega = BlackScholes.vega(
                    S=option_data.underlying_price,
                    K=option_data.strike,
                    T=max(0.001, option_data.expiration),
                    r=self.risk_free_rate,
                    sigma=iv,
                    dividend_yield=dividend_yield
                )
                
                # Validate vega result
                if not np.isnan(vega) and 0.0 <= vega <= 100.0:  # Reasonable bounds for vega
                    option_data.vega = vega
                    logger.debug(f"Calculated vega for {option_data.symbol}: {vega:.4f}")
            except Exception as e:
                logger.error(f"Error calculating vega for {option_data.symbol}: {str(e)}")
        
        return option_data


class OptionDataCache:
    """
    Cache for storing historical option data
    
    Used to maintain a rolling window of data for anomaly detection model fitting
    """
    
    def __init__(self, retention_period: int = 3600):
        """
        Initialize the cache
        
        Args:
            retention_period: Data retention period in seconds
        """
        self.retention_period = retention_period
        self.data = {}  # Dict mapping underlying to list of option data records
        self.lock = threading.RLock()
    
    def add(self, option_data: OptionData) -> None:
        """
        Add option data to the cache
        
        Args:
            option_data: Option data record to add
        """
        with self.lock:
            underlying = option_data.underlying
            
            if underlying not in self.data:
                self.data[underlying] = []
            
            self.data[underlying].append(option_data)
    
    def get_all(self) -> List[OptionData]:
        """
        Get all option data from the cache
        
        Returns:
            List of all option data records
        """
        with self.lock:
            result = []
            for records in self.data.values():
                result.extend(records)
            return result
    
    def get_by_underlying(self, underlying: str) -> List[OptionData]:
        """
        Get option data for a specific underlying
        
        Args:
            underlying: Underlying symbol
            
        Returns:
            List of option data records for the underlying
        """
        with self.lock:
            return self.data.get(underlying, [])
    
    def clean(self) -> None:
        """Clean expired data from the cache"""
        with self.lock:
            current_time = time.time()
            
            for underlying in list(self.data.keys()):
                # Filter records within retention period
                self.data[underlying] = [
                    r for r in self.data[underlying]
                    if current_time - r.timestamp <= self.retention_period
                ]
                
                # Remove empty lists
                if not self.data[underlying]:
                    del self.data[underlying]


class AnomalyDetectionPipeline:
    """
    Pipeline for option anomaly detection
    
    Coordinates data processing and anomaly detection
    """
    
    def __init__(self, config: Dict):
        """
        Initialize the pipeline
        
        Args:
            config: Configuration dict
        """
        self.config = config
        self.data_processor = DataProcessor(
            risk_free_rate=config.get("risk_free_rate", 0.05)
        )
        
        # Set a longer retention period to have sufficient data for fitting
        retention_period = config.get("data_retention_period", 3600)
        self.data_cache = OptionDataCache(retention_period=retention_period)
        
        # Create anomaly detectors based on configuration
        detectors = []
        
        if config["anomaly_detection"]["iv_surface"]["enabled"]:
            iv_detector = IVSurfaceAnomalyDetector(
                threshold_stddev=config["anomaly_detection"]["iv_surface"]["threshold_stddev"],
                rolling_window=config["anomaly_detection"]["iv_surface"]["rolling_window"],
                min_samples=config["anomaly_detection"]["iv_surface"]["min_samples"]
            )
            detectors.append(iv_detector)
            logger.info(f"IV Surface detector enabled with threshold={iv_detector.threshold_stddev}, " +
                      f"rolling_window={iv_detector.rolling_window/60}m, min_samples={iv_detector.min_samples}")
        
        if config["anomaly_detection"]["isolation_forest"]["enabled"]:
            if_detector = IsolationForestAnomalyDetector(
                contamination=config["anomaly_detection"]["isolation_forest"]["contamination"],
                n_estimators=config["anomaly_detection"]["isolation_forest"]["n_estimators"],
                max_samples=config["anomaly_detection"]["isolation_forest"]["max_samples"],
                random_state=config["anomaly_detection"]["isolation_forest"]["random_state"],
                model_update_interval=config["anomaly_detection"]["isolation_forest"]["model_update_interval"]
            )
            detectors.append(if_detector)
            logger.info(f"Isolation Forest detector enabled with contamination={if_detector.contamination}, " +
                      f"n_estimators={if_detector.n_estimators}")
        
        self.detector = CombinedAnomalyDetector(detectors)
        
        # Status tracking
        self.last_model_fit_time = 0
        self.model_update_interval = 300  # 5 minutes in seconds
        
        # Cache cleaning thread
        self.clean_thread = threading.Thread(target=self._clean_cache_periodically, daemon=True)
        self.running = True
        self.clean_thread.start()
        
        logger.info("Anomaly detection pipeline initialized with " + 
                  f"{len(detectors)} detectors")
    
    def _clean_cache_periodically(self) -> None:
        """Periodically clean the data cache"""
        while self.running:
            try:
                self.data_cache.clean()
                logger.debug("Cleaned data cache")
            except Exception as e:
                logger.error(f"Error cleaning cache: {str(e)}")
            
            time.sleep(60)  # Clean every minute
    
    def process_data(self, option_data: OptionData) -> Tuple[OptionData, Optional[AnomalyResult]]:
        """
        Process option data and detect anomalies
        
        Args:
            option_data: Raw option data record
            
        Returns:
            Tuple of (processed_data, anomaly_result)
        """
        try:
            # Process option data
            processed_data = self.data_processor.process(option_data)
            
            # Add to cache for future model training
            self.data_cache.add(processed_data)
            
            # Decide if we should update models
            current_time = time.time()
            should_update_models = (current_time - self.last_model_fit_time > self.model_update_interval)
            
            # Also update models if we don't have any fitted models yet
            # Get all detector types and check if any are missing models
            missing_models = False
            for detector in self.detector.detectors:
                if isinstance(detector, IsolationForestAnomalyDetector) and not detector.models:
                    missing_models = True
                elif isinstance(detector, IVSurfaceAnomalyDetector) and not detector.surfaces:
                    missing_models = True
            
            # Periodically fit/update anomaly detection models
            if should_update_models or missing_models:
                logger.info("Fitting anomaly detection models")
                
                # Use a thread pool to fit models in parallel if we have a large dataset
                all_data = self.data_cache.get_all()
                
                if len(all_data) > 1000:
                    with ThreadPoolExecutor(max_workers=2) as executor:
                        # Split detectors and fit in parallel
                        for detector in self.detector.detectors:
                            executor.submit(detector.fit, all_data)
                else:
                    # Sequential fitting for smaller datasets
                    self.detector.fit(all_data)
                
                self.last_model_fit_time = current_time
            
            # Detect anomalies
            anomaly_result = self.detector.detect(processed_data)
            
            return processed_data, anomaly_result
        
        except Exception as e:
            logger.error(f"Error in process_data for {option_data.symbol}: {str(e)}")
            logger.debug(traceback.format_exc())
            return option_data, None
    
    def shutdown(self) -> None:
        """Shutdown the pipeline"""
        self.running = False
        self.clean_thread.join(timeout=5)
        logger.info("Anomaly detection pipeline shutdown")

#########################################
# Kafka Integration
#########################################

class KafkaProducerWrapper:
    """Wrapper for Kafka producer with error handling"""
    
    def __init__(self, bootstrap_servers: str, topic: str):
        """
        Initialize the Kafka producer
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Topic to produce to
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': f'anomaly-detection-producer-{uuid.uuid4()}',
            'linger.ms': 100,  # Batch messages for 100ms
            'queue.buffering.max.messages': 100000,
            'queue.buffering.max.kbytes': 100000,
            'compression.type': 'lz4',
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


class KafkaConsumerWrapper:
    """Wrapper for Kafka consumer with error handling"""
    
    def __init__(self, bootstrap_servers: str, group_id: str, topic: str, poll_timeout: float = 1.0):
        """
        Initialize the Kafka consumer
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            group_id: Consumer group ID
            topic: Topic to consume from
            poll_timeout: Poll timeout in seconds
        """
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topic = topic
        self.poll_timeout = poll_timeout
        
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'max.poll.interval.ms': 300000,  # 5 minutes
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000
        })
        
        self.consumer.subscribe([topic])
        logger.info(f"Kafka consumer initialized for topic {topic}")
    
    def poll(self) -> Optional[Dict]:
        """
        Poll for a message
        
        Returns:
            Message value as dict if available, None otherwise
        """
        try:
            msg = self.consumer.poll(self.poll_timeout)
            
            if msg is None:
                return None
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f'Reached end of partition {msg.topic()}-{msg.partition()}')
                else:
                    logger.error(f'Error while polling: {msg.error()}')
                return None
            
            # Parse message value
            try:
                value = json.loads(msg.value().decode('utf-8'))
                return value
            except json.JSONDecodeError:
                logger.error(f'Error decoding message value: {msg.value()}')
                return None
        
        except Exception as e:
            logger.error(f'Error polling Kafka: {str(e)}')
            return None
    
    def close(self) -> None:
        """Close the consumer"""
        self.consumer.close()


#########################################
# Main Application
#########################################

class AnomalyDetectionService:
    """Main service for option anomaly detection"""
    
    def __init__(self, config: Dict):
        """
        Initialize the service
        
        Args:
            config: Configuration dict
        """
        self.config = config
        self.running = False
        self.pipeline = AnomalyDetectionPipeline(config)
        
        # Initialize Kafka consumer and producer
        bootstrap_servers = config["kafka"]["bootstrap_servers"]
        group_id = config["kafka"]["group_id"]
        input_topic = config["kafka"]["input_topic"]
        output_topic = config["kafka"]["output_topic"]
        poll_timeout = config["kafka"]["consumer_poll_timeout"]
        
        self.consumer = KafkaConsumerWrapper(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            topic=input_topic,
            poll_timeout=poll_timeout
        )
        
        self.producer = KafkaProducerWrapper(
            bootstrap_servers=bootstrap_servers,
            topic=output_topic
        )
        
        # Worker thread pool
        num_workers = config["general"]["num_workers"]
        self.executor = ThreadPoolExecutor(max_workers=num_workers)
        self.tasks = queue.Queue()
        
        # Worker threads
        self.workers = []
        for _ in range(num_workers):
            worker = threading.Thread(target=self._worker_thread, daemon=True)
            self.workers.append(worker)
        
        # Main processing thread
        self.main_thread = threading.Thread(target=self._main_loop, daemon=True)
        
        logger.info("Anomaly detection service initialized")
    
# Fixed worker thread implementation for AnomalyDetectionService
    def _worker_thread(self) -> None:
        """Worker thread for processing tasks with improved error handling"""
        logger.info(f"Starting worker thread {threading.current_thread().name}")
        
        task_count = 0
        anomaly_count = 0
        
        while self.running:
            task = None
            try:
                # Get a task from the queue with a 1 second timeout
                try:
                    task = self.tasks.get(timeout=1)
                except queue.Empty:
                    # No tasks in the queue, just continue
                    continue
                    
                # Skip sentinel value (for shutdown)
                if task is None:
                    logger.debug(f"Worker {threading.current_thread().name} received shutdown signal")
                    break
                
                # Process the task (option data)
                option_data = task
                logger.debug(f"Processing option: {option_data.symbol} for {option_data.underlying}")
                
                try:
                    processed_data, anomaly_result = self.pipeline.process_data(option_data)
                    
                    # Track that we processed an option regardless of anomaly detection result
                    anomaly_system.increment_processed()
                    task_count += 1
                    
                    if anomaly_result:
                        anomaly_count += 1
                        logger.info(f"Anomaly detected: {anomaly_result.description}")
                        
                        # Add to anomaly system
                        anomaly_system.add_anomaly(anomaly_result.to_dict())
                        
                        # Publish to Kafka
                        self.producer.produce(
                            key=processed_data.symbol,
                            value=anomaly_result.to_dict()
                        )
                except Exception as process_error:
                    # Log the error but continue processing other options
                    logger.error(f"Error processing option {option_data.symbol}: {str(process_error)}")
                    logger.debug(traceback.format_exc())
                    
                    # Still increment processed count even if processing failed
                    anomaly_system.increment_processed()
                    task_count += 1
            
            except Exception as e:
                logger.error(f"Error in worker thread: {str(e)}")
                logger.debug(traceback.format_exc())
            
            finally:
                # Mark task as done if we got one
                if task is not None:
                    try:
                        self.tasks.task_done()
                    except ValueError:
                        # Task may have already been marked as done
                        logger.warning("Attempted to mark a task as done more than once")
        
        logger.info(f"Worker thread {threading.current_thread().name} shutting down. Processed {task_count} tasks, detected {anomaly_count} anomalies.")
    
    def reload_config(self, new_config: Dict) -> None:
        """
        Reload configuration and reconfigure components
        
        Args:
            new_config: New configuration dictionary
        """
        logger.info("Reloading configuration")
        
        # Update configuration
        self.config = new_config
        
        # Reconfigure anomaly detection algorithms
        iv_config = new_config["anomaly_detection"]["iv_surface"]
        if_config = new_config["anomaly_detection"]["isolation_forest"]
        
        # Update IV surface detector configuration
        for detector in self.pipeline.detector.detectors:
            if isinstance(detector, IVSurfaceAnomalyDetector):
                detector.threshold_stddev = iv_config.get("threshold_stddev", 2.0)
                detector.rolling_window = iv_config.get("rolling_window", 60) * 60  # Convert to seconds
                detector.min_samples = iv_config.get("min_samples", 5)
                logger.info(f"Updated IV Surface detector: threshold={detector.threshold_stddev}, window={detector.rolling_window/60}m")
                
            elif isinstance(detector, IsolationForestAnomalyDetector):
                detector.contamination = if_config.get("contamination", 0.01)
                detector.model_update_interval = if_config.get("model_update_interval", 30) * 60  # Convert to seconds
                logger.info(f"Updated Isolation Forest detector: contamination={detector.contamination}")
        
        # Reconfigure Kafka producer if needed
        kafka_config = new_config["kafka"]
        if kafka_config.get("bootstrap_servers") != self.producer.bootstrap_servers:
            logger.info(f"Recreating Kafka producer with new bootstrap servers: {kafka_config['bootstrap_servers']}")
            self.producer.close()
            self.producer = KafkaProducerWrapper(
                bootstrap_servers=kafka_config["bootstrap_servers"],
                topic=kafka_config["output_topic"]
            )
        
        # Update worker threads if needed
        num_workers = new_config["general"].get("num_workers", 4)
        current_workers = len(self.workers)
        
        if num_workers > current_workers:
            # Add more workers
            logger.info(f"Adding {num_workers - current_workers} worker threads")
            for i in range(current_workers, num_workers):
                worker = threading.Thread(
                    target=self._worker_thread,
                    daemon=True,
                    name=f"worker-{i}"
                )
                self.workers.append(worker)
                worker.start()
        
        # Note: Reducing worker count would require a more complex approach
        # that isn't implemented here since it would require safely shutting down
        # existing threads
        
        logger.info("Configuration reload complete")

    def _main_loop(self) -> None:
        """Main processing loop"""
        while self.running:
            try:
                message = self.consumer.poll()
                
                if message:
                    try:
                        # Parse message into OptionData
                        option_data = OptionData.from_dict(message)
                        
                        # Add to task queue for processing
                        self.tasks.put(option_data)
                    
                    except Exception as e:
                        logger.error(f"Error parsing message: {str(e)}")
                        logger.debug(f"Message: {message}")
                        logger.debug(traceback.format_exc())
            
            except Exception as e:
                logger.error(f"Error in main loop: {str(e)}")
                logger.debug(traceback.format_exc())
    
    def start(self) -> None:
        """Start the service"""
        if self.running:
            logger.warning("Service already running")
            return
        
        self.running = True
        
        # Start worker threads
        for worker in self.workers:
            worker.start()
        
        # Start main thread
        self.main_thread.start()
        
        logger.info("Anomaly detection service started")
    
    def stop(self) -> None:
        """Stop the service"""
        if not self.running:
            logger.warning("Service not running")
            return
        
        self.running = False
        
        # Add sentinel values to stop workers
        for _ in range(len(self.workers)):
            self.tasks.put(None)
        
        # Wait for workers to finish
        for worker in self.workers:
            worker.join(timeout=5)
        
        # Wait for main thread to finish
        self.main_thread.join(timeout=5)
        
        # Shutdown pipeline
        self.pipeline.shutdown()
        
        # Close Kafka connections
        self.consumer.close()
        self.producer.close()
        
        logger.info("Anomaly detection service stopped")


#########################################
# REST API for Dashboard
#########################################

def create_rest_api(anomaly_detection_service):
    """
    Create a REST API for the dashboard using Flask
    
    Args:
        anomaly_detection_service: Anomaly detection service instance
    """
    from flask import Flask, jsonify, request
    
    app = Flask("options_anomaly_detection")
    

    @app.route('/api/market_data', methods=['GET'])
    def get_market_data():
        """
        Get latest market data for all underlyings
        
        Returns:
            JSON with latest price data for all underlyings
        """
        try:
            # Get all option data from the cache
            all_data = anomaly_system.get_all_option_data()
            
            # Extract unique underlyings
            underlyings = {}
            
            # Get the latest price for each underlying
            for option in all_data:
                underlying = option.get('option_data', {}).get('underlying')
                price = option.get('option_data', {}).get('underlying_price')
                timestamp = option.get('timestamp', 0)
                
                if underlying and price:
                    # Keep the most recent price for each underlying
                    if underlying not in underlyings or timestamp > underlyings[underlying]['timestamp']:
                        underlyings[underlying] = {
                            'price': price,
                            'timestamp': timestamp
                        }
            
            # Format for response
            result = {
                'prices': {
                    symbol: data['price'] for symbol, data in underlyings.items()
                },
                'timestamp': time.time()
            }
            
            return jsonify(result)
        
        except Exception as e:
            logger.error(f"Error fetching market data: {str(e)}")
            return jsonify({'error': str(e)}), 500


    @app.route('/api/anomalies', methods=['GET'])
    def get_anomalies():
        """Get recent anomalies with optional filtering"""
        # Parse query parameters
        limit = request.args.get('limit', default=100, type=int)
        
        # Parse list filters
        underlying = request.args.get('underlying', default=None)
        underlying_filter = underlying.split(',') if underlying else None
        
        option_type = request.args.get('option_type', default=None)
        option_type_filter = option_type.split(',') if option_type else None
        
        anomaly_type = request.args.get('anomaly_type', default=None)
        anomaly_type_filter = anomaly_type.split(',') if anomaly_type else None
        
        # Parse numeric filters
        confidence = request.args.get('confidence', default=0.0, type=float)
        time_range = request.args.get('time_range', default=0, type=int)
        
        # Get anomalies with filters
        anomalies = anomaly_system.get_anomalies(
            limit=limit,
            underlying_filter=underlying_filter,
            option_type_filter=option_type_filter,
            anomaly_type_filter=anomaly_type_filter,
            confidence_threshold=confidence,
            time_range=time_range
        )
        
        return jsonify({"anomalies": anomalies})
    
    @app.route('/api/stats', methods=['GET'])
    def get_stats():
        """Get system statistics"""
        return jsonify(anomaly_system.get_stats())
    
    @app.route('/api/config', methods=['GET'])
    def get_config():
        """Get current configuration"""
        return jsonify(anomaly_detection_service.config)
    
    @app.route('/api/config', methods=['POST'])
    def update_config():
        """Update configuration with proper validation and restart of components"""
        try:
            # Get new config from request
            new_config = request.json
            if not new_config:
                return jsonify({"success": False, "message": "No configuration provided"}), 400
                
            # Validate configuration structure
            required_sections = ["kafka", "anomaly_detection", "dashboard", "general"]
            for section in required_sections:
                if section not in new_config:
                    return jsonify({"success": False, "message": f"Missing required section: {section}"}), 400
            
            # Validate Kafka configuration
            kafka_config = new_config.get("kafka", {})
            if not kafka_config.get("bootstrap_servers"):
                return jsonify({"success": False, "message": "Kafka bootstrap servers not specified"}), 400
            if not kafka_config.get("input_topic") or not kafka_config.get("output_topic"):
                return jsonify({"success": False, "message": "Kafka topics not specified"}), 400
                
            # Validate anomaly detection configuration
            anomaly_config = new_config.get("anomaly_detection", {})
            if "iv_surface" not in anomaly_config or "isolation_forest" not in anomaly_config:
                return jsonify({"success": False, "message": "Missing anomaly detection algorithms configuration"}), 400
                
            # Save configuration to file
            config_path = os.path.join("config", "config.json")
            with open(config_path, 'w') as f:
                json.dump(new_config, f, indent=2)
                
            # Signal service to reload configuration
            anomaly_detection_service.reload_config(new_config)
            
            return jsonify({
                "success": True, 
                "message": "Configuration updated successfully",
                "config": new_config
            })
            
        except Exception as e:
            logger.error(f"Error updating configuration: {str(e)}")
            return jsonify({"success": False, "message": f"Error updating configuration: {str(e)}"}), 500
    
    return app

#########################################
# Command Line Interface
#########################################

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Options Pricing Anomaly Detection System')
    
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
    
    return parser.parse_args()


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
                
                # Update config with file values
                for section, section_config in file_config.items():
                    if section in config:
                        if isinstance(section_config, dict):
                            config[section].update(section_config)
                        else:
                            config[section] = section_config
                    else:
                        config[section] = section_config
        
        except Exception as e:
            logger.error(f"Error loading configuration from {config_path}: {str(e)}")
            logger.debug(traceback.format_exc())
    
    return config


def main():
    """Main entry point"""
    args = parse_args()
    
    # Set log level
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    # Load configuration
    config = load_config(args.config)
    
    # Update config based on command line arguments
    if args.debug:
        config["general"]["debug"] = True
    
    # Create and start service
    service = AnomalyDetectionService(config)
    
    try:
        service.start()
        
        # Create REST API if dashboard enabled
        if config["dashboard"]["api_port"] > 0:
            from flask import Flask
            app = create_rest_api(service)
            app.run(host='0.0.0.0', port=config["dashboard"]["api_port"])
        else:
            # Keep the main thread alive
            while True:
                time.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    
    finally:
        service.stop()


if __name__ == "__main__":
    main()
