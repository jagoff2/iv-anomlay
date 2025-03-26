"""
Options Pricing Anomaly Detection System

A production-ready implementation of a dual-layer anomaly detection system for options:
1. Layer 1: Implied Volatility Surface Anomaly Alerts
2. Layer 2: Unsupervised Greed-Based Anomaly Detection with Isolation Forest

This system is designed for options traders to detect pricing irregularities in options,
particularly for short-dated (0DTE) contracts.
"""

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
            "threshold_stddev": 2.0,
            "rolling_window": 60,  # in minutes
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

class BlackScholes:
    """Black-Scholes option pricing model implementation"""
    
    @staticmethod
    def _d1(S: float, K: float, T: float, r: float, sigma: float) -> float:
        """Calculate d1 component of Black-Scholes formula"""
        if T <= 0 or sigma <= 0:
            return float('nan')
        return (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    
    @staticmethod
    def _d2(S: float, K: float, T: float, r: float, sigma: float) -> float:
        """Calculate d2 component of Black-Scholes formula"""
        if T <= 0 or sigma <= 0:
            return float('nan')
        return BlackScholes._d1(S, K, T, r, sigma) - sigma * np.sqrt(T)
    
    @staticmethod
    def call_price(S: float, K: float, T: float, r: float, sigma: float) -> float:
        """Calculate the price of a European call option"""
        if T <= 0 or sigma <= 0:
            return max(0, S - K)
        
        d1 = BlackScholes._d1(S, K, T, r, sigma)
        d2 = BlackScholes._d2(S, K, T, r, sigma)
        
        return S * stats.norm.cdf(d1) - K * np.exp(-r * T) * stats.norm.cdf(d2)
    
    @staticmethod
    def put_price(S: float, K: float, T: float, r: float, sigma: float) -> float:
        """Calculate the price of a European put option"""
        if T <= 0 or sigma <= 0:
            return max(0, K - S)
        
        d1 = BlackScholes._d1(S, K, T, r, sigma)
        d2 = BlackScholes._d2(S, K, T, r, sigma)
        
        return K * np.exp(-r * T) * stats.norm.cdf(-d2) - S * stats.norm.cdf(-d1)
    
    @staticmethod
    def option_price(S: float, K: float, T: float, r: float, sigma: float, option_type: str) -> float:
        """Calculate option price based on type"""
        if option_type.lower() == 'call':
            return BlackScholes.call_price(S, K, T, r, sigma)
        elif option_type.lower() == 'put':
            return BlackScholes.put_price(S, K, T, r, sigma)
        else:
            raise ValueError(f"Invalid option type: {option_type}. Must be 'call' or 'put'.")
    
    @staticmethod
    def vega(S: float, K: float, T: float, r: float, sigma: float) -> float:
        """Calculate the vega of an option (same for calls and puts)"""
        if T <= 0 or sigma <= 0:
            return 0.0
        
        d1 = BlackScholes._d1(S, K, T, r, sigma)
        return S * np.sqrt(T) * stats.norm.pdf(d1)
    
    @staticmethod
    def delta(S: float, K: float, T: float, r: float, sigma: float, option_type: str) -> float:
        """Calculate the delta of an option"""
        if T <= 0 or sigma <= 0:
            return 1.0 if option_type.lower() == 'call' else -1.0
        
        d1 = BlackScholes._d1(S, K, T, r, sigma)
        
        if option_type.lower() == 'call':
            return stats.norm.cdf(d1)
        elif option_type.lower() == 'put':
            return stats.norm.cdf(d1) - 1.0
        else:
            raise ValueError(f"Invalid option type: {option_type}. Must be 'call' or 'put'.")
    
    @staticmethod
    def gamma(S: float, K: float, T: float, r: float, sigma: float) -> float:
        """Calculate the gamma of an option (same for calls and puts)"""
        if T <= 0 or sigma <= 0:
            return 0.0
        
        d1 = BlackScholes._d1(S, K, T, r, sigma)
        return stats.norm.pdf(d1) / (S * sigma * np.sqrt(T))
    
    @staticmethod
    def theta(S: float, K: float, T: float, r: float, sigma: float, option_type: str) -> float:
        """Calculate the theta of an option"""
        if T <= 0 or sigma <= 0:
            return 0.0
        
        d1 = BlackScholes._d1(S, K, T, r, sigma)
        d2 = BlackScholes._d2(S, K, T, r, sigma)
        
        if option_type.lower() == 'call':
            return -S * stats.norm.pdf(d1) * sigma / (2 * np.sqrt(T)) - r * K * np.exp(-r * T) * stats.norm.cdf(d2)
        elif option_type.lower() == 'put':
            return -S * stats.norm.pdf(d1) * sigma / (2 * np.sqrt(T)) + r * K * np.exp(-r * T) * stats.norm.cdf(-d2)
        else:
            raise ValueError(f"Invalid option type: {option_type}. Must be 'call' or 'put'.")
    
    @staticmethod
    def rho(S: float, K: float, T: float, r: float, sigma: float, option_type: str) -> float:
        """Calculate the rho of an option"""
        if T <= 0 or sigma <= 0:
            return 0.0
        
        d2 = BlackScholes._d2(S, K, T, r, sigma)
        
        if option_type.lower() == 'call':
            return K * T * np.exp(-r * T) * stats.norm.cdf(d2)
        elif option_type.lower() == 'put':
            return -K * T * np.exp(-r * T) * stats.norm.cdf(-d2)
        else:
            raise ValueError(f"Invalid option type: {option_type}. Must be 'call' or 'put'.")
    
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
    
    def __init__(self, threshold_stddev: float = 2.0, rolling_window: int = 60, min_samples: int = 5):
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
        
        if len(underlying_data) < self.min_samples:
            logger.warning(f"Not enough data to fit IV surface for {underlying}. Need at least {self.min_samples} samples.")
            return
        
        # Extract features for surface fitting
        X = np.array([(d.moneyness, d.expiration) for d in underlying_data])
        y = np.array([d.implied_volatility for d in underlying_data])
        
        # Remove NaN values
        valid_indices = ~np.isnan(y)
        X = X[valid_indices]
        y = y[valid_indices]
        
        if len(y) < self.min_samples:
            logger.warning(f"Not enough valid IV data for {underlying} after filtering NaNs")
            return
        
        try:
            # Use SmoothBivariateSpline for surface fitting if enough data points,
            # otherwise use a simpler method like griddata interpolation
            if len(X) >= 10:  # Arbitrary threshold, adjust based on testing
                # SmoothBivariateSpline requires at least 3 unique x and y values
                unique_moneyness = len(np.unique(X[:, 0]))
                unique_expiry = len(np.unique(X[:, 1]))
                
                if unique_moneyness >= 3 and unique_expiry >= 3:
                    self.surfaces[underlying] = SmoothBivariateSpline(
                        X[:, 0], X[:, 1], y, kx=min(3, unique_moneyness-1), ky=min(3, unique_expiry-1)
                    )
                else:
                    # If not enough unique values, fall back to griddata
                    self.surfaces[underlying] = (X, y)
            else:
                # For small datasets, use griddata
                self.surfaces[underlying] = (X, y)
                
            # Calculate and store residuals
            predicted_ivs = self._predict_iv(underlying_data, underlying)
            actual_ivs = [d.implied_volatility for d in underlying_data]
            
            # Calculate residuals only for valid predictions and actual values
            valid_indices = [i for i, (pred, actual) in enumerate(zip(predicted_ivs, actual_ivs)) 
                             if not np.isnan(pred) and not np.isnan(actual)]
            
            if len(valid_indices) >= 2:  # Need at least 2 points for std calculation
                residuals = [actual_ivs[i] - predicted_ivs[i] for i in valid_indices]
                mean_residual = np.mean(residuals)
                std_residual = np.std(residuals)
                self.residuals[underlying] = (mean_residual, std_residual)
            else:
                logger.warning(f"Not enough valid residuals for {underlying}")
                self.residuals[underlying] = (0.0, 0.05)  # Default values
                
            # Update last fit time
            self.last_fit_time[underlying] = time.time()
            
            logger.info(f"Successfully fitted IV surface for {underlying} with {len(X)} points")
        
        except Exception as e:
            logger.error(f"Error fitting IV surface for {underlying}: {str(e)}")
            logger.debug(traceback.format_exc())
    
    def _predict_iv(self, data: List[OptionData], underlying: str) -> List[float]:
        """Predict implied volatility using the fitted surface"""
        if underlying not in self.surfaces:
            return [float('nan')] * len(data)
        
        features = np.array([(d.moneyness, d.expiration) for d in data])
        
        # Check for NaN values in features
        valid_features = ~np.isnan(features).any(axis=1)
        if not np.any(valid_features):
            return [float('nan')] * len(data)
        
        # Split the calculation based on surface type
        if isinstance(self.surfaces[underlying], tuple):
            # This is a griddata surface
            X, y = self.surfaces[underlying]
            
            # Initialize predictions with NaNs
            predictions = np.full(len(data), np.nan)
            
            # Only predict for valid features
            valid_indices = np.where(valid_features)[0]
            
            if len(valid_indices) > 0:
                try:
                    valid_predictions = griddata(
                        X, y, features[valid_features], method='linear', rescale=True
                    )
                    predictions[valid_indices] = valid_predictions
                except Exception as e:
                    logger.error(f"Error predicting IV with griddata for {underlying}: {str(e)}")
            
            return predictions.tolist()
        else:
            # This is a SmoothBivariateSpline surface
            try:
                # Initialize predictions with NaNs
                predictions = np.full(len(data), np.nan)
                
                # Only predict for valid features
                valid_indices = np.where(valid_features)[0]
                
                if len(valid_indices) > 0:
                    spline = self.surfaces[underlying]
                    valid_predictions = spline.ev(
                        features[valid_features, 0], features[valid_features, 1]
                    )
                    predictions[valid_indices] = valid_predictions
                
                return predictions.tolist()
            except Exception as e:
                logger.error(f"Error predicting IV with spline for {underlying}: {str(e)}")
                return [float('nan')] * len(data)
    
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
            
            if current_time - last_fit > 300:  # Update every 5 minutes
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
        # Check if we have a surface for this underlying
        underlying = option_data.underlying
        
        if underlying not in self.surfaces or underlying not in self.residuals:
            return None
        
        # Skip if IV is not available
        if option_data.implied_volatility is None or np.isnan(option_data.implied_volatility):
            return None
        
        # Predict IV for this option
        predicted_iv = self._predict_iv([option_data], underlying)[0]
        
        # Skip if prediction failed
        if np.isnan(predicted_iv):
            return None
        
        # Calculate residual
        residual = option_data.implied_volatility - predicted_iv
        
        # Get distribution parameters for this underlying
        mean_residual, std_residual = self.residuals[underlying]
        
        # Calculate Z-score
        if std_residual > 0:
            z_score = (residual - mean_residual) / std_residual
        else:
            z_score = 0
        
        # Check if anomalous
        if abs(z_score) > self.threshold_stddev:
            description = (
                f"IV anomaly detected for {option_data.symbol}. "
                f"Observed IV: {option_data.implied_volatility:.4f}, "
                f"Predicted IV: {predicted_iv:.4f}, "
                f"Z-score: {z_score:.2f}"
            )
            
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
        """Extract feature vector from option data"""
        features = [
            option_data.moneyness,
            option_data.expiration,
            option_data.implied_volatility if option_data.implied_volatility else float('nan'),
            option_data.delta if option_data.delta else float('nan'),
            option_data.gamma if option_data.gamma else float('nan'),
            option_data.theta if option_data.theta else float('nan'),
            option_data.vega if option_data.vega else float('nan'),
            option_data.volume,
            option_data.open_interest,
            (option_data.ask - option_data.bid) / option_data.mid_price  # Relative spread
        ]
        
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
            "relative_spread"
        ]
    
    def fit(self, data: List[OptionData]) -> None:
        """
        Fit Isolation Forest models for each underlying and option type
        
        Args:
            data: List of option data records
        """
        # Group data by underlying and option type
        grouped_data = {}
        for d in data:
            key = (d.underlying, d.option_type)
            if key not in grouped_data:
                grouped_data[key] = []
            grouped_data[key].append(d)
        
        for (underlying, option_type), group_data in grouped_data.items():
            # Check if we need to update the model
            key = (underlying, option_type)
            current_time = time.time()
            last_fit = self.last_fit_time.get(key, 0)
            
            if current_time - last_fit > self.model_update_interval:
                try:
                    # Extract features
                    features_list = []
                    for d in group_data:
                        features = self._extract_features(d)[0]
                        if not np.isnan(features).any():  # Skip records with NaN values
                            features_list.append(features)
                    
                    if len(features_list) < 10:  # Need a minimum number of samples
                        logger.warning(f"Not enough valid data to fit model for {underlying} {option_type}")
                        continue
                    
                    features = np.array(features_list)
                    
                    # Create and fit scaler
                    scaler = StandardScaler()
                    scaled_features = scaler.fit_transform(features)
                    
                    # Create and fit model
                    model = IsolationForest(
                        contamination=self.contamination,
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
                    
                    logger.info(f"Successfully fitted Isolation Forest for {underlying} {option_type} with {len(features)} samples")
                
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
        key = (option_data.underlying, option_data.option_type)
        
        # Check if we have a model for this underlying and option type
        if key not in self.models or key not in self.scalers:
            return None
        
        # Extract features
        features = self._extract_features(option_data)
        
        # Skip if any features are NaN
        if np.isnan(features).any():
            return None
        
        try:
            # Scale features
            scaled_features = self.scalers[key].transform(features)
            
            # Get anomaly score
            # Note: decision_function returns a score where negative values are anomalies
            # and the lower the value, the more anomalous the observation
            raw_score = self.models[key].decision_function(scaled_features)[0]
            
            # Invert score so that higher values indicate more anomalous observations
            anomaly_score = -raw_score
            
            # Check if anomalous (score < 0 in the original scale means anomalous)
            if raw_score < 0:
                # Get feature importances if possible (not directly available in Isolation Forest)
                # We could analyze this further by seeing which features are most anomalous
                # by comparing them to the distribution of normal data
                
                description = (
                    f"Anomaly detected by Isolation Forest for {option_data.symbol}. "
                    f"Anomaly score: {anomaly_score:.4f}"
                )
                
                # Scale confidence based on anomaly score
                # Typically scores are in range [0, 0.5] after inversion
                confidence = min(1.0, anomaly_score / 0.5)
                
                return AnomalyResult(
                    option_data=option_data,
                    anomaly_score=anomaly_score,
                    anomaly_type="isolation_forest",
                    description=description,
                    confidence=confidence
                )
        
        except Exception as e:
            logger.error(f"Error during anomaly detection with Isolation Forest: {str(e)}")
            logger.debug(traceback.format_exc())
        
        return None


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
            return option_data
        
        # Calculate mid price
        mid_price = option_data.mid_price
        
        try:
            # Calculate implied volatility
            iv = BlackScholes.implied_volatility(
                S=option_data.underlying_price,
                K=option_data.strike,
                T=option_data.expiration,
                r=self.risk_free_rate,
                market_price=mid_price,
                option_type=option_data.option_type
            )
            
            # Calculate Greeks
            if not np.isnan(iv):
                delta = BlackScholes.delta(
                    S=option_data.underlying_price,
                    K=option_data.strike,
                    T=option_data.expiration,
                    r=self.risk_free_rate,
                    sigma=iv,
                    option_type=option_data.option_type
                )
                
                gamma = BlackScholes.gamma(
                    S=option_data.underlying_price,
                    K=option_data.strike,
                    T=option_data.expiration,
                    r=self.risk_free_rate,
                    sigma=iv
                )
                
                theta = BlackScholes.theta(
                    S=option_data.underlying_price,
                    K=option_data.strike,
                    T=option_data.expiration,
                    r=self.risk_free_rate,
                    sigma=iv,
                    option_type=option_data.option_type
                )
                
                vega = BlackScholes.vega(
                    S=option_data.underlying_price,
                    K=option_data.strike,
                    T=option_data.expiration,
                    r=self.risk_free_rate,
                    sigma=iv
                )
                
                # Update option data
                option_data.implied_volatility = iv
                option_data.delta = delta
                option_data.gamma = gamma
                option_data.theta = theta
                option_data.vega = vega
        
        except Exception as e:
            logger.error(f"Error processing option data: {str(e)}")
            logger.debug(traceback.format_exc())
        
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
        self.data_processor = DataProcessor()
        self.data_cache = OptionDataCache()
        
        # Create anomaly detectors based on configuration
        detectors = []
        
        if config["anomaly_detection"]["iv_surface"]["enabled"]:
            iv_detector = IVSurfaceAnomalyDetector(
                threshold_stddev=config["anomaly_detection"]["iv_surface"]["threshold_stddev"],
                rolling_window=config["anomaly_detection"]["iv_surface"]["rolling_window"],
                min_samples=config["anomaly_detection"]["iv_surface"]["min_samples"]
            )
            detectors.append(iv_detector)
        
        if config["anomaly_detection"]["isolation_forest"]["enabled"]:
            if_detector = IsolationForestAnomalyDetector(
                contamination=config["anomaly_detection"]["isolation_forest"]["contamination"],
                n_estimators=config["anomaly_detection"]["isolation_forest"]["n_estimators"],
                max_samples=config["anomaly_detection"]["isolation_forest"]["max_samples"],
                random_state=config["anomaly_detection"]["isolation_forest"]["random_state"],
                model_update_interval=config["anomaly_detection"]["isolation_forest"]["model_update_interval"]
            )
            detectors.append(if_detector)
        
        self.detector = CombinedAnomalyDetector(detectors)
        
        # Cache cleaning thread
        self.clean_thread = threading.Thread(target=self._clean_cache_periodically, daemon=True)
        self.running = True
        self.clean_thread.start()
        
        logger.info("Anomaly detection pipeline initialized")
    
    def _clean_cache_periodically(self) -> None:
        """Periodically clean the data cache"""
        while self.running:
            try:
                self.data_cache.clean()
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
            
            # Periodically fit/update anomaly detection models
            if time.time() % 300 < 1:  # Every 5 minutes approximately
                logger.info("Fitting anomaly detection models")
                all_data = self.data_cache.get_all()
                self.detector.fit(all_data)
            
            # Detect anomalies
            anomaly_result = self.detector.detect(processed_data)
            
            return processed_data, anomaly_result
        
        except Exception as e:
            logger.error(f"Error in process_data: {str(e)}")
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
    
    def _worker_thread(self) -> None:
        """Worker thread for processing tasks"""
        while self.running:
            try:
                task = self.tasks.get(timeout=1)
                if task is None:  # Sentinel value for shutdown
                    break
                
                option_data = task
                processed_data, anomaly_result = self.pipeline.process_data(option_data)
                
                if anomaly_result:
                    logger.info(f"Anomaly detected: {anomaly_result.description}")
                    self.producer.produce(
                        key=processed_data.symbol,
                        value=anomaly_result.to_dict()
                    )
            
            except queue.Empty:
                continue
            
            except Exception as e:
                logger.error(f"Error in worker thread: {str(e)}")
                logger.debug(traceback.format_exc())
            
            finally:
                self.tasks.task_done()
    
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
    Create a simple REST API for the dashboard using Flask
    
    Args:
        anomaly_detection_service: Anomaly detection service instance
    """
    from flask import Flask, jsonify, request
    
    app = Flask("options_anomaly_detection")
    
    @app.route('/api/anomalies', methods=['GET'])
    def get_anomalies():
        """Get recent anomalies"""
        # In a real implementation, this would retrieve anomalies from a database
        # For now, return a placeholder
        return jsonify({"anomalies": []})
    
    @app.route('/api/stats', methods=['GET'])
    def get_stats():
        """Get system statistics"""
        # In a real implementation, this would retrieve stats about the system
        return jsonify({
            "processed_options": 0,
            "detected_anomalies": 0,
            "uptime": 0
        })
    
    @app.route('/api/config', methods=['GET'])
    def get_config():
        """Get current configuration"""
        return jsonify(anomaly_detection_service.config)
    
    @app.route('/api/config', methods=['POST'])
    def update_config():
        """Update configuration"""
        new_config = request.json
        # In a real implementation, this would update the configuration
        return jsonify({"success": True})
    
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
