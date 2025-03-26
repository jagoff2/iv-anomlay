"""
Integration module for American options pricing

This module allows the AmericanOptionPricing class to be used as a drop-in
replacement for the BlackScholes class in the existing codebase.
"""

import numpy as np
from typing import Optional
from american_options_pricing import AmericanOptionPricing

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
    def implied_volatility(S: float, K: float, T: float, r: float, market_price: float, option_type: str, dividend_yield: float = 0.0) -> float:
        """Calculate implied volatility for American options"""
        return AmericanOptionPricing.implied_volatility(
            S=S, K=K, T=T, r=r, market_price=market_price, 
            option_type=option_type, dividend_yield=dividend_yield,
            method='binomial'
        )


# Similarly update the BlackScholesMerton class for the simulator
class BlackScholesMerton:
    """Merton's version of Black-Scholes with dividends (for the simulator)"""
    
    @staticmethod
    def d1(S: float, K: float, T: float, r: float, sigma: float, q: float = 0.0) -> float:
        """Calculate d1 component with dividend yield"""
        return (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    
    @staticmethod
    def d2(S: float, K: float, T: float, r: float, sigma: float, q: float = 0.0) -> float:
        """Calculate d2 component with dividend yield"""
        return BlackScholesMerton.d1(S, K, T, r, sigma, q) - sigma * np.sqrt(T)
    
    @staticmethod
    def call_price(S: float, K: float, T: float, r: float, sigma: float, q: float = 0.0) -> float:
        """Calculate the price of an American call option with dividend yield"""
        return AmericanOptionPricing.option_price(
            S=S, K=K, T=T, r=r, sigma=sigma, 
            option_type='call', dividend_yield=q,
            method='binomial'
        )
    
    @staticmethod
    def put_price(S: float, K: float, T: float, r: float, sigma: float, q: float = 0.0) -> float:
        """Calculate the price of an American put option with dividend yield"""
        return AmericanOptionPricing.option_price(
            S=S, K=K, T=T, r=r, sigma=sigma, 
            option_type='put', dividend_yield=q,
            method='binomial'
        )
