"""
American Options Pricing Module

Implements pricing models for American options using binomial tree and approximation methods.
Can be used as a drop-in replacement for the current Black-Scholes implementation.
"""

import numpy as np
import math
from scipy import stats, optimize
from typing import Dict, List, Tuple, Optional, Union, Callable


class AmericanOptionPricing:
    """American option pricing implementation using binomial tree and approximation methods"""
    
    @staticmethod
    def binomial_tree_price(S: float, K: float, T: float, r: float, sigma: float, 
                           option_type: str, dividend_yield: float = 0.0, steps: int = 100) -> float:
        """
        Price American options using binomial tree model
        
        Args:
            S: Underlying price
            K: Strike price
            T: Time to expiration in years
            r: Risk-free rate
            sigma: Volatility
            option_type: 'call' or 'put'
            dividend_yield: Continuous dividend yield
            steps: Number of steps in the tree (higher = more accurate but slower)
            
        Returns:
            Option price
        """
        # Handle edge cases
        if T <= 0:
            # Time to expiration is zero or negative
            if option_type.lower() == 'call':
                return max(0.0, S - K)
            else:
                return max(0.0, K - S)
        
        if sigma <= 0:
            # Volatility is zero or negative
            if option_type.lower() == 'call':
                return max(0.0, S - K * np.exp(-r * T))
            else:
                return max(0.0, K * np.exp(-r * T) - S)
        
        # Adjust for dividend yield
        q = dividend_yield
        
        # Tree parameters
        dt = T / steps
        u = np.exp(sigma * np.sqrt(dt))
        d = 1 / u
        a = np.exp((r - q) * dt)
        p = (a - d) / (u - d)
        
        # Initialize asset prices at maturity (final step)
        prices = np.zeros(steps + 1)
        for i in range(steps + 1):
            prices[i] = S * (u ** (steps - i)) * (d ** i)
        
        # Initialize option values at maturity
        if option_type.lower() == 'call':
            option_values = np.maximum(0, prices - K)
        else:
            option_values = np.maximum(0, K - prices)
        
        # Backward induction through the tree
        discount = np.exp(-r * dt)
        for step in range(steps - 1, -1, -1):
            # Calculate continuation value at each node
            for i in range(step + 1):
                # Price at this node
                S_node = S * (u ** (step - i)) * (d ** i)
                
                # Continuation value (discounted expected value)
                continuation = discount * (p * option_values[i] + (1 - p) * option_values[i + 1])
                
                # For American options, compare with immediate exercise value
                if option_type.lower() == 'call':
                    exercise = max(0, S_node - K)
                else:
                    exercise = max(0, K - S_node)
                
                # Option value is maximum of continuation and exercise
                option_values[i] = max(continuation, exercise)
        
        return option_values[0]
    
    @staticmethod
    def barone_adesi_whaley_approximation(S: float, K: float, T: float, r: float, 
                                         sigma: float, option_type: str, dividend_yield: float = 0.0) -> float:
        """
        Price American options using Barone-Adesi and Whaley approximation
        
        This method is faster than binomial tree but still accurate for American options.
        
        Args:
            S: Underlying price
            K: Strike price
            T: Time to expiration in years
            r: Risk-free rate
            sigma: Volatility
            option_type: 'call' or 'put'
            dividend_yield: Continuous dividend yield
            
        Returns:
            Option price
        """
        # Handle edge cases
        if T <= 0:
            # Time to expiration is zero or negative
            if option_type.lower() == 'call':
                return max(0.0, S - K)
            else:
                return max(0.0, K - S)
        
        if sigma <= 0:
            # Volatility is zero or negative
            if option_type.lower() == 'call':
                return max(0.0, S - K * np.exp(-r * T))
            else:
                return max(0.0, K * np.exp(-r * T) - S)
        
        # Adjust for dividend yield
        q = dividend_yield
        
        # First, get the European option price
        d1 = (np.log(S / K) + (r - q + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        
        if option_type.lower() == 'call':
            european_price = S * np.exp(-q * T) * stats.norm.cdf(d1) - K * np.exp(-r * T) * stats.norm.cdf(d2)
            
            # American call with no dividends equals European call
            if q <= 0.0001:  # effectively zero dividends
                return european_price
        else:  # Put option
            european_price = K * np.exp(-r * T) * stats.norm.cdf(-d2) - S * np.exp(-q * T) * stats.norm.cdf(-d1)
        
        # Compute the early exercise premium
        b = r - q
        beta_inside = (0.5 - b/(sigma**2)) + np.sqrt((b/(sigma**2) - 0.5)**2 + 2*r/(sigma**2))
        if option_type.lower() == 'call':
            beta = beta_inside
        else:
            beta = -beta_inside
        
        # Compute critical price
        if option_type.lower() == 'call':
            # The critical price above which it's optimal to exercise
            def call_critical_equation(S_critical):
                d1_crit = (np.log(S_critical / K) + (r - q + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
                return S_critical - K - (S_critical - K * np.exp(-r * T) * stats.norm.cdf(d1_crit - sigma * np.sqrt(T))) - \
                       (1 - np.exp(-q * T) * stats.norm.cdf(d1_crit)) * S_critical / beta
            
            try:
                S_critical = optimize.newton(call_critical_equation, K, maxiter=100)
            except:
                S_critical = 2 * K  # Default approximation
            
            # Compute the coefficient
            d1_crit = (np.log(S_critical / K) + (r - q + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
            A2 = (1 - np.exp(-q * T) * stats.norm.cdf(d1_crit)) * S_critical / beta
            
            # If S < S_critical, compute the early exercise premium
            if S < S_critical:
                early_exercise = A2 * (S / S_critical)**beta
                return european_price + early_exercise
            else:
                return S - K
            
        else:  # Put option
            # The critical price below which it's optimal to exercise
            def put_critical_equation(S_critical):
                d1_crit = (np.log(S_critical / K) + (r - q + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
                return K - S_critical - (K * np.exp(-r * T) * stats.norm.cdf(-(d1_crit - sigma * np.sqrt(T))) - S_critical * np.exp(-q * T) * stats.norm.cdf(-d1_crit)) - \
                       (1 - np.exp(-q * T) * stats.norm.cdf(-d1_crit)) * S_critical / beta
            
            try:
                S_critical = optimize.newton(put_critical_equation, K, maxiter=100)
            except:
                S_critical = K / 2  # Default approximation
            
            # Compute the coefficient
            d1_crit = (np.log(S_critical / K) + (r - q + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
            A1 = -(1 - np.exp(-q * T) * stats.norm.cdf(-d1_crit)) * S_critical / beta
            
            # If S > S_critical, compute the early exercise premium
            if S > S_critical:
                early_exercise = A1 * (S / S_critical)**beta
                return european_price + early_exercise
            else:
                return K - S
    
    @staticmethod
    def option_price(S: float, K: float, T: float, r: float, sigma: float, 
                    option_type: str, dividend_yield: float = 0.0, method: str = 'binomial') -> float:
        """
        Calculate option price based on type and method
        
        Args:
            S: Underlying price
            K: Strike price
            T: Time to expiration in years
            r: Risk-free rate
            sigma: Volatility
            option_type: 'call' or 'put'
            dividend_yield: Continuous dividend yield
            method: 'binomial' or 'barone_adesi_whaley'
            
        Returns:
            Option price
        """
        if method == 'binomial':
            return AmericanOptionPricing.binomial_tree_price(
                S, K, T, r, sigma, option_type, dividend_yield
            )
        elif method == 'barone_adesi_whaley':
            return AmericanOptionPricing.barone_adesi_whaley_approximation(
                S, K, T, r, sigma, option_type, dividend_yield
            )
        else:
            raise ValueError(f"Unknown method: {method}. Use 'binomial' or 'barone_adesi_whaley'")
    
    @staticmethod
    def implied_volatility(S: float, K: float, T: float, r: float, market_price: float, 
                          option_type: str, dividend_yield: float = 0.0, 
                          method: str = 'binomial', max_iterations: int = 100) -> float:
        """
        Calculate implied volatility for American options
        
        Args:
            S: Underlying price
            K: Strike price
            T: Time to expiration in years
            r: Risk-free rate
            market_price: Observed market price of the option
            option_type: 'call' or 'put'
            dividend_yield: Continuous dividend yield
            method: 'binomial' or 'barone_adesi_whaley'
            max_iterations: Maximum number of iterations for numerical solver
            
        Returns:
            Implied volatility or NaN if calculation fails
        """
        if T <= 0:
            return float('nan')
        
        # Handle boundary conditions
        # For calls, price must be between max(0, S - K*exp(-r*T)) and S
        # For puts, price must be between max(0, K*exp(-r*T) - S) and K
        if option_type.lower() == 'call':
            lower_bound = max(0, S - K * np.exp(-r * T))
            upper_bound = S
            
            if market_price < lower_bound or market_price > upper_bound:
                return float('nan')
                
        else:  # Put
            lower_bound = max(0, K * np.exp(-r * T) - S)
            upper_bound = K
            
            if market_price < lower_bound or market_price > upper_bound:
                return float('nan')
        
        # Define objective function
        def objective_function(sigma):
            price = AmericanOptionPricing.option_price(
                S, K, T, r, sigma, option_type, dividend_yield, method
            )
            return price - market_price
        
        # Initial guess based on moneyness and time to expiration
        moneyness = K / S
        if moneyness < 0.8:  # Deep ITM
            initial_guess = 0.4
        elif moneyness > 1.2:  # Deep OTM
            initial_guess = 0.3
        else:  # Near the money
            initial_guess = 0.25
        
        try:
            # Try Brent's method first (bracketing method)
            implied_vol = optimize.brentq(
                objective_function, 0.001, 2.0, maxiter=max_iterations, xtol=1e-6
            )
            return implied_vol
        except:
            # If Brent's method fails, try Newton-Raphson with numerical derivative
            try:
                implied_vol = optimize.newton(
                    objective_function, initial_guess, maxiter=max_iterations, tol=1e-6
                )
                return implied_vol
            except:
                # If all methods fail, use bisection (slower but more robust)
                try:
                    low, high = 0.001, 2.0
                    for _ in range(max_iterations):
                        mid = (low + high) / 2
                        mid_price = AmericanOptionPricing.option_price(
                            S, K, T, r, mid, option_type, dividend_yield, method
                        )
                        
                        if abs(mid_price - market_price) < 1e-6:
                            return mid
                        
                        if mid_price > market_price:
                            high = mid
                        else:
                            low = mid
                            
                    return (low + high) / 2
                except:
                    return float('nan')
    
    @staticmethod
    def vega(S: float, K: float, T: float, r: float, sigma: float, 
            option_type: str, dividend_yield: float = 0.0, method: str = 'binomial') -> float:
        """
        Calculate vega (sensitivity to volatility changes) for American options
        
        Since vega is not directly available for tree methods, use finite difference approximation
        
        Args:
            S: Underlying price
            K: Strike price
            T: Time to expiration in years
            r: Risk-free rate
            sigma: Volatility
            option_type: 'call' or 'put'
            dividend_yield: Continuous dividend yield
            method: 'binomial' or 'barone_adesi_whaley'
            
        Returns:
            Vega value
        """
        if T <= 0 or sigma <= 0:
            return 0.0
        
        # Small change in volatility for finite difference
        d_sigma = 0.001
        
        # Calculate prices at sigma +/- d_sigma
        price_up = AmericanOptionPricing.option_price(
            S, K, T, r, sigma + d_sigma, option_type, dividend_yield, method
        )
        
        price_down = AmericanOptionPricing.option_price(
            S, K, T, r, sigma - d_sigma, option_type, dividend_yield, method
        )
        
        # Central difference approximation of vega
        vega = (price_up - price_down) / (2 * d_sigma)
        
        return vega
    
    @staticmethod
    def delta(S: float, K: float, T: float, r: float, sigma: float, 
             option_type: str, dividend_yield: float = 0.0, method: str = 'binomial') -> float:
        """
        Calculate delta (sensitivity to underlying price changes) for American options
        
        Args:
            S: Underlying price
            K: Strike price
            T: Time to expiration in years
            r: Risk-free rate
            sigma: Volatility
            option_type: 'call' or 'put'
            dividend_yield: Continuous dividend yield
            method: 'binomial' or 'barone_adesi_whaley'
            
        Returns:
            Delta value
        """
        if T <= 0 or sigma <= 0:
            if option_type.lower() == 'call':
                return 1.0 if S > K else 0.0
            else:
                return -1.0 if S < K else 0.0
        
        # Small change in price for finite difference
        d_S = S * 0.001
        
        # Calculate prices at S +/- d_S
        price_up = AmericanOptionPricing.option_price(
            S + d_S, K, T, r, sigma, option_type, dividend_yield, method
        )
        
        price_down = AmericanOptionPricing.option_price(
            S - d_S, K, T, r, sigma, option_type, dividend_yield, method
        )
        
        # Central difference approximation of delta
        delta = (price_up - price_down) / (2 * d_S)
        
        return delta
    
    @staticmethod
    def gamma(S: float, K: float, T: float, r: float, sigma: float, 
             option_type: str, dividend_yield: float = 0.0, method: str = 'binomial') -> float:
        """
        Calculate gamma (second derivative with respect to underlying price) for American options
        
        Args:
            S: Underlying price
            K: Strike price
            T: Time to expiration in years
            r: Risk-free rate
            sigma: Volatility
            option_type: 'call' or 'put'
            dividend_yield: Continuous dividend yield
            method: 'binomial' or 'barone_adesi_whaley'
            
        Returns:
            Gamma value
        """
        if T <= 0 or sigma <= 0:
            return 0.0
        
        # Small change in price for finite difference
        d_S = S * 0.001
        
        # Calculate deltas at S +/- d_S
        delta_up = AmericanOptionPricing.delta(
            S + d_S, K, T, r, sigma, option_type, dividend_yield, method
        )
        
        delta_down = AmericanOptionPricing.delta(
            S - d_S, K, T, r, sigma, option_type, dividend_yield, method
        )
        
        # Central difference approximation of gamma
        gamma = (delta_up - delta_down) / (2 * d_S)
        
        return gamma
    
    @staticmethod
    def theta(S: float, K: float, T: float, r: float, sigma: float, 
             option_type: str, dividend_yield: float = 0.0, method: str = 'binomial') -> float:
        """
        Calculate theta (sensitivity to time changes) for American options
        
        Args:
            S: Underlying price
            K: Strike price
            T: Time to expiration in years
            r: Risk-free rate
            sigma: Volatility
            option_type: 'call' or 'put'
            dividend_yield: Continuous dividend yield
            method: 'binomial' or 'barone_adesi_whaley'
            
        Returns:
            Theta value (per year)
        """
        if T <= 0.01 or sigma <= 0:
            return 0.0
        
        # Small change in time for finite difference
        d_T = min(0.001, T / 10)
        
        # Calculate price at current T and T - d_T
        price_now = AmericanOptionPricing.option_price(
            S, K, T, r, sigma, option_type, dividend_yield, method
        )
        
        price_earlier = AmericanOptionPricing.option_price(
            S, K, T - d_T, r, sigma, option_type, dividend_yield, method
        )
        
        # Forward difference approximation of theta (negative of derivative)
        theta = (price_earlier - price_now) / d_T
        
        return theta
    
    @staticmethod
    def rho(S: float, K: float, T: float, r: float, sigma: float, 
           option_type: str, dividend_yield: float = 0.0, method: str = 'binomial') -> float:
        """
        Calculate rho (sensitivity to interest rate changes) for American options
        
        Args:
            S: Underlying price
            K: Strike price
            T: Time to expiration in years
            r: Risk-free rate
            sigma: Volatility
            option_type: 'call' or 'put'
            dividend_yield: Continuous dividend yield
            method: 'binomial' or 'barone_adesi_whaley'
            
        Returns:
            Rho value
        """
        if T <= 0 or sigma <= 0:
            return 0.0
        
        # Small change in rate for finite difference
        d_r = 0.0001
        
        # Calculate prices at r +/- d_r
        price_up = AmericanOptionPricing.option_price(
            S, K, T, r + d_r, sigma, option_type, dividend_yield, method
        )
        
        price_down = AmericanOptionPricing.option_price(
            S, K, T, r - d_r, sigma, option_type, dividend_yield, method
        )
        
        # Central difference approximation of rho
        rho = (price_up - price_down) / (2 * d_r)
        
        return rho
