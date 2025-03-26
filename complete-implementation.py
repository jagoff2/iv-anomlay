"""
Complete Anomaly Detection System Implementation with fixes for:
1. Thread queue management
2. Statistics tracking
3. Anomaly storage
4. REST API endpoints
5. Configuration management
"""

# Add at the top of the file after your imports
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
    
    def __init__(self, max_stored_anomalies: int = 1000):
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


# Fixed worker thread implementation for AnomalyDetectionService
def _worker_thread(self) -> None:
    """Worker thread for processing tasks"""
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
                break
            
            # Process the task (option data)
            option_data = task
            processed_data, anomaly_result = self.pipeline.process_data(option_data)
            
            # Track that we processed an option
            anomaly_system.increment_processed()
            
            if anomaly_result:
                logger.info(f"Anomaly detected: {anomaly_result.description}")
                
                # Add to anomaly system
                anomaly_system.add_anomaly(anomaly_result.to_dict())
                
                # Publish to Kafka
                self.producer.produce(
                    key=processed_data.symbol,
                    value=anomaly_result.to_dict()
                )
        
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


# Complete REST API implementation
def create_rest_api(anomaly_detection_service):
    """
    Create a REST API for the dashboard using Flask
    
    Args:
        anomaly_detection_service: Anomaly detection service instance
    """
    from flask import Flask, jsonify, request
    
    app = Flask("options_anomaly_detection")
    
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


# Add this method to the AnomalyDetectionService class
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


# Fixed worker thread implementation for YFinanceDataFeed
def _worker_thread_yfinance(self):
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


# Fixed version of _process_option_row for YFinanceOptionsFetcher
def _process_option_row(self, ticker_symbol: str, underlying_price: float, 
                       exp_date: str, option_type: str, row: pd.Series) -> Optional[Dict]:
    """
    Process an option row from the option chain
    
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
        contract_symbol = row.get('contractSymbol', f"{ticker_symbol}{exp_date.replace('-', '')}{option_type[0].upper()}{int(row['strike']):05d}")
        
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
        
        # Make sure bid and ask are valid
        bid = float(row['bid']) if not pd.isna(row['bid']) else 0.0
        ask = float(row['ask']) if not pd.isna(row['ask']) else 0.0
        
        # Make sure implied volatility is valid
        implied_volatility = row['impliedVolatility']
        if pd.isna(implied_volatility):
            # Use a reasonable default based on option type and moneyness
            moneyness = float(row['strike']) / underlying_price
            if option_type == 'call':
                if moneyness < 0.9:  # ITM
                    implied_volatility = 0.3
                elif moneyness > 1.1:  # OTM
                    implied_volatility = 0.5
                else:  # ATM
                    implied_volatility = 0.4
            else:  # Put
                if moneyness > 1.1:  # ITM
                    implied_volatility = 0.3
                elif moneyness < 0.9:  # OTM
                    implied_volatility = 0.5
                else:  # ATM
                    implied_volatility = 0.4
        else:
            implied_volatility = float(implied_volatility)
        
        # Convert to our format
        option_data = {
            "timestamp": time.time(),
            "symbol": contract_symbol,
            "underlying": ticker_symbol,
            "option_type": option_type,
            "strike": float(row['strike']),
            "expiration": t_expiry,
            "bid": bid,
            "ask": ask,
            "volume": volume,
            "open_interest": open_interest,
            "implied_volatility": implied_volatility,
            "underlying_price": underlying_price
        }
        
        return option_data
    
    except Exception as e:
        logger.warning(f"Error processing option row: {str(e)}")
        logger.debug(f"Row data: {row}")
        logger.debug(traceback.format_exc())
        return None
