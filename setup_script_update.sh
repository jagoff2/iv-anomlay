#!/bin/bash

# The following lines should be added to the existing setup_script.sh file
# This script adds the yfinance data feed to the Options Pricing Anomaly Detection System

echo "Setting up YFinance Options Data Feed..."

# Create yfinance data feed config
cat > config/yfinance_config.json << EOF
{
  "kafka": {
    "bootstrap_servers": "${KAFKA_SERVERS}",
    "topic": "${INPUT_TOPIC}",
    "batch_size": 100,
    "queue_buffer_size": 10000,
    "linger_ms": 100,
    "compression_type": "lz4"
  },
  "yfinance": {
    "tickers": ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "SPY", "QQQ", "IWM"],
    "fetch_interval": 60,
    "max_expiry_days": 90,
    "risk_free_rate": ${RISK_FREE_RATE:-0.05}
  },
  "general": {
    "debug": false,
    "num_workers": 4
  }
}
EOF

# Install required packages for yfinance data feed
echo "Installing YFinance and dependencies..."
pip install yfinance pandas numpy

# Add yfinance service to docker-compose.yml
echo "Adding YFinance service to Docker Compose configuration..."

# This is the content to be appended to docker-compose.yml
cat >> docker-compose.yml << EOF

  # YFinance Data Feed Service
  yfinance-data-feed:
    build:
      context: .
      dockerfile: Dockerfile
    image: options-anomaly-detection/yfinance-feed
    container_name: yfinance-data-feed
    volumes:
      - ./config:/app/config
      - ./logs:/app/logs
    command: python3 yfinance_data_feed.py --config config/yfinance_config.json
    restart: unless-stopped
    depends_on:
      - redpanda
EOF

# Create or update requirements.txt to include yfinance
echo "Updating requirements.txt to include YFinance..."
if [ -f "requirements.txt" ]; then
    if ! grep -q "yfinance" requirements.txt; then
        echo "yfinance==0.2.55" >> requirements.txt
    fi
else
    echo "yfinance==0.2.55" > requirements.txt
fi

echo "YFinance data feed setup complete!"
echo 
echo "To use the YFinance data feed instead of the simulator, update your docker-compose.yml:"
echo "1. Comment out or remove the 'options-simulator' service"
echo "2. Keep the 'yfinance-data-feed' service that was just added"
echo 
echo "Alternatively, you can use both to supplement the simulated data with real market data."
echo 
