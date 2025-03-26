#!/bin/bash

# Options Pricing Anomaly Detection System Setup Script
# This script sets up the environment and starts the system

# Display ASCII art header
echo "============================================================"
echo "  ____        _   _                                         "
echo " / __ \      | | (_)                                        "
echo "| |  | |_ __ | |_ _  ___  _ __  ___                         "
echo "| |  | | '_ \| __| |/ _ \| '_ \/ __|                        "
echo "| |__| | |_) | |_| | (_) | | | \__ \                        "
echo " \____/| .__/ \__|_|\___/|_| |_|___/                        "
echo "       | |                                                  "
echo "       |_|   Anomaly Detection System                       "
echo "============================================================"
echo "Setting up the Options Pricing Anomaly Detection System..."
echo ""

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Docker is not installed. Please install Docker and try again."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "Docker Compose is not installed. Please install Docker Compose and try again."
    exit 1
fi

# Create necessary directories
echo "Creating necessary directories..."
mkdir -p config logs data

# Copy default configuration if it doesn't exist
if [ ! -f config/config.json ]; then
    echo "Copying default configuration..."
    cp config.default.json config/config.json
    echo "Default configuration copied to config/config.json"
    echo "You can modify this file to customize the system"
fi

# Check available resources
echo "Checking system resources..."
MEM_TOTAL=$(free -m | awk '/^Mem:/{print $2}')
CPU_COUNT=$(nproc)

echo "Available memory: ${MEM_TOTAL}MB"
echo "Available CPU cores: ${CPU_COUNT}"

if [ "$MEM_TOTAL" -lt 2048 ]; then
    echo "Warning: System has less than 2GB of RAM. Performance may be affected."
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Set up environment variables for Docker Compose
export COMPOSE_PROJECT_NAME=options_anomaly_detection

# Prompt user for configuration
echo 
echo "System Configuration"
echo "-------------------"
echo "Please enter the following configuration values (or press Enter for defaults):"
echo 

# Ask for Redpanda/Kafka configuration
read -p "Redpanda/Kafka bootstrap servers [redpanda:9092]: " KAFKA_SERVERS
KAFKA_SERVERS=${KAFKA_SERVERS:-redpanda:9092}

# Ask for topics
read -p "Input topic for options data [options_data]: " INPUT_TOPIC
INPUT_TOPIC=${INPUT_TOPIC:-options_data}

read -p "Output topic for anomalies [options_anomalies]: " OUTPUT_TOPIC
OUTPUT_TOPIC=${OUTPUT_TOPIC:-options_anomalies}

# Ask for dashboard port
read -p "Dashboard port [8050]: " DASHBOARD_PORT
DASHBOARD_PORT=${DASHBOARD_PORT:-8050}

# Ask for API port
read -p "API port [8000]: " API_PORT
API_PORT=${API_PORT:-8000}

# Ask for anomaly detection parameters
read -p "IV surface anomaly threshold (standard deviations) [2.5]: " IV_THRESHOLD
IV_THRESHOLD=${IV_THRESHOLD:-2.5}

read -p "Isolation Forest contamination (expected ratio of anomalies) [0.02]: " IF_CONTAMINATION
IF_CONTAMINATION=${IF_CONTAMINATION:-0.02}

# Update configuration
echo "Updating configuration..."
cat > config/config.json << EOF
{
  "kafka": {
    "bootstrap_servers": "${KAFKA_SERVERS}",
    "group_id": "options_anomaly_detection",
    "input_topic": "${INPUT_TOPIC}",
    "output_topic": "${OUTPUT_TOPIC}",
    "consumer_poll_timeout": 1.0
  },
  "anomaly_detection": {
    "iv_surface": {
      "enabled": true,
      "threshold_stddev": ${IV_THRESHOLD},
      "rolling_window": 120,
      "min_samples": 10
    },
    "isolation_forest": {
      "enabled": true,
      "contamination": ${IF_CONTAMINATION},
      "n_estimators": 100,
      "max_samples": "auto",
      "random_state": 42,
      "model_update_interval": 30
    }
  },
  "dashboard": {
    "update_interval": 5,
    "api_port": ${API_PORT}
  },
  "general": {
    "debug": false,
    "num_workers": 4
  }
}
EOF

# Update ports in docker-compose.yml
echo "Updating Docker Compose configuration..."
sed -i "s/- \"8050:8050\"/- \"${DASHBOARD_PORT}:8050\"/" docker-compose.yml
sed -i "s/- \"8000:8000\"/- \"${API_PORT}:8000\"/" docker-compose.yml

# Pull Docker images
echo "Pulling Docker images (this may take a while)..."
docker-compose pull

# Build custom images
echo "Building custom images..."
docker-compose build

# Start the system
echo 
echo "Setup complete! Starting the system..."
echo 
docker-compose up -d

# Wait for services to start
echo "Waiting for services to start..."
sleep 10

# Check if services are running
if docker-compose ps | grep -q "Up"; then
    echo 
    echo "Success! The Options Pricing Anomaly Detection System is now running."
    echo 
    echo "You can access the following services:"
    echo "- Dashboard: http://localhost:${DASHBOARD_PORT}"
    echo "- API: http://localhost:${API_PORT}"
    echo "- Redpanda Console: http://localhost:8080"
    echo 
    echo "To view logs:"
    echo "docker-compose logs -f"
    echo 
    echo "To stop the system:"
    echo "docker-compose down"
    echo 
else
    echo "Error: Some services failed to start. Please check the logs with 'docker-compose logs'."
    exit 1
fi
