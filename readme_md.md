# Options Pricing Anomaly Detection System

A production-ready implementation of an automated alert system for detecting options pricing anomalies. This system combines two complementary approaches:

1. **Implied Volatility (IV) Surface Analysis**: Detects deviations from a fitted IV surface to identify mispriced options.
2. **Unsupervised Machine Learning**: Uses Isolation Forest to detect anomalies based on multiple features of options data.

This system is especially useful for options traders dealing with short-dated (0DTE) contracts who want to be alerted when pricing irregularities arise.

## Architecture Overview

![Architecture Diagram](docs/architecture.png)

### Components

- **Data Ingestion**: Real-time options data is streamed through Redpanda (Kafka-compatible) from market data providers.
- **Data Processing**: Each option is processed to calculate implied volatility and Greeks.
- **Anomaly Detection**: Two-layer approach combining IV surface analysis and Isolation Forest.
- **Alerts & Visualization**: Real-time dashboard showing detected anomalies and statistics.

## Features

- **Real-time Processing**: Stream-based architecture for immediate analysis of market data.
- **Dual-Layer Detection**: Higher confidence through complementary approaches to anomaly detection.
- **Production Ready**: Dockerized, scalable, and built with industry best practices.
- **Interactive Dashboard**: Visualize anomalies, filter by various criteria, and monitor system performance.
- **Configurable**: Easy configuration through JSON files and environment variables.

## Prerequisites

- Docker and Docker Compose
- At least 4GB of RAM
- Internet connection (for pulling Docker images)

## Quick Start

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/options-anomaly-detection.git
   cd options-anomaly-detection
   ```

2. Start the system with Docker Compose:
   ```bash
   docker-compose up -d
   ```

3. Access the dashboard at http://localhost:8050

4. View Redpanda Console at http://localhost:8080 to inspect the data streams

## Configuration

The system can be configured through the `config/config.json` file. Key configuration options include:

- **Kafka Settings**: Bootstrap servers, topics, and consumer group ID.
- **Anomaly Detection Parameters**: Threshold values, model parameters, and feature weights.
- **Dashboard Settings**: Update intervals and API port.
- **General Settings**: Debug mode and number of worker threads.

Example configuration:
```json
{
  "kafka": {
    "bootstrap_servers": "redpanda:9092",
    "group_id": "options_anomaly_detection",
    "input_topic": "options_data",
    "output_topic": "options_anomalies"
  },
  "anomaly_detection": {
    "iv_surface": {
      "enabled": true,
      "threshold_stddev": 2.5
    },
    "isolation_forest": {
      "enabled": true,
      "contamination": 0.01
    }
  }
}
```

## Data Simulation for Testing

The system includes a data simulator for testing purposes. The simulator generates realistic options data with occasional anomalies to test the detection system:

```bash
docker-compose up options-simulator
```

You can configure the simulator with these parameters:
- `--rate`: Number of options to generate per second
- `--anomaly_rate`: Probability of generating anomalous options (0-1)

## Components in Detail

### 1. Data Ingestion & Preprocessing

Options data is ingested through Kafka/Redpanda and preprocessed to calculate:
- Implied volatility using the Black-Scholes model
- Option Greeks (delta, gamma, theta, vega)
- Additional derived features for anomaly detection

### 2. Implied Volatility Surface Analysis

- Fits a continuous IV surface using regression/interpolation methods
- Calculates residuals between observed and predicted IVs
- Flags options where residuals exceed a statistical threshold

### 3. Isolation Forest Anomaly Detection

- Constructs feature vectors from options data
- Detects anomalies based on how easily points can be "isolated"
- Automatically adapts to changing market conditions

### 4. Alert System & Dashboard

- Real-time alerts for detected anomalies
- Interactive dashboard for visualization and analysis
- Filtering and sorting capabilities

## Production Deployment

For production deployment, consider:

1. Setting up proper monitoring and logging using tools like Prometheus and Grafana
2. Implementing authentication for the dashboard
3. Using a load balancer for high availability
4. Increasing the number of Redpanda nodes for scalability
5. Implementing a proper backup strategy for data persistence

## Extending the System

You can extend the system by:

1. Adding new anomaly detection algorithms
2. Integrating with trading systems for automated response
3. Implementing custom alerting mechanisms (email, SMS, etc.)
4. Adding machine learning models that learn from trader feedback

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Based on research in options pricing anomalies and volatility surface analysis
- Uses open-source technologies including Python, Redpanda, Dash, and scikit-learn
