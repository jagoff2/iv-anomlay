FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements-main.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements-main.txt

# Copy application files
COPY anomaly_detection.py .
COPY american_options_pricing.py .
COPY config ./config

# Set Python to unbuffered mode to see logs immediately
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["python", "anomaly_detection.py"]
