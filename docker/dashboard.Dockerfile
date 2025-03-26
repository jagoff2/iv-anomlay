FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements-dashboard.txt requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY dashboard.py .

# Set Python to unbuffered mode
ENV PYTHONUNBUFFERED=1

# Expose the dashboard port
EXPOSE 8050

# Run the application
CMD ["python", "dashboard.py"]
