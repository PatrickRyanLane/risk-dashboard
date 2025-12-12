# Start with Python 3.11 slim image (smaller, faster)
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Install system dependencies needed for Python packages
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libxml2-dev \
    libxslt-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file first (Docker layer caching optimization)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Flask and gunicorn for the web app
RUN pip install flask gunicorn

# Copy all application files to container
COPY . .

# Expose port 8080 (Cloud Run default)
EXPOSE 8080

# Set environment variable for the port
ENV PORT=8080

# Command to run when container starts
# Use gunicorn to run the Flask app (production-ready)
# --bind :$PORT - Listen on the port Cloud Run assigns
# --workers 1 - One worker process (Cloud Run scales containers, not workers)
# --threads 8 - Handle multiple requests per worker
# --timeout 0 - No timeout (for long-running data collection)
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 app:app
