#!/bin/bash

# Local testing script for Docker container
# Run this before deploying to Cloud Run to test locally

set -e

echo "🧪 Testing Docker Container Locally"
echo "==================================="
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running"
    echo "Please start Docker Desktop and try again"
    exit 1
fi

echo "✅ Docker is running"
echo ""

# Build the image
echo "🔨 Building Docker image..."
docker build -t news-sentiment-test .

if [ $? -ne 0 ]; then
    echo "❌ Build failed. Check the errors above."
    exit 1
fi

echo "✅ Build successful"
echo ""

# Run the container
echo "🚀 Starting container on http://localhost:8080"
echo ""
echo "📝 Your dashboard should open in your browser"
echo "Press Ctrl+C to stop the server"
echo ""

# Try to open browser (works on Mac)
sleep 2
if [[ "$OSTYPE" == "darwin"* ]]; then
    open http://localhost:8080
fi

# Run container
docker run -p 8080:8080 news-sentiment-test
