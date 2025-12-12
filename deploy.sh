#!/bin/bash

# Simple deployment script for Google Cloud Run
# This script makes deployment as easy as running: ./deploy.sh

set -e  # Exit on any error

echo "🚀 News Sentiment Dashboard - Cloud Run Deployment"
echo "=================================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo -e "${RED}❌ Error: gcloud CLI is not installed${NC}"
    echo "Please install it from: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Get current project
PROJECT_ID=$(gcloud config get-value project 2>/dev/null)

if [ -z "$PROJECT_ID" ]; then
    echo -e "${RED}❌ Error: No Google Cloud project is configured${NC}"
    echo "Run: gcloud config set project YOUR_PROJECT_ID"
    exit 1
fi

echo -e "${BLUE}📦 Current project: ${PROJECT_ID}${NC}"
echo ""

# Ask for confirmation
echo "This will deploy your dashboard to Cloud Run."
read -p "Continue? (y/n) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Deployment cancelled."
    exit 0
fi

echo ""
echo -e "${BLUE}🔨 Building and deploying...${NC}"
echo ""

# Deploy using gcloud
gcloud run deploy news-sentiment-dashboard \
  --source . \
  --region us-central1 \
  --platform managed \
  --allow-unauthenticated \
  --memory 512Mi \
  --cpu 1 \
  --port 8080

# Check if deployment succeeded
if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✅ Deployment successful!${NC}"
    echo ""
    echo "Your dashboard is now live at:"
    gcloud run services describe news-sentiment-dashboard \
      --region us-central1 \
      --format 'value(status.url)'
    echo ""
else
    echo -e "${RED}❌ Deployment failed. Check the error messages above.${NC}"
    exit 1
fi
