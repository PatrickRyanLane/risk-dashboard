#!/bin/bash
# Helper script to watch Cloud Run logs in real-time

SERVICE_NAME="news-sentiment-dashboard"
REGION="us-central1"

echo "📊 Streaming logs from Cloud Run..."
echo "   Service: $SERVICE_NAME"
echo "   Region: $REGION"
echo ""
echo "Press Ctrl+C to stop"
echo "========================================"
echo ""

# Stream logs with colorization
gcloud run services logs tail $SERVICE_NAME \
  --region $REGION \
  --follow \
  --format="value(textPayload)" 2>/dev/null | while IFS= read -r line; do
    # Add color coding based on content
    if [[ $line == *"ERROR"* ]] || [[ $line == *"❌"* ]]; then
      echo -e "\033[0;31m$line\033[0m"  # Red
    elif [[ $line == *"WARNING"* ]] || [[ $line == *"⚠️"* ]]; then
      echo -e "\033[0;33m$line\033[0m"  # Yellow
    elif [[ $line == *"✅"* ]] || [[ $line == *"SUCCESS"* ]]; then
      echo -e "\033[0;32m$line\033[0m"  # Green
    elif [[ $line == *"INFO"* ]] || [[ $line == *"📊"* ]] || [[ $line == *"📋"* ]]; then
      echo -e "\033[0;36m$line\033[0m"  # Cyan
    else
      echo "$line"
    fi
done
