#!/bin/bash

# Health check script for YodaBuffett workers
# Called by Docker HEALTHCHECK instruction

set -e

HEALTH_CHECK_PORT=${HEALTH_CHECK_PORT:-8080}
HEALTH_ENDPOINT="http://localhost:$HEALTH_CHECK_PORT/health"

# Check if health endpoint is responding
if curl -f -s "$HEALTH_ENDPOINT" > /dev/null 2>&1; then
    echo "✅ Health check passed"
    exit 0
else
    echo "❌ Health check failed - endpoint not responding"
    exit 1
fi