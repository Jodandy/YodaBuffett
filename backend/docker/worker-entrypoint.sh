#!/bin/bash
set -e

# YodaBuffett Multi-Market Worker Entrypoint Script
# Dynamically starts the correct worker based on environment variables

# Default values
WORKER_TYPE=${WORKER_TYPE:-"base"}
WORKER_NAME=${WORKER_NAME:-"base-worker"}
WORKER_MARKET=${WORKER_MARKET:-""}
HEALTH_CHECK_PORT=${HEALTH_CHECK_PORT:-8080}

echo "🚀 Starting YodaBuffett Worker: $WORKER_NAME"
echo "   Type: $WORKER_TYPE"
echo "   Market: ${WORKER_MARKET:-'N/A'}"
echo "   Mode: ${WORKER_MODE:-'production'}"
echo "   Health Check: http://localhost:$HEALTH_CHECK_PORT/health"
echo "   Timestamp: $(date -u +"%Y-%m-%d %H:%M:%S UTC")"

# Ensure directories exist
mkdir -p "${DATA_VOLUME_PATH:-/app/data}"
mkdir -p "$(dirname "${LOG_FILE_PATH:-/app/logs/worker.log}")"
mkdir -p "${TEMP_PATH:-/app/temp}"

# Start health check endpoint in background
python -m workers.base.health_server --port=$HEALTH_CHECK_PORT &
HEALTH_PID=$!

# Function to cleanup on exit
cleanup() {
    echo "🛑 Shutting down worker..."
    if [ ! -z "$WORKER_PID" ]; then
        kill -TERM $WORKER_PID 2>/dev/null || true
        wait $WORKER_PID 2>/dev/null || true
    fi
    kill $HEALTH_PID 2>/dev/null || true
    wait
    echo "✅ Worker shutdown complete"
}

# Set up signal handlers
trap cleanup SIGTERM SIGINT

# Wait for database to be ready
echo "⏳ Waiting for database connection..."
python -c "
import asyncio
import sys
import os
sys.path.append('/app')
from workers.worker_config import get_config

async def wait_for_db():
    config = get_config()
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy import text
    
    engine = create_async_engine(config.database.connection_url, echo=False)
    
    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            async with engine.begin() as conn:
                await conn.execute(text('SELECT 1'))
            print('✅ Database connection successful')
            break
        except Exception as e:
            if attempt == max_attempts - 1:
                print(f'❌ Database connection failed after {max_attempts} attempts: {e}')
                sys.exit(1)
            print(f'⏳ Database connection attempt {attempt + 1}/{max_attempts} failed, retrying...')
            await asyncio.sleep(2)
    
    await engine.dispose()

asyncio.run(wait_for_db())
"

# Create necessary directories
mkdir -p /app/data /app/logs

# Set proper permissions
chown -R worker:worker /app/data /app/logs 2>/dev/null || true

echo "✅ Initialization complete"

# Determine which worker to start based on configuration
case $WORKER_TYPE in
    "document_ingestor")
        case $WORKER_MARKET in
            "swedish")
                echo "📊 Starting Swedish Document Ingestor"
                python -m workers.ingestors.swedish_document_ingestor "$@" &
                ;;
            "norwegian")
                echo "📊 Starting Norwegian Document Ingestor"
                python -m workers.ingestors.norwegian_document_ingestor "$@" &
                ;;
            "danish")
                echo "📊 Starting Danish Document Ingestor"
                python -m workers.ingestors.danish_document_ingestor "$@" &
                ;;
            "finnish")
                echo "📊 Starting Finnish Document Ingestor"
                python -m workers.ingestors.finnish_document_ingestor "$@" &
                ;;
            *)
                echo "❌ Unknown market for document ingestor: $WORKER_MARKET"
                exit 1
                ;;
        esac
        ;;
    
    "event_monitor")
        case $WORKER_NAME in
            "swedish-event-monitor")
                echo "📅 Starting Swedish Event Monitor"
                python -m workers.monitors.swedish_event_monitor "$@" &
                ;;
            "nordic-surprise-scanner")
                echo "🔍 Starting Nordic Surprise Scanner"
                python -m workers.monitors.surprise_scanner "$@" &
                ;;
            *)
                echo "❌ Unknown event monitor: $WORKER_NAME"
                exit 1
                ;;
        esac
        ;;
    
    "market_data")
        case $WORKER_NAME in
            "nordic-price-collector")
                echo "💰 Starting Nordic Price Collector"
                python -m workers.market_data.price_collector "$@" &
                ;;
            "dividend-tracker")
                echo "💵 Starting Dividend Tracker"
                python -m workers.market_data.dividend_tracker "$@" &
                ;;
            *)
                echo "❌ Unknown market data worker: $WORKER_NAME"
                exit 1
                ;;
        esac
        ;;
    
    "maintenance")
        case $WORKER_NAME in
            "database-cleanup-worker")
                echo "🧹 Starting Database Cleanup Worker"
                python -m workers.maintenance.database_cleanup "$@" &
                ;;
            "data-quality-auditor")
                echo "🔍 Starting Data Quality Auditor"
                python -m workers.maintenance.data_quality_auditor "$@" &
                ;;
            *)
                echo "❌ Unknown maintenance worker: $WORKER_NAME"
                exit 1
                ;;
        esac
        ;;
    
    "scheduler")
        echo "📅 Starting Daily Event Scheduler"
        python -m workers.daily_scheduler --time ${DAILY_RUN_TIME:-06:00} &
        ;;
    
    "manager")
        echo "🎛️  Starting Worker Manager"
        python -m workers.management.worker_manager "$@" &
        ;;
    
    "base")
        echo "🤖 Starting Base Worker"
        python -m workers.base.base_worker "$@" &
        ;;
    
    *)
        echo "❌ Unknown worker type: $WORKER_TYPE"
        echo "Available types: document_ingestor, event_monitor, market_data, maintenance, scheduler, manager, base"
        exit 1
        ;;
esac

WORKER_PID=$!
echo "📋 Worker PID: $WORKER_PID"

# Wait for worker to complete
wait $WORKER_PID
EXIT_CODE=$?

echo "🏁 Worker completed with exit code: $EXIT_CODE"
exit $EXIT_CODE