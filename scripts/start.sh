#!/usr/bin/env sh
set -e

echo "Starting Dagster deployment with orphaned run cleanup..."

# Start webserver first so GraphQL is available for cleanup
echo "Starting Dagster webserver..."
uv run dagster-webserver -h 0.0.0.0 -p 3000 &
WS_PID=$!

# Run cleanup once webserver is responsive
echo "Running orphaned run cleanup..."
uv run python /app/scripts/cancel_orphaned_runs.py || {
    echo "WARNING: Run cleanup failed, but continuing with startup..."
}

# Now start the daemon
echo "Starting Dagster daemon..."
uv run dagster-daemon run &
DAEMON_PID=$!

echo "All services started! Webserver PID: $WS_PID, Daemon PID: $DAEMON_PID"

# Handle signals to gracefully shut down both processes
cleanup() {
    echo "Shutting down Dagster services..."
    kill $DAEMON_PID $WS_PID 2>/dev/null || true
    wait $DAEMON_PID $WS_PID 2>/dev/null || true
    echo "Shutdown complete."
}

trap cleanup TERM INT

# Wait for either process to exit
wait
