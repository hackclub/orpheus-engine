#!/usr/bin/env bash
# =============================================================================
# DuckLake Web UI
# =============================================================================
# Starts DuckDB's built-in web UI with DuckLake connected.
# Reads credentials from .env file in the project root.
#
# Usage: ./scripts/ducklake_ui.sh
# =============================================================================

set -euo pipefail

# Source shared code
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/ducklake_common.sh"

echo "📁 Loading from .env..."
ducklake_setup --ui || exit 1

echo "🌐 Starting DuckDB Web UI on http://localhost:4213"
echo "   Access via SSH tunnel: ssh -L 4213:localhost:4213 user@this-server"
echo "   Press Ctrl+C to stop"
echo ""

# Run DuckDB with UI - keep stdin open to prevent exit
exec duckdb --init "$DUCKLAKE_INIT_SQL" < <(tail -f /dev/null)
