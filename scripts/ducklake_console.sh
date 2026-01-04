#!/usr/bin/env bash
# =============================================================================
# DuckLake Console
# =============================================================================
# Opens an interactive DuckDB console with DuckLake connected.
# Reads credentials from .env file in the project root.
#
# Usage: ./scripts/ducklake_console.sh
# =============================================================================

set -euo pipefail

# Source shared code
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/ducklake_common.sh"

echo "📁 Loading from .env..."
ducklake_setup || exit 1

echo "🖥️  Starting DuckDB console..."
echo "   Type .help for commands, .exit to quit"
echo ""

# Start interactive DuckDB console
exec duckdb --init "$DUCKLAKE_INIT_SQL"


