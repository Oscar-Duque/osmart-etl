#!/usr/bin/env bash
set -Eeuo pipefail

# --- Resolve this script's directory in bash/zsh/other ---
if [ -n "${BASH_SOURCE:-}" ]; then
  SCRIPT_PATH="${BASH_SOURCE[0]}"
elif [ -n "${ZSH_VERSION:-}" ]; then
  SCRIPT_PATH="${(%):-%N}"
else
  SCRIPT_PATH="$0"
fi
SCRIPT_DIR="$(cd -- "$(dirname -- "$SCRIPT_PATH")" && pwd -P)"
PROJECT_ROOT="${OSMART_ETL_ROOT:-$SCRIPT_DIR}"

# --- Pick Python (prefer project venv if available) ---
if [ -x "$PROJECT_ROOT/venv/bin/python" ]; then
  PY="$PROJECT_ROOT/venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PY="$(command -v python3)"
else
  PY="$(command -v python)"
fi

# --- Activate venv if it exists (optional) ---
if [ -f "$PROJECT_ROOT/venv/bin/activate" ]; then
  # shellcheck source=/dev/null
  source "$PROJECT_ROOT/venv/bin/activate"
fi

# --- Run from project root ---
cd "$PROJECT_ROOT"

# --- Run tasks ---
"$PY" etl_sales/update_clean_data.py
"$PY" etl_inventory/update_raw_stock_movements.py
"$PY" etl_inventory/update_stock_points.py