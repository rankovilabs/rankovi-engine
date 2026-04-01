#!/bin/bash
# ── Rankovi API startup script ─────────────────────────────────────────────────
# Runs inside the Cloud Run container
# Reads PORT from environment (Cloud Run sets this automatically)

set -e

PORT="${PORT:-8080}"

echo "▶ Starting Rankovi API on port $PORT"

exec uvicorn api.main:app \
  --host 0.0.0.0 \
  --port "$PORT" \
  --workers 2 \
  --log-level info \
  --access-log
