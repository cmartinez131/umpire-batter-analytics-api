#!/usr/bin/env bash
set -euo pipefail

echo "[startup] DATA_ROOT=${DATA_ROOT:-/app/data}"
if [[ -n "${S3_DATA_PREFIX:-}" ]]; then
  echo "[startup] syncing ${S3_DATA_PREFIX} -> ${DATA_ROOT}"
  mkdir -p "${DATA_ROOT}"
  aws s3 sync "${S3_DATA_PREFIX}" "${DATA_ROOT}" --no-progress
else
  echo "[startup] S3_DATA_PREFIX not set; using existing data"
fi

exec uvicorn services.api.main:app --host 0.0.0.0 --port "${PORT:-8080}"
