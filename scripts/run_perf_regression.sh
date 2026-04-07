#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${ROOT_DIR}/build/bin/kvdb_perf"
OUT_DIR="${ROOT_DIR}/testdata/perf"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_FILE="${OUT_DIR}/perf_regression_${STAMP}.csv"

if [[ ! -x "${BIN}" ]]; then
  echo "[ERROR] ${BIN} not found. Please build first: cmake -S . -B build && cmake --build build" >&2
  exit 1
fi

mkdir -p "${OUT_DIR}"

echo "[INFO] Writing results to ${OUT_FILE}"

"${BIN}" --profile write-heavy --ops 200000 --threads 1 --out "${OUT_FILE}"
"${BIN}" --profile mixed       --ops 200000 --threads 1 --out "${OUT_FILE}"
"${BIN}" --profile read-heavy  --ops 200000 --threads 1 --out "${OUT_FILE}"

echo "[INFO] Perf regression suite completed: ${OUT_FILE}"
