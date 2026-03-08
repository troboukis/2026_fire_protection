#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ ! -x ".fireprotection/bin/python" ]]; then
  echo "[error] Missing virtual env python at .fireprotection/bin/python"
  echo "Run: python3 -m venv .fireprotection"
  exit 1
fi

if [[ ! -d "app/node_modules" ]]; then
  echo "[error] Missing app/node_modules"
  echo "Run: (cd app && npm install)"
  exit 1
fi

echo "[1/5] Python syntax check..."
python3 -m py_compile fetch_diavgeia.py fetch_kimdis_procurements.py
python3 -m py_compile src/*.py ingest/*.py

echo "[2/5] Python tests..."
./.fireprotection/bin/python -m pytest

echo "[3/5] Frontend tests..."
npm --prefix app run test

echo "[4/5] Frontend lint..."
npm --prefix app run lint

echo "[5/5] Frontend build..."
npm --prefix app run build

echo "[done] Local verification passed."
