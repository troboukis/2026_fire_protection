#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ ! -d ".git" ]]; then
  echo "[error] Not a git repository: $REPO_ROOT"
  exit 1
fi

if [[ ! -x ".fireprotection/bin/python" ]]; then
  echo "[error] Missing virtual env python at .fireprotection/bin/python"
  echo "Create it first: python3 -m venv .fireprotection"
  exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "[0/8] Working tree has local changes. Auto-committing them first..."
  git add -A
  git commit -m "chore: auto-commit local changes before fetch sync"
fi

echo "[1/8] Pull latest changes (rebase)..."
git pull --rebase origin main

echo "[2/8] Run fetch script..."
./.fireprotection/bin/python fetch_diavgeia.py

echo "[3/8] Rebuild filtered dataset..."
./.fireprotection/bin/python src/filter_relevance.py

echo "[4/8] Rebuild org mappings (single-match + coverage)..."
./.fireprotection/bin/python ingest/build_org_mapping.py

if [[ "${RUN_DB_INGEST:-0}" == "1" ]]; then
  echo "[5/8] Sync procurement + coverage tables to database..."
  ./.fireprotection/bin/python ingest/ingest_procurement.py
  ./.fireprotection/bin/python ingest/ingest_procurement_lines.py
  ./.fireprotection/bin/python ingest/ingest_org_municipality_coverage.py
else
  echo "[5/8] Skipping DB ingest (set RUN_DB_INGEST=1 to enable)."
fi

echo "[6/8] Stage generated artifacts..."
git add \
  data/2026_diavgeia.csv \
  data/2026_diavgeia_filtered.csv \
  data/mappings/org_to_municipality.csv \
  data/mappings/org_to_municipality_coverage.csv \
  data/mappings/region_to_municipalities.csv \
  state/state.json \
  logs/fetch_runs.csv \
  logs/relevance_filter_runs.csv

if git diff --cached --quiet; then
  echo "[7/8] No changes to commit."
  echo "[8/8] Nothing to push."
  exit 0
fi

echo "[7/8] Commit changes..."
git commit -m "chore(data): update Diavgeia dataset"

echo "[8/8] Push to origin/main..."
git push origin main

echo "[done] Fetch + sync completed successfully."
