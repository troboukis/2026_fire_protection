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

DOWNLOAD_DIAVGEIA_PDFS="${DOWNLOAD_DIAVGEIA_PDFS:-0}"
REBUILD_ORG_MAPPINGS="${REBUILD_ORG_MAPPINGS:-0}"
RUN_DB_INGEST="${RUN_DB_INGEST:-1}"

if [[ -n "$(git status --porcelain)" ]]; then
  echo "[0/11] Working tree has local changes. Auto-committing them first..."
  git add -A
  git commit -m "chore: auto-commit local changes before fetch sync"
fi

echo "[1/11] Pull latest changes (rebase)..."
git pull --rebase origin main

if [[ "$DOWNLOAD_DIAVGEIA_PDFS" == "1" ]]; then
  echo "[2/11] Run Diavgeia fetch script (PDF download enabled)..."
else
  echo "[2/11] Run Diavgeia fetch script (PDF download disabled; set DOWNLOAD_DIAVGEIA_PDFS=1 to enable)..."
fi
DOWNLOAD_DIAVGEIA_PDFS="$DOWNLOAD_DIAVGEIA_PDFS" ./.fireprotection/bin/python fetch_diavgeia.py

echo "[3/11] Rebuild Diavgeia filtered dataset..."
./.fireprotection/bin/python src/filter_relevance.py

if [[ "$REBUILD_ORG_MAPPINGS" == "1" ]]; then
  echo "[4/11] Rebuild org mappings (single-match + coverage)..."
  ./.fireprotection/bin/python ingest/build_org_mapping.py
else
  echo "[4/11] Skipping org mapping rebuild (set REBUILD_ORG_MAPPINGS=1 to enable)."
fi

echo "[5/11] Fetch KIMDIS raw procurements..."
./.fireprotection/bin/python fetch_kimdis_procurements.py

echo "[6/11] Fetch Copernicus fires and upsert public.copernicus..."
./.fireprotection/bin/python src/fetch_copernicus.py

if [[ "$RUN_DB_INGEST" == "1" ]]; then
  echo "[7/11] Sync ERD tables to database (stage2_load_erd.py)..."
  ./.fireprotection/bin/python ingest/stage2_load_erd.py
else
  echo "[7/11] Skipping DB ingest (set RUN_DB_INGEST=1 to enable; default is enabled)."
fi

echo "[8/11] Stage generated artifacts..."
git add \
  data/2026_diavgeia.csv \
  data/2026_diavgeia_filtered.csv \
  data/fires/copernicus_latest.csv \
  data/raw_items_backup.json \
  data/raw_procurements.csv \
  data/mappings/org_to_municipality.csv \
  data/mappings/org_to_municipality_coverage.csv \
  data/mappings/region_to_municipalities.csv \
  state/state.json \
  logs/fetch_runs.csv \
  logs/relevance_filter_runs.csv \
  logs/kimdis_fetch_runs.csv

if git diff --cached --quiet; then
  echo "[9/11] No changes to commit."
  echo "[10/11] Nothing to push."
  echo "[11/11] Done."
  exit 0
fi

echo "[9/11] Commit changes..."
git commit -m "chore(data): update Diavgeia + KIMDIS + Copernicus datasets"

echo "[10/11] Push to origin/main..."
git push origin main

echo "[11/11] Done."
echo "[done] Fetch + sync completed successfully."
