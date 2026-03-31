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
ERD_TABLES="${ERD_TABLES:-region,municipality,organization,diavgeia_document_type,procurement,cpv,diavgeia,payment,fund,diavgeia_procurement,beneficiary}"

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
echo "[7/13] Sync ERD tables to database (stage2_load_erd.py, excluding forest_fire)..."
  ./.fireprotection/bin/python ingest/stage2_load_erd.py --tables "$ERD_TABLES"
else
  echo "[7/13] Skipping DB ingest (set RUN_DB_INGEST=1 to enable; default is enabled)."
fi

echo "[8/13] Backfill canonical organization authority scopes..."
./.fireprotection/bin/python scripts/backfill_organization_authority_scope.py

echo "[9/13] Reload organization municipality coverage..."
./.fireprotection/bin/python scripts/load_org_municipality_coverage.py

echo "[10/13] Stage generated artifacts..."
git add \
  data/2026_diavgeia.csv \
  data/2026_diavgeia_filtered.csv \
  data/fires/copernicus_latest.csv \
  data/raw_procurements.csv \
  data/mappings/org_to_municipality.csv \
  data/mappings/org_to_municipality_coverage.csv \
  data/mappings/region_to_municipalities.csv \
  state/kimdis_state.json \
  state/state.json \
  logs/fetch_runs.csv \
  logs/org_municipality_coverage_unresolved.csv \
  logs/relevance_filter_runs.csv \
  logs/kimdis_fetch_runs.csv

if git diff --cached --quiet; then
  echo "[11/13] No changes to commit."
  echo "[12/13] Nothing to push."
  echo "[13/13] Done."
  exit 0
fi

echo "[11/13] Commit changes..."
git commit -m "chore(data): update Diavgeia + KIMDIS + Copernicus datasets"

echo "[12/13] Push to origin/main..."
git push origin main

echo "[13/13] Done."
echo "[done] Fetch + sync completed successfully."
