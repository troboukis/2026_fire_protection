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
  echo "[0/5] Working tree has local changes. Auto-committing them first..."
  git add -A
  git commit -m "chore: auto-commit local changes before fetch sync"
fi

echo "[1/5] Pull latest changes (rebase)..."
git pull --rebase origin main

echo "[2/5] Run fetch script..."
./.fireprotection/bin/python fetch_diavgeia.py

echo "[3/5] Stage generated artifacts..."
git add data/2026_diavgeia.csv state/state.json logs/fetch_runs.csv

if git diff --cached --quiet; then
  echo "[4/5] No changes to commit."
  echo "[5/5] Nothing to push."
  exit 0
fi

echo "[4/5] Commit changes..."
git commit -m "chore(data): update Diavgeia dataset"

echo "[5/5] Push to origin/main..."
git push origin main

echo "[done] Fetch + sync completed successfully."
