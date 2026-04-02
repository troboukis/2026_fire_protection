#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ ! -d ".git" ]]; then
  echo "[error] Not a git repository: $REPO_ROOT"
  exit 1
fi

if [[ $# -ge 1 ]]; then
  COMMIT_MESSAGE="$*"
else
  read -r -p "Commit message: " COMMIT_MESSAGE
fi

if [[ -z "${COMMIT_MESSAGE// }" ]]; then
  COMMIT_MESSAGE="chore: update on $(date '+%Y-%m-%d %H:%M:%S')"
fi
CURRENT_BRANCH="$(git branch --show-current)"
UPSTREAM_REF="$(git rev-parse --abbrev-ref --symbolic-full-name '@{u}' 2>/dev/null || true)"
DEFAULT_REMOTE="$(git remote | head -n 1)"

if [[ -z "$CURRENT_BRANCH" ]]; then
  echo "[error] Could not determine current branch."
  exit 1
fi

echo "[1/4] Staging changes..."
git add -A

if git diff --cached --quiet; then
  echo "[2/4] No staged changes to commit."
else
  echo "[2/4] Creating commit..."
  git commit -m "$COMMIT_MESSAGE"
fi

if [[ -n "$UPSTREAM_REF" ]]; then
  echo "[3/4] Pulling latest changes with rebase from $UPSTREAM_REF..."
  git pull --rebase
else
  echo "[3/4] No upstream configured for $CURRENT_BRANCH. Skipping pull --rebase."
fi

echo "[4/4] Pushing branch $CURRENT_BRANCH..."
if [[ -n "$UPSTREAM_REF" ]]; then
  git push
elif [[ -n "$DEFAULT_REMOTE" ]]; then
  git push -u "$DEFAULT_REMOTE" "$CURRENT_BRANCH"
else
  echo "[error] No git remote is configured."
  exit 1
fi

echo "[done] Git sync completed."
