#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

REMOTE="${NEWS_FIRES_GIT_REMOTE:-origin}"
BRANCH="${NEWS_FIRES_GIT_BRANCH:-${GITHUB_REF_NAME:-$(git branch --show-current)}}"
PYTHON_BIN="${PYTHON:-python}"
STATE_FILE="logs/news_fires_state.json"
COMMIT_MESSAGE="${NEWS_FIRES_STATE_COMMIT_MESSAGE:-chore(data): update news fires state}"

if [ -z "$BRANCH" ]; then
  echo "Cannot determine git branch. Set NEWS_FIRES_GIT_BRANCH."
  exit 1
fi

sync_branch() {
  git fetch "$REMOTE" "$BRANCH"
  git pull --rebase --autostash "$REMOTE" "$BRANCH"
}

preserve_generated_state() {
  local state_copy="$1"
  if [ -f "$STATE_FILE" ]; then
    cp "$STATE_FILE" "$state_copy"
  fi
}

restore_generated_state() {
  local state_copy="$1"
  if [ -f "$state_copy" ]; then
    mkdir -p "$(dirname "$STATE_FILE")"
    cp "$state_copy" "$STATE_FILE"
  fi
}

mkdir -p logs

echo "[news_fires_script] sync_before_run remote=$REMOTE branch=$BRANCH"
sync_branch

echo "[news_fires_script] run_news_fires"
"$PYTHON_BIN" src/fetch_news_fires.py

if [ ! -f "$STATE_FILE" ]; then
  echo "[news_fires_script] state file not found: $STATE_FILE"
  exit 0
fi

state_copy="$(mktemp)"
trap 'rm -f "$state_copy"' EXIT
preserve_generated_state "$state_copy"

echo "[news_fires_script] sync_before_commit remote=$REMOTE branch=$BRANCH"
sync_branch
restore_generated_state "$state_copy"

git add "$STATE_FILE"
if git diff --cached --quiet; then
  echo "[news_fires_script] no state changes to commit"
  exit 0
fi

git config user.name "${GIT_AUTHOR_NAME:-github-actions[bot]}"
git config user.email "${GIT_AUTHOR_EMAIL:-41898282+github-actions[bot]@users.noreply.github.com}"

git commit -m "$COMMIT_MESSAGE"

for attempt in 1 2 3; do
  if git push "$REMOTE" "HEAD:$BRANCH"; then
    echo "[news_fires_script] pushed state update"
    exit 0
  fi

  if [ "$attempt" -eq 3 ]; then
    echo "[news_fires_script] push failed after $attempt attempts"
    exit 1
  fi

  echo "[news_fires_script] push failed, rebasing and retrying attempt=$((attempt + 1))"
  preserve_generated_state "$state_copy"
  sync_branch
  restore_generated_state "$state_copy"
  git add "$STATE_FILE"
  if ! git diff --cached --quiet; then
    git commit --amend --no-edit
  fi
done
