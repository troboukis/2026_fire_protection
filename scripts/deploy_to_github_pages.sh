#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_DIR="$REPO_ROOT/app"
PAGES_REPO_DIR="${PAGES_REPO_DIR:-/Users/troboukis/Code/troboukis.github.io}"
TARGET_SUBDIR="${TARGET_SUBDIR:-fire_protection}"
TARGET_DIR="$PAGES_REPO_DIR/$TARGET_SUBDIR"
SITE_URL="${SITE_URL:-https://troboukis.github.io/$TARGET_SUBDIR/}"
SOCIAL_IMAGE_URL="${SOCIAL_IMAGE_URL:-${SITE_URL}cover_16_9_social.jpg}"
BASE_PATH="/$TARGET_SUBDIR/"

if [[ ! -d "$APP_DIR" ]]; then
  echo "[error] app directory not found: $APP_DIR"
  exit 1
fi

if [[ ! -d "$PAGES_REPO_DIR/.git" ]]; then
  echo "[error] GitHub Pages repo not found: $PAGES_REPO_DIR"
  exit 1
fi

echo "[1/4] Building frontend for GitHub Pages base /$TARGET_SUBDIR/ ..."
cd "$APP_DIR"
VITE_SITE_URL="$SITE_URL" \
VITE_SOCIAL_IMAGE_URL="$SOCIAL_IMAGE_URL" \
VITE_BASE_PATH="$BASE_PATH" \
npm run build -- --base="$BASE_PATH"

echo "[2/4] Syncing dist to $TARGET_DIR ..."
rm -rf "$TARGET_DIR"
mkdir -p "$TARGET_DIR"
cp -R dist/* "$TARGET_DIR/"

echo "[3/4] Committing in Pages repo ..."
cd "$PAGES_REPO_DIR"
git add "$TARGET_SUBDIR"
if git diff --cached --quiet; then
  echo "[info] No changes to commit."
  exit 0
fi
git commit -m "deploy: update $TARGET_SUBDIR app"

echo "[4/4] Pushing ..."
git push

echo "[done] Deployed to https://troboukis.github.io/$TARGET_SUBDIR/"
