#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

VITE_ENABLE_DEV_VIEW=1 TARGET_SUBDIR=fire_protection "$REPO_ROOT/scripts/deploy_to_github_pages.sh"
