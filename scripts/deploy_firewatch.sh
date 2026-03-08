#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

VITE_ENABLE_DEV_VIEW=0 TARGET_SUBDIR=firewatch "$REPO_ROOT/scripts/deploy_to_github_pages.sh"
