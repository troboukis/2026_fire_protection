#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

VITE_ENABLE_DEV_VIEW=0 \
TARGET_SUBDIR=firewatch \
SITE_URL=https://troboukis.github.io/firewatch/ \
SOCIAL_IMAGE_URL=https://troboukis.github.io/firewatch/cover_16_9_social.jpg \
"$REPO_ROOT/scripts/deploy_to_github_pages.sh"
