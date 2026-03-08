#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

VITE_ENABLE_DEV_VIEW=1 \
TARGET_SUBDIR=fire_protection \
SITE_URL=https://troboukis.github.io/fire_protection/ \
SOCIAL_IMAGE_URL=https://troboukis.github.io/fire_protection/cover_16_9_social.jpg \
"$REPO_ROOT/scripts/deploy_to_github_pages.sh"
