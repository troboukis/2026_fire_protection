#!/usr/bin/env bash

# Update the standalone West Attica fire map published by FireWatch.
#
# This script deliberately stops on the first error. That prevents an old or
# incomplete map build from being copied into FireWatch.

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
firewatch_root="$(dirname "$script_dir")"
mega_fire_root="$(dirname "$firewatch_root")/mega_fire_2026"
mega_fire_app="$mega_fire_root/app"
firewatch_app="$firewatch_root/app"
mega_fire_vite_config="$script_dir/vite.west_attica_fire.config.mjs"
public_path="/analysis/west-attica-fire-2026/"

if [[ ! -f "$mega_fire_app/package.json" ]]; then
  echo "Cannot find the mega_fire_2026 app at: $mega_fire_app" >&2
  echo "Keep mega_fire_2026 beside fire_protection_2026, then run this script again." >&2
  exit 1
fi

if [[ ! -d "$mega_fire_app/node_modules" ]]; then
  echo "mega_fire_2026 dependencies are missing." >&2
  echo "Run 'npm install' inside $mega_fire_app, then run this script again." >&2
  exit 1
fi

if [[ ! -d "$firewatch_app/node_modules" ]]; then
  echo "FireWatch dependencies are missing." >&2
  echo "Run 'npm install' inside $firewatch_app, then run this script again." >&2
  exit 1
fi

echo "Step 1 of 4: Refresh the West Attica data used by mega_fire_2026."
(
  cd "$mega_fire_app"
  npm run sync:data -- attica-boeotia-2026
)

echo
echo "Step 2 of 4: Build the latest map for its FireWatch URL."
(
  cd "$mega_fire_app"
  npm run build -- --base="$public_path" --config="$mega_fire_vite_config"
)

echo
echo "Step 3 of 4: Copy the finished map into FireWatch."
(
  cd "$firewatch_app"
  npm run sync:west-attica-fire
)

echo
echo "Step 4 of 4: Build FireWatch to check that everything fits together."
(
  cd "$firewatch_app"
  npm run build
)

echo
echo "Done: the West Attica map is updated inside the local FireWatch project."
echo "Public URL after the FireWatch changes are committed and deployed:"
echo "https://www.fire-watch-app.gr$public_path"
echo
echo "Files waiting to be committed:"
git -C "$firewatch_root" status --short -- \
  .gitignore \
  app/README.md \
  app/public/analysis/west-attica-fire-2026 \
  app/public/sitemap.xml \
  app/vercel.json \
  app/vite.config.ts \
  scripts/sync_west_attica_fire_dist.mjs \
  scripts/update_west_attica_fire_map.sh \
  scripts/vite.west_attica_fire.config.mjs \
  vercel.json
