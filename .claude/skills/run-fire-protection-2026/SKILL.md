---
name: run-fire-protection-2026
description: Run, start, build, screenshot, or verify the FireWatch web app (React + Vite, port 5173). Use when asked to run the app, take a screenshot, check a route, or test a UI change.
---

FireWatch is a React + Vite SPA (Greek-language observatory for forest-fire procurements). It uses a `HashRouter`, so all routes are `/#/<path>`. The agent driver is `.claude/skills/run-fire-protection-2026/driver.mjs`, which drives the app with Playwright + system Chrome. All commands below are run from `app/` (the web app root).

## Prerequisites

- Node.js (project uses v21)
- Google Chrome.app installed on macOS (used by Playwright's `channel: 'chrome'`)
- Playwright available via npx: `npx playwright --version` should print a version
- `.env` or `.env.local` in `app/` with `VITE_SUPABASE_URL` and `VITE_SUPABASE_ANON_KEY` — already present in the repo

No apt-get steps needed on macOS.

## Build

```bash
cd app
npm install        # already done if node_modules/ exists
```

## Run (agent path)

**Start the dev server** (from `app/`):

```bash
cd app
npm run dev > /tmp/vite.log 2>&1 &
echo $! > /tmp/vite.pid
# Wait for it to serve (polling, no timeout command on macOS)
for i in $(seq 1 30); do
  curl -sf http://localhost:5173 >/dev/null 2>&1 && echo "Ready after ${i}s" && break
  sleep 1
done
```

**Take a screenshot of one route** (from repo root):

```bash
node .claude/skills/run-fire-protection-2026/driver.mjs --route /#/maps --out /tmp/maps.png
```

**Smoke-test all routes** (exits non-zero if any route errors):

```bash
node .claude/skills/run-fire-protection-2026/driver.mjs --smoke
```

Driver options:

| Flag | Default | Description |
|---|---|---|
| `--route <hash>` | `/#/` | Hash route to visit |
| `--out <path>` | `/tmp/firewatch-<route>.png` | Screenshot output |
| `--smoke` | — | Visit all 6 routes, report errors |
| `--base <url>` | `http://localhost:5173` | Dev server base URL |

Known routes: `/#/`, `/#/maps`, `/#/contracts`, `/#/analysis`, `/#/municipalities`, `/#/environment-ministry`

**Stop the server:**

```bash
kill $(cat /tmp/vite.pid) 2>/dev/null || pkill -f "vite"
```

## Run (human path)

```bash
cd app && npm run dev
# Opens http://localhost:5173 — Ctrl-C to stop
```

## Test

```bash
cd app && npm test
```

## Gotchas

- **`*/` in JSDoc comments breaks `.mjs` parsing** — Node.js ES module parsing terminates the `/** */` block at the first `*/` anywhere in the source, including inside comments. Avoid glob patterns with `*/` in block comments in `.mjs` files.
- **HashRouter, not path routes** — navigating to `http://localhost:5173/maps` lands on the homepage (no 404, just silent fallback). Always use `/#/maps` form.
- **Playwright headless shell not installed** — `npx playwright install` would fail silently or install the wrong version. Use `channel: 'chrome'` (system Chrome) instead; it works without any install step.
- **`timeout` command unavailable on macOS** — the `timeout 30 bash -c ...` pattern from Linux examples fails. Use the `for i in $(seq 1 30)` polling loop above.
- **Supabase data loads asynchronously** — the app renders its shell immediately and loads data via Supabase RPCs. Using `waitUntil: 'networkidle'` in the driver correctly waits for all fetches to settle before screenshotting.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `SyntaxError: missing ) after argument list` in `driver.mjs` | A `*/` sequence inside a block comment closed the comment early — rewrite that comment line |
| `browserType.launch: Executable doesn't exist` | Do not use `chromium.launch()` without `channel`. Add `channel: 'chrome'` to use system Chrome |
| `EADDRINUSE: address already in use :::5173` | Run `pkill -f vite` before relaunching |
| Contracts page shows only spinner, no rows | Expected — Supabase data loads in background; use `waitUntil: 'networkidle'` or wait for a specific element |
