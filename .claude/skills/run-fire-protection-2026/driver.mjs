#!/usr/bin/env node
/**
 * Playwright driver for FireWatch web app.
 *
 * Usage:
 *   node driver.mjs [--route /#/maps] [--out /tmp/shot.png] [--smoke]
 *
 * Options:
 *   --route <hash>   Hash route to visit, e.g. /#/contracts (default: /#/)
 *   --out <path>     Screenshot output path (default: /tmp/firewatch-<route>.png)
 *   --smoke          Visit all known routes, report errors, exit non-zero on any
 *   --base <url>     Dev server base URL (default: http://localhost:5173)
 *
 * Requires: system Chrome (Google Chrome.app on macOS)
 * Playwright is loaded from the npx cache at ~/.npm/_npx/[hash]/node_modules/playwright.
 *
 * Example:
 *   node driver.mjs --route /#/maps --out /tmp/maps.png
 *   node driver.mjs --smoke
 */

import { spawnSync } from 'node:child_process'
import { resolve, join } from 'node:path'
import { createRequire } from 'node:module'
import { homedir, tmpdir } from 'node:os'
import { existsSync, readdirSync } from 'node:fs'

// Resolve playwright from the npx cache
function findPlaywright() {
  const npxCache = join(homedir(), '.npm', '_npx')
  if (!existsSync(npxCache)) return null
  for (const dir of readdirSync(npxCache)) {
    const p = join(npxCache, dir, 'node_modules', 'playwright')
    if (existsSync(p)) return p
  }
  return null
}

const playwrightPath = findPlaywright()
if (!playwrightPath) {
  console.error('playwright not found in ~/.npm/_npx/ — run: npx playwright --version')
  process.exit(1)
}

const require = createRequire(import.meta.url)
const { chromium } = require(playwrightPath)

const KNOWN_ROUTES = [
  '/#/',
  '/#/maps',
  '/#/contracts',
  '/#/analysis',
  '/#/municipalities',
  '/#/environment-ministry',
]

const args = process.argv.slice(2)
const get = (flag) => { const i = args.indexOf(flag); return i >= 0 ? args[i + 1] : null }
const has = (flag) => args.includes(flag)

const BASE = get('--base') || 'http://localhost:5173'
const ROUTE = get('--route') || '/#/'
const SMOKE = has('--smoke')

function safeFilename(route) {
  return route.replace(/[/#]/g, '_').replace(/^_+/, '') || 'home'
}

async function screenshot(page, route, outPath) {
  const url = BASE + route
  const out = outPath || join(tmpdir(), `firewatch-${safeFilename(route)}.png`)
  await page.goto(url, { waitUntil: 'networkidle', timeout: 30_000 })
  await page.screenshot({ path: out, fullPage: false })
  return out
}

async function main() {
  const consoleErrors = []
  const browser = await chromium.launch({
    channel: 'chrome',
    args: ['--no-sandbox'],
    headless: true,
  })

  try {
    const page = await (await browser.newContext({ viewport: { width: 1280, height: 900 } })).newPage()
    page.on('console', msg => { if (msg.type() === 'error') consoleErrors.push(msg.text()) })
    page.on('pageerror', err => consoleErrors.push(err.message))

    if (SMOKE) {
      let failed = false
      for (const route of KNOWN_ROUTES) {
        try {
          const out = await screenshot(page, route)
          console.log(`OK  ${route}  ->  ${out}`)
        } catch (e) {
          console.error(`ERR ${route}: ${e.message}`)
          failed = true
        }
      }
      if (consoleErrors.length) {
        console.error('\nBrowser console errors:')
        consoleErrors.forEach(e => console.error(' ', e))
      } else {
        console.log('\nNo browser console errors.')
      }
      process.exitCode = failed ? 1 : 0
    } else {
      const out = get('--out')
      const saved = await screenshot(page, ROUTE, out)
      console.log(`Screenshot: ${saved}`)
      if (consoleErrors.length) {
        console.error('Browser console errors:')
        consoleErrors.forEach(e => console.error(' ', e))
      }
    }
  } finally {
    await browser.close()
  }
}

main().catch(e => { console.error(e.message); process.exit(1) })
