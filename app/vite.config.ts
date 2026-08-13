import { execSync } from 'node:child_process'
import { readFile } from 'node:fs/promises'
import path from 'node:path'
import { defineConfig, type Plugin } from 'vite'
import react from '@vitejs/plugin-react'
import { getCompanyUrlByAfm, normalizeAfm } from './server/gemiCompanyUrl.js'

function getLastCommitIso(): string {
  try {
    return execSync('git log -1 --format=%cI', { encoding: 'utf8' }).trim()
  } catch {
    return new Date().toISOString()
  }
}

function standaloneWestAtticaMapDevRoute(): Plugin {
  const routePaths = new Set([
    '/west-attica-fire-2026',
    '/west-attica-fire-2026/',
  ])
  const indexPath = path.resolve(process.cwd(), 'public/west-attica-fire-2026/index.html')

  return {
    name: 'standalone-west-attica-map-dev-route',
    configureServer(server) {
      server.middlewares.use(async (req, res, next) => {
        const requestUrl = req.url
        if (!requestUrl || req.method !== 'GET') {
          next()
          return
        }

        const url = new URL(requestUrl, 'http://localhost')
        if (!routePaths.has(url.pathname)) {
          next()
          return
        }

        try {
          const html = await readFile(indexPath, 'utf8')
          res.statusCode = 200
          res.setHeader('Content-Type', 'text/html; charset=utf-8')
          res.setHeader('Cache-Control', 'no-store')
          res.end(html)
        } catch (error) {
          next(error)
        }
      })
    },
  }
}

// https://vite.dev/config/
export default defineConfig(({ command }) => ({
  plugins: [
    react(),
    standaloneWestAtticaMapDevRoute(),
    {
      name: 'gemi-company-url-dev-api',
      configureServer(server) {
        server.middlewares.use(async (req, res, next) => {
          const requestUrl = req.url
          if (!requestUrl || req.method !== 'GET') {
            next()
            return
          }

          const url = new URL(requestUrl, 'http://localhost')
          if (url.pathname !== '/api/gemi/company-url') {
            next()
            return
          }

          res.setHeader('Content-Type', 'application/json; charset=utf-8')
          res.setHeader('Cache-Control', 'no-store')

          const afm = normalizeAfm(url.searchParams.get('afm'))
          if (!afm) {
            res.statusCode = 400
            res.end(JSON.stringify({ error: 'Missing or invalid AFM' }))
            return
          }

          try {
            const payload = await getCompanyUrlByAfm(afm)
            res.statusCode = 200
            res.end(JSON.stringify(payload))
          } catch (error) {
            const message = error instanceof Error ? error.message : 'Unknown GEMI lookup error'
            res.statusCode = message.includes('No exact AFM match') ? 404 : 502
            res.end(JSON.stringify({ error: message }))
          }
        })
      },
    },
  ],
  define: {
    __LAST_COMMIT_ISO__: JSON.stringify(getLastCommitIso()),
  },
  esbuild: command === 'build'
    ? {
        drop: ['console', 'debugger'],
        pure: ['logDebug', 'logWarn', 'logError'],
      }
    : undefined,
  build: {
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (!id.includes('node_modules')) return
          if (id.includes('@supabase/supabase-js')) return 'supabase'
          if (id.includes('/d3-')) return 'd3'
          if (id.includes('react') || id.includes('scheduler')) return 'react-vendor'
        },
      },
    },
  },
}))
