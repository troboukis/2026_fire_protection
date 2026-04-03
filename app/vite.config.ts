import { execSync } from 'node:child_process'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { getCompanyUrlByAfm, normalizeAfm } from './server/gemiCompanyUrl.js'

function getLastCommitIso(): string {
  try {
    return execSync('git log -1 --format=%cI', { encoding: 'utf8' }).trim()
  } catch {
    return new Date().toISOString()
  }
}

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    react(),
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
})
