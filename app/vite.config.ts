import { execSync } from 'node:child_process'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

function getLastCommitIso(): string {
  try {
    return execSync('git log -1 --format=%cI', { encoding: 'utf8' }).trim()
  } catch {
    return new Date().toISOString()
  }
}

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
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
