import { fileURLToPath } from 'node:url'
import megaFireConfig from '../../mega_fire_2026/app/vite.config.ts'

// Share FireWatch branding and isolate the source app's largest dependency so
// repeat visits can cache MapLibre independently from report code.
export default {
  ...megaFireConfig,
  resolve: {
    ...megaFireConfig.resolve,
    alias: {
      ...megaFireConfig.resolve?.alias,
      './assets/logos/firewatch.png': fileURLToPath(
        new URL('../app/public/FireWatch_logo.svg', import.meta.url),
      ),
    },
  },
  build: {
    ...megaFireConfig.build,
    // MapLibre is a single large ESM module; its generated chunk is expected to
    // exceed Vite's generic 500 kB default while remaining below this budget.
    chunkSizeWarningLimit: 1100,
    rollupOptions: {
      ...megaFireConfig.build?.rollupOptions,
      output: {
        ...megaFireConfig.build?.rollupOptions?.output,
        manualChunks(id) {
          if (!id.includes('node_modules')) return
          if (id.includes('maplibre-gl')) return 'maplibre'
          if (id.includes('react') || id.includes('scheduler')) return 'react-vendor'
        },
      },
    },
  },
}
