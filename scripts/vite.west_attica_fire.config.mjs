import megaFireConfig from '../../mega_fire_2026/app/vite.config.ts'

// Keep the source app's build configuration intact, but isolate its largest
// dependency so repeat visits can cache MapLibre independently from report code.
export default {
  ...megaFireConfig,
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
