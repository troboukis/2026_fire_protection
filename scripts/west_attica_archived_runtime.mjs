import { readFileSync, readdirSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

// Language work must not upgrade the published renderer or its matching worker.
// The neighbouring source project can have a newer installed MapLibre version.
export function archivedWestAtticaRuntime() {
  const archive = new URL('../app/public/analysis/west-attica-fire-2026/', import.meta.url)
  const assets = readdirSync(new URL('assets/', archive))
  const unique = pattern => {
    const matches = assets.filter(name => pattern.test(name))
    if (matches.length !== 1) throw new Error('Expected one published West Attica MapLibre asset.')
    return matches[0]
  }
  const javascript = unique(/^maplibre-.*\.js$/)
  const stylesheet = unique(/^maplibre-.*\.css$/)
  const base = '/analysis/west-attica-fire-2026/'
  const runtimeUrl = `${base}assets/${javascript}`
  const source = readFileSync(new URL(`assets/${javascript}`, archive), 'utf8')
  if (!source.includes('export{aD as K,iD as g,sD as i,rD as m}')) {
    throw new Error('Published MapLibre exports changed; review the source adapter before rebuilding.')
  }
  const preserved = [
    `assets/${javascript}`, `assets/${stylesheet}`,
    'vendor/maplibre/maplibre-gl-worker.mjs',
    'vendor/maplibre/maplibre-gl-shared.mjs',
  ].map(fileName => ({ fileName, source: readFileSync(new URL(fileName, archive)) }))
  return {
    name: 'firewatch-archived-maplibre',
    enforce: 'pre',
    resolveId(id) {
      if (id === 'maplibre-gl') return '\0firewatch-archived-maplibre'
      if (id === 'maplibre-gl/dist/maplibre-gl.css') return '\0firewatch-archived-maplibre-css'
      if (id === runtimeUrl) return { id, external: true }
    },
    load(id) {
      if (id === '\0firewatch-archived-maplibre') {
        return `export {m as MapLibreMap,g as NavigationControl,K as Popup,i as setWorkerUrl} from ${JSON.stringify(runtimeUrl)};`
      }
      if (id === '\0firewatch-archived-maplibre-css') return ''
    },
    transformIndexHtml: {
      order: 'post',
      handler() {
        return [{ tag: 'link', attrs: { rel: 'stylesheet', crossorigin: '', href: `${base}assets/${stylesheet}` }, injectTo: 'head-prepend' }]
      },
    },
    generateBundle() {
      for (const asset of preserved) this.emitFile({ type: 'asset', ...asset })
    },
    // Keep a concrete path available for tests and maintenance diagnostics.
    archivePath: fileURLToPath(archive),
  }
}
