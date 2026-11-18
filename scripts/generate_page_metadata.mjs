import { readFileSync, writeFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import { loadPageMetadata } from './page_metadata.mjs'

const root = fileURLToPath(new URL('../', import.meta.url))
const { getPageMetadata, metadataRewrites, metadataSitemap } = loadPageMetadata()
const appConfig = JSON.parse(readFileSync(path.join(root, 'app/vercel.json'), 'utf8'))
const fallbacks = appConfig.rewrites.filter(rule => rule.source.includes('((') && ['/en/index.html', '/index.html'].includes(rule.destination))
if (fallbacks.length !== 2) throw new Error('Expected the existing Greek and English SPA fallbacks')
const outputs = [['app/public/sitemap.xml', metadataSitemap()]]
for (const file of ['app/vercel.json', 'vercel.json']) {
  const config = JSON.parse(readFileSync(path.join(root, file), 'utf8'))
  const custom = config.rewrites.filter(rule => !getPageMetadata(rule.source) && !rule.source.includes('(('))
  config.rewrites = [...custom, ...metadataRewrites(), ...fallbacks]
  outputs.push([file, JSON.stringify(config, null, 2) + '\n'])
}
for (const [file, expected] of outputs) {
  if (process.argv.includes('--check')) {
    if (readFileSync(path.join(root, file), 'utf8') !== expected) throw new Error(`Generated page metadata is stale: ${file}`)
  } else writeFileSync(path.join(root, file), expected)
}
console.log(process.argv.includes('--check') ? 'Page rewrites and sitemap are current.' : 'Generated bilingual page rewrites and sitemap.')
