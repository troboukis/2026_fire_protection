import { readFileSync, writeFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { isDeepStrictEqual } from 'node:util'
import path from 'node:path'
import { loadPageMetadata } from './page_metadata.mjs'

const repositoryRoot = fileURLToPath(new URL('../', import.meta.url))

export function generatePageMetadata({ root = repositoryRoot, check = false } = {}) {
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
    if (check) {
      const actual = readFileSync(path.join(root, file), 'utf8')
      // Build systems can reserialize JSON without changing routing. Object key
      // order and whitespace are irrelevant; rewrite array order is significant.
      const matches = file.endsWith('.json')
        ? isDeepStrictEqual(JSON.parse(actual).rewrites, JSON.parse(expected).rewrites)
        : actual === expected
      if (!matches) throw new Error(`Generated page metadata is stale: ${file}. Run npm run metadata:generate from app/.`)
    } else writeFileSync(path.join(root, file), expected)
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const check = process.argv.includes('--check')
  generatePageMetadata({ check })
  console.log(check ? 'Page rewrites and sitemap are current.' : 'Generated bilingual page rewrites and sitemap.')
}
