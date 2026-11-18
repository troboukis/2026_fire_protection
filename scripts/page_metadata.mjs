import { mkdirSync } from 'node:fs'
import { createRequire } from 'node:module'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const root = fileURLToPath(new URL('../', import.meta.url))
const require = createRequire(new URL('../app/package.json', import.meta.url))
export function loadPageMetadata() {
  const directory = path.join(root, 'state/translations/metadata-build')
  mkdirSync(directory, { recursive: true })
  const output = path.join(directory, 'metadata.cjs')
  require('esbuild').buildSync({
    stdin: { contents: `export * from './src/seo/pageMetadata'; export * from './src/seo/metadataHtml';`, resolveDir: path.join(root, 'app'), loader: 'ts' },
    bundle: true, platform: 'node', format: 'cjs', outfile: output,
  })
  delete require.cache[output]
  return require(output)
}
