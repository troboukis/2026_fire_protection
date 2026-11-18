import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { generatePageMetadata } from './generate_page_metadata.mjs'

const repository = fileURLToPath(new URL('../', import.meta.url))
const generatedFiles = ['app/public/sitemap.xml', 'app/vercel.json', 'vercel.json']

function fixture(t) {
  const root = mkdtempSync(path.join(tmpdir(), 'firewatch-metadata-check-'))
  t.after(() => rmSync(root, { recursive: true, force: true }))
  mkdirSync(path.join(root, 'app/public'), { recursive: true })
  for (const file of generatedFiles) {
    writeFileSync(path.join(root, file), readFileSync(path.join(repository, file)))
  }
  return root
}

for (const file of ['vercel.json', 'app/vercel.json']) {
  test(`accepts compact/reordered JSON in ${file} without rewriting it`, t => {
    const root = fixture(t)
    const target = path.join(root, file)
    const config = JSON.parse(readFileSync(target, 'utf8'))
    config.rewrites = config.rewrites.map(({ source, destination, ...rest }) => ({ destination, ...rest, source }))
    config.buildCommand = 'npm run build'
    for (const content of [JSON.stringify(config), JSON.stringify(config, null, 4).replaceAll('\n', '\r\n')]) {
      writeFileSync(target, content)
      generatePageMetadata({ root, check: true })
      assert.equal(readFileSync(target, 'utf8'), content)
    }
  })

  for (const change of ['wrong destination', 'missing route', 'changed priority']) {
    test(`rejects ${change} in ${file} while leaving the files untouched`, t => {
      const root = fixture(t)
      const target = path.join(root, file)
      const config = JSON.parse(readFileSync(target, 'utf8'))
      const index = config.rewrites.findIndex(rule => rule.source === '/en/contracts')
      assert.ok(index >= 0)
      if (change === 'wrong destination') config.rewrites[index].destination = '/index.html'
      if (change === 'missing route') config.rewrites.splice(index, 1)
      if (change === 'changed priority') config.rewrites.push(config.rewrites.splice(index, 1)[0])
      writeFileSync(target, JSON.stringify(config))
      const before = generatedFiles.map(file => readFileSync(path.join(root, file), 'utf8'))
      assert.throws(() => generatePageMetadata({ root, check: true }), error => error.message.includes(`stale: ${file}.`))
      assert.deepEqual(generatedFiles.map(file => readFileSync(path.join(root, file), 'utf8')), before)
    })
  }
}

test('rejects a sitemap missing the English URLs', t => {
  const root = fixture(t)
  const target = path.join(root, 'app/public/sitemap.xml')
  const original = readFileSync(target, 'utf8')
  writeFileSync(target, original.replace(/  <url>\s*<loc>[^<]*\/en\/[\s\S]*?<\/url>\n/g, ''))
  assert.notEqual(readFileSync(target, 'utf8'), original)
  assert.throws(() => generatePageMetadata({ root, check: true }), /stale: app\/public\/sitemap.xml/)
})

test('generation preserves custom routing and deployment settings and then passes verification', t => {
  const root = fixture(t)
  const target = path.join(root, 'app/vercel.json')
  const config = JSON.parse(readFileSync(target, 'utf8'))
  const custom = { source: '/old-press', destination: '/media/index.html' }
  config.rewrites.unshift(custom)
  config.headers = [{ source: '/assets/(.*)', headers: [{ key: 'X-Content-Type-Options', value: 'nosniff' }] }]
  const index = config.rewrites.findIndex(rule => rule.source === '/en/contracts')
  config.rewrites[index].destination = '/index.html'
  writeFileSync(target, JSON.stringify(config))
  generatePageMetadata({ root })
  generatePageMetadata({ root, check: true })
  const generated = JSON.parse(readFileSync(target, 'utf8'))
  assert.deepEqual(generated.rewrites[0], custom)
  assert.deepEqual(generated.headers, config.headers)
  assert.equal(generated.rewrites.find(rule => rule.source === '/en/contracts').destination, '/en/contracts/index.html')
})
