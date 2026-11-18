import test from 'node:test'
import assert from 'node:assert/strict'
import vm from 'node:vm'
import { standaloneConsentScript } from './sync_west_attica_fire_dist.mjs'
import { replaceOnce } from './west_attica_language.mjs'

async function consent(pathname) {
  const configurations = [], changes = [], listeners = {}
  const footer = ['terms', 'privacy', 'preferences'].map(key => ({ textContent: '', getAttribute: () => key }))
  const cookie = {
    run(config) { configurations.push(config); return Promise.resolve() },
    acceptedCategory: () => false,
    setLanguage(language, force) { changes.push([language, force]); return Promise.resolve(true) },
  }
  const window = { CookieConsent: cookie, location: { pathname, protocol: 'https:', hostname: 'localhost' }, addEventListener: (name, callback) => { listeners[name] = callback } }
  const document = { querySelector: () => ({}), querySelectorAll: () => footer }
  vm.runInNewContext(standaloneConsentScript('G-LOCAL-TEST'), { window, document, Set, Date, encodeURIComponent })
  await Promise.resolve()
  return { window, footer, listeners, configurations, changes }
}

test('standalone English consent preserves the shared opt-in policy and cookie lifetime', async () => {
  const result = await consent('/en/analysis/west-attica-fire-2026/')
  assert.equal(result.configurations.length, 1)
  const config = result.configurations[0]
  assert.equal(config.language.default, 'en')
  assert.equal(config.mode, 'opt-in')
  assert.equal(config.revision, 1)
  assert.equal(config.cookie.name, 'firewatch_consent')
  assert.equal(config.cookie.path, '/')
  assert.equal(config.cookie.expiresAfterDays, 182)
  assert.equal(config.categories.necessary.readOnly, true)
  assert.equal(config.categories.analytics.autoClear.reloadPage, false)
  assert.match(config.language.translations.en.consentModal.footer, /\/en\/privacy/)
  assert.equal(result.footer[0].textContent, 'Terms of use')
})

test('language events and browser history update consent/footer without reinitializing or changing consent', async () => {
  const result = await consent('/analysis/west-attica-fire-2026/')
  assert.equal(result.configurations[0].language.default, 'el')
  result.window.location.pathname = '/en/analysis/west-attica-fire-2026/'
  result.listeners['firewatch-language-change']()
  assert.equal(result.footer[0].textContent, 'Terms of use')
  result.window.location.pathname = '/analysis/west-attica-fire-2026/'
  result.listeners.popstate()
  assert.equal(result.footer[0].textContent, 'Όροι χρήσης')
  assert.deepEqual(result.changes, [['en', true], ['el', true]])
  assert.equal(result.configurations.length, 1)
})

test('source integration stops on missing or ambiguous upstream anchors', () => {
  assert.equal(replaceOnce('before anchor after', 'anchor', 'translation', 'test'), 'before translation after')
  assert.throws(() => replaceOnce('changed source', 'anchor', 'translation', 'test'), /source changed/)
  assert.throws(() => replaceOnce('anchor anchor', 'anchor', 'translation', 'test'), /source changed/)
})
