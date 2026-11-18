import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { localizeCookieConsentLinks } from '../cookieConsent'

vi.mock('vanilla-cookieconsent', () => ({ run: vi.fn(), showPreferences: vi.fn() }))
vi.mock('../analytics', () => ({ disableGA: vi.fn(), initGA: vi.fn(), trackPageView: vi.fn() }))

function anchorsFor(hrefs: string[]) {
  const anchors = hrefs.map(initial => {
    let href = initial
    return { getAttribute: () => href, setAttribute: (_name: string, value: string) => { href = value }, get href() { return href } }
  })
  vi.stubGlobal('document', { querySelectorAll: () => anchors })
  return anchors
}

beforeEach(() => vi.stubEnv('BASE_URL', '/'))
afterEach(() => { vi.unstubAllGlobals(); vi.unstubAllEnvs() })

describe('privacy links in cookie dialogs', () => {
  it('keeps English legal links in the app while leaving sources and assets unchanged', () => {
    const anchors = anchorsFor(['/privacy#cookies', '/terms', 'https://example.com/privacy', '/downloads/policy.pdf'])
    localizeCookieConsentLinks('en')
    expect(anchors.map(anchor => anchor.href)).toEqual(['/en/privacy#cookies', '/en/terms', 'https://example.com/privacy', '/downloads/policy.pdf'])
  })
  it('returns to the Greek URLs without rewriting queries or hashes', () => {
    const anchors = anchorsFor(['/en/privacy?tag=a%20b&tag=a+b#cookies'])
    localizeCookieConsentLinks('el')
    expect(anchors[0].href).toBe('/privacy?tag=a%20b&tag=a+b#cookies')
  })
  it('keeps a configured base path exactly once in either language', () => {
    vi.stubEnv('BASE_URL', '/fire_protection/')
    const anchors = anchorsFor(['/fire_protection/privacy#cookies'])
    localizeCookieConsentLinks('en')
    expect(anchors[0].href).toBe('/fire_protection/en/privacy#cookies')
    localizeCookieConsentLinks('el')
    expect(anchors[0].href).toBe('/fire_protection/privacy#cookies')
  })
})
