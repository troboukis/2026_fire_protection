import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { CookieConsentConfig } from 'vanilla-cookieconsent'

const mocks = vi.hoisted(() => ({
  run: vi.fn(), setLanguage: vi.fn(), acceptedCategory: vi.fn(),
  initGA: vi.fn(), disableGA: vi.fn(), trackPageView: vi.fn(),
}))
vi.mock('vanilla-cookieconsent', () => ({ ...mocks, showPreferences: vi.fn() }))
vi.mock('../analytics', () => ({ initGA: mocks.initGA, disableGA: mocks.disableGA, trackPageView: mocks.trackPageView }))

beforeEach(() => {
  vi.resetModules(); vi.clearAllMocks()
  vi.stubEnv('BASE_URL', '/')
  vi.stubGlobal('window', { location: { pathname: '/en/privacy', search: '?tag=a%20b&tag=a+b' } })
  vi.stubGlobal('document', { querySelectorAll: () => [] })
  mocks.run.mockResolvedValue(undefined); mocks.setLanguage.mockResolvedValue(true); mocks.acceptedCategory.mockReturnValue(false)
})
afterEach(() => { vi.unstubAllGlobals(); vi.unstubAllEnvs() })

const config = () => mocks.run.mock.calls[0][0] as CookieConsentConfig

describe('cookie consent language and analytics gates', () => {
  it.each([['/en/privacy', '/', 'en'], ['/privacy', '/', 'el'], ['/fire_protection/en/privacy', '/fire_protection/', 'en']])('uses the URL language at %s', async (pathname, base, language) => {
    vi.stubGlobal('window', { location: { pathname, search: '' } }); vi.stubEnv('BASE_URL', base)
    const { initCookieConsent } = await import('../cookieConsent')
    await initCookieConsent(); await initCookieConsent()
    expect(mocks.run).toHaveBeenCalledTimes(1)
    expect(config().language.default).toBe(language)
    expect(config().language.autoDetect).toBeUndefined()
    expect(config().mode).toBe('opt-in')
    expect(config().revision).toBe(1)
    expect(config().cookie).toMatchObject({ name: 'firewatch_consent', path: '/', sameSite: 'Lax', expiresAfterDays: 182 })
    expect(config().categories.necessary).toMatchObject({ enabled: true, readOnly: true })
    expect(config().categories.analytics.enabled).not.toBe(true)
  })

  it('updates language without accepting categories, restarting consent or invoking analytics', async () => {
    const { initCookieConsent, syncCookieConsentLanguage } = await import('../cookieConsent')
    await initCookieConsent(); await syncCookieConsentLanguage('el'); await syncCookieConsentLanguage('en')
    expect(mocks.run).toHaveBeenCalledTimes(1)
    expect(mocks.setLanguage.mock.calls).toEqual([['el'], ['en']])
    expect(mocks.acceptedCategory).not.toHaveBeenCalled()
    expect(mocks.initGA).not.toHaveBeenCalled(); expect(mocks.disableGA).not.toHaveBeenCalled(); expect(mocks.trackPageView).not.toHaveBeenCalled()
  })

  it('keeps the latest language when the first consent initialisation is still pending', async () => {
    let finish!: () => void
    mocks.run.mockImplementationOnce(() => new Promise<void>(resolve => { finish = resolve }))
    const { initCookieConsent, syncCookieConsentLanguage } = await import('../cookieConsent')
    const initial = initCookieConsent(), en = syncCookieConsentLanguage('en'), el = syncCookieConsentLanguage('el')
    finish(); await Promise.all([initial, en, el])
    expect(mocks.setLanguage.mock.calls.every(call => call[0] === 'el')).toBe(true)
    expect(mocks.setLanguage).toHaveBeenCalled(); expect(mocks.run).toHaveBeenCalledTimes(1)
  })

  it('retains analytics activation only for accepted statistics and disables it on rejection', async () => {
    const { initCookieConsent } = await import('../cookieConsent'); await initCookieConsent()
    config().onConsent?.({} as never)
    expect(mocks.disableGA).toHaveBeenCalledTimes(1); expect(mocks.initGA).not.toHaveBeenCalled()
    mocks.acceptedCategory.mockReturnValue(true)
    config().onFirstConsent?.({} as never)
    expect(mocks.initGA).toHaveBeenCalledTimes(1)
    expect(mocks.trackPageView).toHaveBeenCalledWith('/en/privacy?tag=a%20b&tag=a+b')
    mocks.acceptedCategory.mockReturnValue(false)
    config().onChange?.({ changedCategories: ['analytics'] } as never)
    expect(mocks.disableGA).toHaveBeenCalledTimes(2)
  })
})
