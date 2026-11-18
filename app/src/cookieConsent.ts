import { cookieConsentTranslation } from './i18n/cookieConsentTranslation'
import { logError } from './lib/logger'
import * as CookieConsent from 'vanilla-cookieconsent'
import 'vanilla-cookieconsent/dist/cookieconsent.css'
import { disableGA, initGA, trackPageView } from './analytics'
import { documentHref, languageFromPath, localizeHref } from './routing/localizedHref'
import type { AppLanguage } from './i18n/languages'

const ANALYTICS_CATEGORY = 'analytics'
let initialization: Promise<void> | null = null
let languageUpdate = Promise.resolve()
let requestedLanguage: AppLanguage = 'el'

export function syncCookieConsentLanguage(language: AppLanguage): Promise<void> {
  requestedLanguage = language
  if (!initialization) return Promise.resolve()
  // Serialise updates and apply the latest URL language after initialisation.
  // Changing copy never accepts categories or restarts the consent instance.
  languageUpdate = languageUpdate.then(async () => {
    await initialization
    const next = requestedLanguage
    await CookieConsent.setLanguage(next)
    localizeCookieConsentLinks(next)
  }).catch(error => { logError('[CookieConsent] language update failed', error) })
  return languageUpdate
}

export function localizeCookieConsentLinks(language: AppLanguage) {
  for (const anchor of document.querySelectorAll<HTMLAnchorElement>('#cc-main a[href]')) {
    const href = anchor.getAttribute('href')!
    const base = import.meta.env.BASE_URL.replace(/\/$/, '')
    const appHref = base && href.startsWith(`${base}/`) ? href.slice(base.length) : href
    const localized = localizeHref(appHref, language)
    if (localized !== appHref) anchor.setAttribute('href', documentHref(localized, import.meta.env.BASE_URL))
  }
}

function syncGoogleAnalytics(trackCurrentPage: boolean) {
  if (CookieConsent.acceptedCategory(ANALYTICS_CATEGORY)) {
    initGA()
    if (trackCurrentPage) {
      trackPageView(`${window.location.pathname}${window.location.search}`)
    }
    return
  }

  disableGA()
}

export function initCookieConsent() {
  if (initialization) return initialization

  const routePath = window.location.pathname.slice(import.meta.env.BASE_URL.replace(/\/$/, '').length) || '/'

  initialization = CookieConsent.run({
    mode: 'opt-in',
    revision: 1,
    cookie: {
      name: 'firewatch_consent',
      path: '/',
      sameSite: 'Lax',
      secure: import.meta.env.PROD,
      expiresAfterDays: 182,
    },
    guiOptions: {
      consentModal: {
        layout: 'box inline',
        position: 'bottom center',
        equalWeightButtons: true,
        flipButtons: false,
      },
      preferencesModal: {
        layout: 'box',
        equalWeightButtons: true,
        flipButtons: false,
      },
    },
    onFirstConsent: () => syncGoogleAnalytics(true),
    onModalReady: () => {
      const pathname = window.location.pathname.slice(import.meta.env.BASE_URL.replace(/\/$/, '').length) || '/'
      localizeCookieConsentLinks(languageFromPath(pathname))
    },
    onConsent: () => syncGoogleAnalytics(false),
    onChange: ({ changedCategories }) => {
      if (changedCategories.includes(ANALYTICS_CATEGORY)) syncGoogleAnalytics(true)
    },
    categories: {
      necessary: {
        enabled: true,
        readOnly: true,
      },
      analytics: {
        autoClear: {
          cookies: [
            { name: /^_ga/ },
            { name: '_gid' },
            { name: '_gat' },
          ],
          reloadPage: false,
        },
        services: {
          googleAnalytics: {
            label: 'Google Analytics',
          },
        },
      },
    },
    language: {
      default: languageFromPath(routePath),
      translations: { el: cookieConsentTranslation('el'), en: cookieConsentTranslation('en') },
    },
  })

  return initialization
}

export function showCookiePreferences() {
  CookieConsent.showPreferences()
}
