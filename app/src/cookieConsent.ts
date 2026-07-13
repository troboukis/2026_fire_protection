import * as CookieConsent from 'vanilla-cookieconsent'
import 'vanilla-cookieconsent/dist/cookieconsent.css'
import { disableGA, initGA, trackPageView } from './analytics'

const ANALYTICS_CATEGORY = 'analytics'
let initialization: Promise<void> | null = null

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

  const privacyHref = `${import.meta.env.BASE_URL}privacy`

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
      default: 'el',
      translations: {
        el: {
          consentModal: {
            title: 'Επιλογές απορρήτου',
            description: 'Χρησιμοποιούμε προαιρετικά cookies του Google Analytics για να κατανοούμε τη χρήση του FireWatch. Με την «Αποδοχή όλων» ενεργοποιούνται τα στατιστικά cookies, ενώ με την «Απόρριψη όλων» παραμένουν απενεργοποιημένα.',
            acceptAllBtn: 'Αποδοχη ολων',
            acceptNecessaryBtn: 'Απορριψη ολων',
            showPreferencesBtn: 'Ρυθμισεις',
            footer: `<a href="${privacyHref}">Πολιτική απορρήτου και cookies</a>`,
          },
          preferencesModal: {
            title: 'Ρυθμίσεις απορρήτου',
            acceptAllBtn: 'Αποδοχη ολων',
            acceptNecessaryBtn: 'Απορριψη ολων',
            savePreferencesBtn: 'Αποθηκευση επιλογων',
            closeIconLabel: 'Κλείσιμο',
            serviceCounterLabel: 'Υπηρεσία|Υπηρεσίες',
            sections: [
              {
                title: 'Οι επιλογές σας',
                description: 'Μπορείτε να αλλάξετε την επιλογή σας οποιαδήποτε στιγμή από τον σύνδεσμο «Ρυθμίσεις cookies» στο κάτω μέρος της εφαρμογής.',
              },
              {
                title: 'Απαραίτητα',
                description: 'Αποθηκεύουν αποκλειστικά την επιλογή σας για τα cookies, ώστε να μη σας ζητείται σε κάθε επίσκεψη. Δεν μπορούν να απενεργοποιηθούν.',
                linkedCategory: 'necessary',
                cookieTable: {
                  caption: 'Απαραίτητα cookies',
                  headers: {
                    name: 'Όνομα',
                    desc: 'Σκοπός',
                    expiration: 'Διάρκεια',
                  },
                  body: [
                    {
                      name: 'firewatch_consent',
                      desc: 'Αποθήκευση των επιλογών συγκατάθεσης.',
                      expiration: '182 ημέρες',
                    },
                  ],
                },
              },
              {
                title: 'Στατιστικά',
                description: 'Το Google Analytics μάς βοηθά να κατανοούμε συγκεντρωτικά την επισκεψιμότητα και τη χρήση της εφαρμογής. Παραμένει απενεργοποιημένο μέχρι να το επιλέξετε.',
                linkedCategory: ANALYTICS_CATEGORY,
                cookieTable: {
                  caption: 'Cookies στατιστικών',
                  headers: {
                    name: 'Όνομα',
                    desc: 'Σκοπός',
                    expiration: 'Διάρκεια',
                  },
                  body: [
                    {
                      name: '_ga',
                      desc: 'Διάκριση επισκεπτών για συγκεντρωτικά στατιστικά.',
                      expiration: 'Έως 2 έτη',
                    },
                    {
                      name: '_ga_*',
                      desc: 'Διατήρηση της κατάστασης μιας επίσκεψης.',
                      expiration: 'Έως 2 έτη',
                    },
                  ],
                },
              },
              {
                title: 'Περισσότερες πληροφορίες',
                description: `Δείτε την <a href="${privacyHref}">Πολιτική απορρήτου και cookies</a> ή επικοινωνήστε στο troboukis[at]gmail[dot]com.`,
              },
            ],
          },
        },
      },
    },
  })

  return initialization
}

export function showCookiePreferences() {
  CookieConsent.showPreferences()
}
