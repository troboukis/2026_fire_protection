import { cp, mkdir, readFile, rm, writeFile } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import path from 'node:path'

const scriptDir = path.dirname(fileURLToPath(import.meta.url))
const repoRoot = path.resolve(scriptDir, '..')
const sourceDir = path.resolve(repoRoot, '../mega_fire_2026/app/dist')
const targetDir = path.resolve(repoRoot, 'app/public/west-attica-fire-2026')
const appDir = path.resolve(repoRoot, 'app')
const expectedBase = '/west-attica-fire-2026/'

function parseEnvValue(contents, key) {
  for (const rawLine of contents.split(/\r?\n/)) {
    const line = rawLine.trim()
    if (!line || line.startsWith('#')) continue
    const separator = line.indexOf('=')
    if (separator === -1 || line.slice(0, separator).trim() !== key) continue
    return line.slice(separator + 1).trim().replace(/^(["'])(.*)\1$/, '$2')
  }
  return null
}

async function readGoogleAnalyticsId() {
  if (process.env.VITE_GA_MEASUREMENT_ID) return process.env.VITE_GA_MEASUREMENT_ID

  for (const envName of ['.env.local', '.env']) {
    try {
      const contents = await readFile(path.join(appDir, envName), 'utf8')
      const value = parseEnvValue(contents, 'VITE_GA_MEASUREMENT_ID')
      if (value) return value
    } catch (error) {
      if (error?.code !== 'ENOENT') throw error
    }
  }

  throw new Error('VITE_GA_MEASUREMENT_ID is required to publish the standalone map.')
}

function standaloneConsentScript(measurementId) {
  return `(() => {
  const CookieConsent = window.CookieConsent;
  const GA_ID = ${JSON.stringify(measurementId)};
  const GA_SCRIPT_ID = 'firewatch-google-analytics';
  const ANALYTICS_CATEGORY = 'analytics';
  const analyticsEnabled = new Set([
    'fire-watch-app.gr',
    'www.fire-watch-app.gr',
  ]).has(window.location.hostname);

  function setGoogleAnalyticsDisabled(disabled) {
    window['ga-disable-' + GA_ID] = disabled;
  }

  function initGoogleAnalytics() {
    if (!analyticsEnabled) return;
    setGoogleAnalyticsDisabled(false);
    if (document.getElementById(GA_SCRIPT_ID)) return;

    window.dataLayer = window.dataLayer || [];
    window.gtag = function () { window.dataLayer.push(arguments); };

    const script = document.createElement('script');
    script.id = GA_SCRIPT_ID;
    script.async = true;
    script.src = 'https://www.googletagmanager.com/gtag/js?id=' + encodeURIComponent(GA_ID);
    document.head.appendChild(script);

    window.gtag('js', new Date());
    window.gtag('config', GA_ID, { send_page_view: false });
  }

  function disableGoogleAnalytics() {
    if (!analyticsEnabled) return;
    setGoogleAnalyticsDisabled(true);
    if (typeof window.gtag === 'function') {
      window.gtag('consent', 'update', { analytics_storage: 'denied' });
    }
  }

  function trackCurrentPage() {
    if (!analyticsEnabled) return;
    if (typeof window.gtag !== 'function') return;
    window.gtag('event', 'page_view', {
      page_path: window.location.pathname + window.location.search,
    });
  }

  function syncGoogleAnalytics(trackPage) {
    if (CookieConsent.acceptedCategory(ANALYTICS_CATEGORY)) {
      initGoogleAnalytics();
      if (trackPage) trackCurrentPage();
      return;
    }
    disableGoogleAnalytics();
  }

  void CookieConsent.run({
    mode: 'opt-in',
    revision: 1,
    cookie: {
      name: 'firewatch_consent',
      path: '/',
      sameSite: 'Lax',
      secure: window.location.protocol === 'https:',
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
      if (changedCategories.includes(ANALYTICS_CATEGORY)) syncGoogleAnalytics(true);
    },
    categories: {
      necessary: { enabled: true, readOnly: true },
      analytics: {
        autoClear: {
          cookies: [{ name: /^_ga/ }, { name: '_gid' }, { name: '_gat' }],
          reloadPage: false,
        },
        services: { googleAnalytics: { label: 'Google Analytics' } },
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
            footer: '<a href="/privacy" target="_blank" rel="noopener">Πολιτική απορρήτου και cookies</a>',
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
                description: 'Η επιλογή σας ισχύει και για την κύρια εφαρμογή του FireWatch.',
              },
              {
                title: 'Απαραίτητα',
                description: 'Αποθηκεύουν αποκλειστικά την επιλογή σας για τα cookies, ώστε να μη σας ζητείται σε κάθε επίσκεψη. Δεν μπορούν να απενεργοποιηθούν.',
                linkedCategory: 'necessary',
                cookieTable: {
                  caption: 'Απαραίτητα cookies',
                  headers: { name: 'Όνομα', desc: 'Σκοπός', expiration: 'Διάρκεια' },
                  body: [{
                    name: 'firewatch_consent',
                    desc: 'Αποθήκευση των επιλογών συγκατάθεσης.',
                    expiration: '182 ημέρες',
                  }],
                },
              },
              {
                title: 'Στατιστικά',
                description: 'Το Google Analytics μάς βοηθά να κατανοούμε συγκεντρωτικά την επισκεψιμότητα και τη χρήση της εφαρμογής. Παραμένει απενεργοποιημένο μέχρι να το επιλέξετε.',
                linkedCategory: ANALYTICS_CATEGORY,
                cookieTable: {
                  caption: 'Cookies στατιστικών',
                  headers: { name: 'Όνομα', desc: 'Σκοπός', expiration: 'Διάρκεια' },
                  body: [
                    { name: '_ga', desc: 'Διάκριση επισκεπτών για συγκεντρωτικά στατιστικά.', expiration: 'Έως 2 έτη' },
                    { name: '_ga_*', desc: 'Διατήρηση της κατάστασης μιας επίσκεψης.', expiration: 'Έως 2 έτη' },
                  ],
                },
              },
              {
                title: 'Περισσότερες πληροφορίες',
                description: 'Δείτε την <a href="/privacy" target="_blank" rel="noopener">Πολιτική απορρήτου και cookies</a> ή επικοινωνήστε στο troboukis[at]gmail[dot]com.',
              },
            ],
          },
        },
      },
    },
  }).then(() => syncGoogleAnalytics(true));
})();\n`
}

async function syncDist() {
  const sourceIndex = await readFile(path.join(sourceDir, 'index.html'), 'utf8')

  if (!sourceIndex.includes(`${expectedBase}assets/`)) {
    throw new Error(
      `The mega_fire_2026 dist is not built for ${expectedBase}. Run npm run build in mega_fire_2026/app first.`,
    )
  }

  const measurementId = await readGoogleAnalyticsId()

  await rm(targetDir, { recursive: true, force: true })
  await mkdir(targetDir, { recursive: true })
  await cp(sourceDir, targetDir, {
    recursive: true,
    force: true,
    filter: (source) => path.basename(source) !== '.DS_Store',
  })

  const vendorDir = path.join(targetDir, 'firewatch')
  await mkdir(vendorDir, { recursive: true })
  await cp(
    path.join(appDir, 'node_modules/vanilla-cookieconsent/dist/cookieconsent.css'),
    path.join(vendorDir, 'cookieconsent.css'),
  )
  await cp(
    path.join(appDir, 'node_modules/vanilla-cookieconsent/dist/cookieconsent.umd.js'),
    path.join(vendorDir, 'cookieconsent.umd.js'),
  )
  await writeFile(
    path.join(vendorDir, 'standalone-consent.js'),
    standaloneConsentScript(measurementId),
    'utf8',
  )

  const headMarkup = [
    '    <meta name="description" content="Διαδραστικός χάρτης της μεγάλης πυρκαγιάς στη Δυτική Αττική το 2026." />',
    `    <link rel="canonical" href="https://fire-watch-app.gr${expectedBase}" />`,
    `    <link rel="stylesheet" href="${expectedBase}firewatch/cookieconsent.css" />`,
  ].join('\n')
  const bodyMarkup = [
    `    <script src="${expectedBase}firewatch/cookieconsent.umd.js"></script>`,
    `    <script src="${expectedBase}firewatch/standalone-consent.js"></script>`,
  ].join('\n')

  const outputIndex = sourceIndex
    .replace('  </head>', `${headMarkup}\n  </head>`)
    .replace('  </body>', `${bodyMarkup}\n  </body>`)

  await writeFile(path.join(targetDir, 'index.html'), outputIndex, 'utf8')
}

await syncDist()
console.log(`Synced ${sourceDir} to ${targetDir}`)
