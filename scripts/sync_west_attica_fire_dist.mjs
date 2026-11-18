import { cp, mkdir, readFile, rm, writeFile } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import { createHash } from 'node:crypto'
import { loadPageMetadata } from './page_metadata.mjs'
import { westAtticaMessages } from './west_attica_messages.mjs'

const scriptDir = path.dirname(fileURLToPath(import.meta.url))
const repoRoot = path.resolve(scriptDir, '..')
const sourceDir = path.resolve(process.env.FIREWATCH_STANDALONE_DIST || path.join(repoRoot, '../mega_fire_2026/app/dist'))
const targetDir = path.resolve(repoRoot, 'app/public/analysis/west-attica-fire-2026')
const appDir = path.resolve(repoRoot, 'app')
const expectedBase = '/analysis/west-attica-fire-2026/'
const publishedEventId = 'attica-boeotia-2026'

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

export function standaloneConsentScript(measurementId) {
  const { languages } = westAtticaMessages()
  return `(() => {
  const CookieConsent = window.CookieConsent;
  const GA_ID = ${JSON.stringify(measurementId)};
  const GA_SCRIPT_ID = 'firewatch-google-analytics';
  const ANALYTICS_CATEGORY = 'analytics';
  const currentLanguage = () => window.location.pathname.startsWith('/en/') ? 'en' : 'el';
  const footerTranslations = ${JSON.stringify(Object.fromEntries(Object.entries(languages).map(([language, value]) => [language, value.footer])))};
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

  function mountFireWatchFooter() {
    if (document.querySelector('.firewatch-footer-shell')) return;

    const shell = document.createElement('div');
    shell.className = 'firewatch-footer-shell';
    shell.innerHTML = \`<footer class="site-footer">
      © \${new Date().getFullYear()} FireWatch · <a href="https://troboukis.gr/" target="_blank" rel="noreferrer">Thanasis Troboukis</a> · <a href="/terms" data-firewatch-footer="terms">Όροι χρήσης</a> · <a href="/privacy" data-firewatch-footer="privacy">Απόρρητο &amp; cookies</a> · <button type="button" data-firewatch-footer="preferences">Ρυθμίσεις cookies</button>
    </footer>\`;

    shell.querySelector('button')?.addEventListener('click', () => {
      CookieConsent.showPreferences();
    });
    document.body.appendChild(shell);

    const syncFooterHeight = () => {
      document.documentElement.style.setProperty(
        '--firewatch-footer-height',
        \`\${shell.getBoundingClientRect().height}px\`,
      );
    };
    syncFooterHeight();

    if ('ResizeObserver' in window) {
      new ResizeObserver(syncFooterHeight).observe(shell);
    } else {
      window.addEventListener('resize', syncFooterHeight);
    }
  }

  function updateFooterLanguage() {
    const copy = footerTranslations[currentLanguage()];
    for (const element of document.querySelectorAll('[data-firewatch-footer]')) {
      const value = copy[element.getAttribute('data-firewatch-footer')];
      if (element.textContent !== value) element.textContent = value;
    }
  }
  function updateLanguage() {
    updateFooterLanguage();
    void CookieConsent.setLanguage(currentLanguage(), true);
  }
  window.addEventListener('firewatch-language-change', updateLanguage);
  window.addEventListener('popstate', updateLanguage);
  mountFireWatchFooter();
  updateFooterLanguage();

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
      default: currentLanguage(),
      translations: ${JSON.stringify(Object.fromEntries(Object.entries(languages).map(([language, value]) => [language, value.cookie])))},
    },
  }).then(() => syncGoogleAnalytics(true));
})();\n`
}

const standaloneFooterCss = `:root {
  --firewatch-footer-height: 0px;
}

.wwf-page {
  bottom: var(--firewatch-footer-height) !important;
}

/* Compensate for the shared SVG's padding to match the partner logos. */
.wwf-brand-logos__img--firewatch {
  height: 32px;
}

.wwf-analysis-panel__credits-img--firewatch {
  height: 24px;
}

@media (max-width: 520px) {
  .wwf-brand-logos__img--firewatch {
    height: 27px;
  }
}

.firewatch-footer-shell {
  --paper: #f7f5ee;
  --paper-2: #f1eee5;
  --ink: #111111;
  --ink-soft: #4d4d4d;
  --ink-faint: #89857c;
  --line: #cfc8bb;
  --accent: #d3482d;
  --font-sans: "IBM Plex Sans", "Helvetica Neue", Helvetica, Arial, sans-serif;
  --font-mono: "IBM Plex Mono", "SFMono-Regular", ui-monospace, Menlo, monospace;
  position: fixed;
  right: 0;
  bottom: 0;
  left: 0;
  z-index: 6;
  box-sizing: border-box;
  padding: 0 max(1rem, env(safe-area-inset-right)) calc(0.55rem + env(safe-area-inset-bottom)) max(1rem, env(safe-area-inset-left));
  border-top: 1px solid var(--line);
  background: #efede4;
}

.site-footer {
  margin-top: 0;
  padding: 0.35rem 0 0;
  color: var(--ink-faint);
  font-family: var(--font-mono);
  font-size: 0.72rem;
  line-height: 1.4;
  text-align: center;
}

.site-footer a {
  color: var(--ink-soft);
  text-decoration-thickness: 1px;
  text-underline-offset: 0.18em;
}

.site-footer button {
  margin: 0;
  padding: 0;
  border: 0;
  background: transparent;
  color: var(--ink-soft);
  font: inherit;
  text-decoration: underline;
  text-decoration-thickness: 1px;
  text-underline-offset: 0.18em;
  cursor: pointer;
}

.site-footer a:hover,
.site-footer a:focus-visible,
.site-footer button:hover,
.site-footer button:focus-visible {
  color: var(--accent);
  outline: none;
}
`

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
    filter: (source) => {
      if (path.basename(source) === '.DS_Store') return false

      const relativePath = path.relative(sourceDir, source)
      const pathParts = relativePath.split(path.sep)
      if (pathParts[0] === 'chalkidiki-2026') return false
      if (pathParts[0] === 'assets' && path.basename(source).startsWith('chalkidiki2026-')) return false
      if (pathParts[0] === 'data' && pathParts[1] && pathParts[1] !== publishedEventId) return false

      return true
    },
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
  const consentScript = standaloneConsentScript(measurementId)
  const consentVersion = createHash('sha256').update(consentScript).digest('hex').slice(0, 12)
  await writeFile(
    path.join(vendorDir, 'standalone-consent.js'),
    consentScript,
    'utf8',
  )
  await writeFile(
    path.join(vendorDir, 'standalone-footer.css'),
    standaloneFooterCss,
    'utf8',
  )

  const headMarkup = [
    `    <link rel="stylesheet" href="${expectedBase}firewatch/cookieconsent.css" />`,
    `    <link rel="stylesheet" href="${expectedBase}firewatch/standalone-footer.css" />`,
  ].join('\n')
  const bodyMarkup = [
    `    <script src="${expectedBase}firewatch/cookieconsent.umd.js"></script>`,
    `    <script src="${expectedBase}firewatch/standalone-consent.js?v=${consentVersion}"></script>`,
  ].join('\n')

  const { applyPageMetadata, getPageMetadata, fillStaticFallback } = loadPageMetadata()
  const metadata = getPageMetadata(expectedBase)
  if (!metadata) throw new Error('Missing standalone page metadata')
  const outputIndex = fillStaticFallback(applyPageMetadata(sourceIndex
    .replace('  </head>', `${headMarkup}\n  </head>`)
    .replace('  </body>', `${bodyMarkup}\n  </body>`), metadata), metadata)

  await writeFile(path.join(targetDir, 'index.html'), outputIndex, 'utf8')
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  await syncDist()
  console.log(`Synced ${sourceDir} to ${targetDir}`)
}
