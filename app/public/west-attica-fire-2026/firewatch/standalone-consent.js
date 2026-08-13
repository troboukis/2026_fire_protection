(() => {
  const CookieConsent = window.CookieConsent;
  const GA_ID = "G-P692F2LTH8";
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
})();
