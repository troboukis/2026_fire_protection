(() => {
  const CookieConsent = window.CookieConsent;
  const GA_ID = "G-P692F2LTH8";
  const GA_SCRIPT_ID = 'firewatch-google-analytics';
  const ANALYTICS_CATEGORY = 'analytics';
  const currentLanguage = () => window.location.pathname.startsWith('/en/') ? 'en' : 'el';
  const footerTranslations = {"el":{"terms":"Όροι χρήσης","privacy":"Απόρρητο & cookies","preferences":"Ρυθμίσεις cookies"},"en":{"terms":"Terms of use","privacy":"Privacy & cookies","preferences":"Cookie settings"}};
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
    shell.innerHTML = `<footer class="site-footer">
      © ${new Date().getFullYear()} FireWatch · <a href="https://troboukis.gr/" target="_blank" rel="noreferrer">Thanasis Troboukis</a> · <a href="/terms" data-firewatch-footer="terms">Όροι χρήσης</a> · <a href="/privacy" data-firewatch-footer="privacy">Απόρρητο &amp; cookies</a> · <button type="button" data-firewatch-footer="preferences">Ρυθμίσεις cookies</button>
    </footer>`;

    shell.querySelector('button')?.addEventListener('click', () => {
      CookieConsent.showPreferences();
    });
    document.body.appendChild(shell);

    const syncFooterHeight = () => {
      document.documentElement.style.setProperty(
        '--firewatch-footer-height',
        `${shell.getBoundingClientRect().height}px`,
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
      translations: {"el":{"consentModal":{"title":"Επιλογές απορρήτου","description":"Χρησιμοποιούμε προαιρετικά cookies της υπηρεσίας Google Analytics, ώστε να κατανοούμε καλύτερα πώς χρησιμοποιείται το FireWatch. Επιλέγοντας «Αποδοχή όλων», ενεργοποιείτε την επιλογή «Στατιστικά». Επιλέγοντας «Απόρριψη όλων», η επιλογή «Στατιστικά» παραμένει απενεργοποιημένη.","acceptAllBtn":"Αποδοχη ολων","acceptNecessaryBtn":"Απορριψη ολων","showPreferencesBtn":"Ρυθμισεις","footer":"<a target=\"_blank\" rel=\"noopener\" href=\"/privacy\">Πολιτική απορρήτου και cookies</a>"},"preferencesModal":{"title":"Ρυθμίσεις απορρήτου","acceptAllBtn":"Αποδοχη ολων","acceptNecessaryBtn":"Απορριψη ολων","savePreferencesBtn":"Αποθηκευση επιλογων","closeIconLabel":"Κλείσιμο","serviceCounterLabel":"Υπηρεσία|Υπηρεσίες","sections":[{"title":"Οι επιλογές σας","description":"Η επιλογή σας ισχύει και για την κύρια εφαρμογή του FireWatch."},{"title":"Απαραίτητα","description":"Αποθηκεύουν αποκλειστικά την επιλογή σας για τα cookies, ώστε να μη σας ζητείται σε κάθε επίσκεψη. Δεν μπορούν να απενεργοποιηθούν.","linkedCategory":"necessary","cookieTable":{"caption":"Απαραίτητα cookies","headers":{"name":"Όνομα","desc":"Σκοπός","expiration":"Διάρκεια"},"body":[{"name":"firewatch_consent","desc":"Αποθήκευση των επιλογών συγκατάθεσης.","expiration":"182 ημέρες"}]}},{"title":"Στατιστικά","description":"Το Google Analytics μάς βοηθά να κατανοούμε συγκεντρωτικά την επισκεψιμότητα και τη χρήση της εφαρμογής. Παραμένει απενεργοποιημένο μέχρι να το επιλέξετε.","linkedCategory":"analytics","cookieTable":{"caption":"Cookies στατιστικών","headers":{"name":"Όνομα","desc":"Σκοπός","expiration":"Διάρκεια"},"body":[{"name":"_ga","desc":"Διάκριση επισκεπτών για συγκεντρωτικά στατιστικά.","expiration":"Έως 2 έτη"},{"name":"_ga_*","desc":"Διατήρηση της κατάστασης μιας επίσκεψης.","expiration":"Έως 2 έτη"}]}},{"title":"Περισσότερες πληροφορίες","description":"Δείτε την <a target=\"_blank\" rel=\"noopener\" href=\"/privacy\">Πολιτική απορρήτου και cookies</a> ή επικοινωνήστε στο troboukis[at]gmail[dot]com."}]}},"en":{"consentModal":{"title":"Privacy options","description":"We use optional Google Analytics cookies to better understand how FireWatch is used. Selecting ‘Accept all’ enables the ‘Statistics’ option. Selecting ‘Reject all’ keeps ‘Statistics’ disabled.","acceptAllBtn":"Accept all","acceptNecessaryBtn":"Reject all","showPreferencesBtn":"Settings","footer":"<a target=\"_blank\" rel=\"noopener\" href=\"/en/privacy\">Privacy and cookies policy</a>"},"preferencesModal":{"title":"Privacy settings","acceptAllBtn":"Accept all","acceptNecessaryBtn":"Reject all","savePreferencesBtn":"Save preferences","closeIconLabel":"Close","serviceCounterLabel":"Service|Services","sections":[{"title":"Your choices","description":"Your selection also applies to the main FireWatch application."},{"title":"Necessary","description":"They store only your cookie preference so that you are not asked to confirm it on every visit. They cannot be disabled.","linkedCategory":"necessary","cookieTable":{"caption":"Necessary cookies","headers":{"name":"Name","desc":"Purpose","expiration":"Duration"},"body":[{"name":"firewatch_consent","desc":"Storage of consent preferences.","expiration":"182 days"}]}},{"title":"Statistics","description":"Google Analytics helps us understand aggregate traffic and use of the application. It remains disabled until you choose to enable it.","linkedCategory":"analytics","cookieTable":{"caption":"Statistics cookies","headers":{"name":"Name","desc":"Purpose","expiration":"Duration"},"body":[{"name":"_ga","desc":"Distinguishing between visitors for aggregate statistics.","expiration":"Up to 2 years"},{"name":"_ga_*","desc":"Maintaining session state.","expiration":"Up to 2 years"}]}},{"title":"Further information","description":"See <a target=\"_blank\" rel=\"noopener\" href=\"/en/privacy\">Privacy and cookies policy</a> or contact troboukis[at]gmail[dot]com."}]}}},
    },
  }).then(() => syncGoogleAnalytics(true));
})();
