import { syncDocumentMetadata } from '../seo/documentMetadata'
import { isStandaloneHref, languageFromPath, localizeHref } from './localizedHref'

// The archived map is a separate application. Add only language/navigation
// behavior around it; keep its compiled map, data, assets and React tree intact.
export function installStandaloneNavigation() {
  if (!isStandaloneHref(window.location.pathname)) return
  if (document.documentElement.hasAttribute('data-firewatch-language-navigation')) return
  document.documentElement.setAttribute('data-firewatch-language-navigation', '')
  const selector = '[data-firewatch-language-switcher]'

  function update() {
    const language = languageFromPath(window.location.pathname)
    syncDocumentMetadata(window.location.pathname)
    for (const anchor of document.querySelectorAll<HTMLAnchorElement>('a[href]')) {
      if (anchor.closest(selector)) continue
      const href = anchor.getAttribute('href')!
      const next = localizeHref(href, language)
      if (href !== next) anchor.setAttribute('href', next)
    }

    const footer = document.querySelector('.firewatch-footer-shell .site-footer')
    if (!footer) return
    let control = footer.querySelector<HTMLSpanElement>(selector)
    if (!control) {
      control = document.createElement('span')
      control.className = 'language-switcher'
      control.dataset.firewatchLanguageSwitcher = ''
      control.setAttribute('role', 'group')
      for (const target of ['el', 'en'] as const) {
        const anchor = document.createElement('a')
        anchor.dataset.language = target
        anchor.lang = target
        anchor.hreflang = target
        anchor.textContent = target === 'el' ? 'Ελληνικά' : 'English'
        control.appendChild(anchor)
      }
      footer.append(' · ', control)
      const style = document.createElement('style')
      style.textContent = '.firewatch-footer-shell .language-switcher{display:inline-flex;gap:.6rem;white-space:nowrap}.firewatch-footer-shell .language-switcher a{display:inline-flex;align-items:center;min-height:32px;color:inherit;font:inherit}.firewatch-footer-shell .language-switcher a[aria-current="true"]{font-weight:700;text-decoration:none}'
      document.head.appendChild(style)
    }
    control.setAttribute('aria-label', language === 'en' ? 'Language' : 'Γλώσσα')
    const href = window.location.pathname + window.location.search + window.location.hash
    for (const anchor of control.querySelectorAll<HTMLAnchorElement>('a')) {
      const target = anchor.dataset.language === 'en' ? 'en' : 'el'
      anchor.href = localizeHref(href, target)
      if (language === target) anchor.setAttribute('aria-current', 'true')
      else anchor.removeAttribute('aria-current')
    }
  }

  document.addEventListener('click', event => {
    if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return
    const anchor = event.target instanceof Element ? event.target.closest<HTMLAnchorElement>(`${selector} a`) : null
    if (!anchor) return
    event.preventDefault()
    if (anchor.getAttribute('aria-current') === 'true') return
    // A full reload would throw away the map's local state. Preserve the current
    // history state and announce the language change for the source integration.
    window.history.pushState(window.history.state, '', anchor.getAttribute('href'))
    update()
    window.dispatchEvent(new CustomEvent('firewatch-language-change', { detail: languageFromPath(window.location.pathname) }))
  })
  window.addEventListener('popstate', update)
  new MutationObserver(update).observe(document.body, { childList: true, subtree: true })
  update()
}

if (typeof window !== 'undefined' && typeof document !== 'undefined') installStandaloneNavigation()
