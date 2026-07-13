declare global {
  interface Window {
    dataLayer: unknown[]
    gtag: (...args: unknown[]) => void
  }
}

const GA_ID = import.meta.env.VITE_GA_MEASUREMENT_ID as string | undefined
const GA_SCRIPT_ID = 'firewatch-google-analytics'

function setGoogleAnalyticsDisabled(disabled: boolean) {
  if (!GA_ID) return
  ;(window as unknown as Record<string, unknown>)[`ga-disable-${GA_ID}`] = disabled
}

export function initGA() {
  if (!import.meta.env.PROD) return
  if (!GA_ID) return

  setGoogleAnalyticsDisabled(false)

  if (document.getElementById(GA_SCRIPT_ID)) return

  window.dataLayer = window.dataLayer || []
  window.gtag = function () {
    // eslint-disable-next-line prefer-rest-params
    window.dataLayer.push(arguments)
  }

  const script = document.createElement('script')
  script.id = GA_SCRIPT_ID
  script.async = true
  script.src = `https://www.googletagmanager.com/gtag/js?id=${GA_ID}`
  document.head.appendChild(script)

  window.gtag('js', new Date())
  window.gtag('config', GA_ID, { send_page_view: false })
}

export function disableGA() {
  if (!GA_ID) return
  setGoogleAnalyticsDisabled(true)

  if (typeof window.gtag === 'function') {
    window.gtag('consent', 'update', { analytics_storage: 'denied' })
  }
}

export function trackPageView(path: string) {
  if (!import.meta.env.PROD) return
  if (!GA_ID || typeof window.gtag !== 'function') return
  window.gtag('event', 'page_view', { page_path: path })
}
