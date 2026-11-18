import { useEffect, type ReactNode } from 'react'
import { useLocation } from 'react-router-dom'
import I18nProvider from '../i18n/I18nProvider'
import { localizeCookieConsentLinks, syncCookieConsentLanguage } from '../cookieConsent'
import { syncDocumentMetadata } from '../seo/documentMetadata'
import { languageFromPath } from './localizedHref'

export default function RouteLanguageProvider({ children }: { children: ReactNode }) {
  const { pathname } = useLocation()
  const language = languageFromPath(pathname)
  useEffect(() => { syncDocumentMetadata(pathname) }, [pathname])
  useEffect(() => {
    document.documentElement.lang = language
    localizeCookieConsentLinks(language)
    void syncCookieConsentLanguage(language)
  }, [language])
  return <I18nProvider language={language}>{children}</I18nProvider>
}
