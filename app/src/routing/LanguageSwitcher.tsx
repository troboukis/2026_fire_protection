import { Link, useLocation } from 'react-router-dom'
import { useAppI18n } from '../i18n/useAppI18n'
import { localizeHref } from './localizedHref'

export default function LanguageSwitcher() {
  const location = useLocation()
  const { language, t } = useAppI18n()
  const current = location.pathname + location.search + location.hash
  return (
    <span className="language-switcher" role="group" aria-label={t('navigation:language')}>
      {(['el', 'en'] as const).map(target => (
        <Link
          key={target}
          to={localizeHref(current, target)}
          state={location.state}
          lang={target}
          hrefLang={target}
          aria-current={language === target ? 'true' : undefined}
          preventScrollReset
          onClick={event => {
            if (language === target && event.button === 0 && !event.metaKey && !event.ctrlKey && !event.shiftKey && !event.altKey) event.preventDefault()
          }}
        >
          {target === 'el' ? 'Ελληνικά' : 'English'}
        </Link>
      ))}
    </span>
  )
}
