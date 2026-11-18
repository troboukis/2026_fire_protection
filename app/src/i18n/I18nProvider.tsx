import { useMemo, type ReactNode } from 'react'
import { I18nextProvider } from 'react-i18next'
import { createAppI18n } from './instance'
import { DEFAULT_LANGUAGE, type AppLanguage } from './languages'

type Props = {
  language?: AppLanguage
  children: ReactNode
}

export default function I18nProvider({ language = DEFAULT_LANGUAGE, children }: Props) {
  const instance = useMemo(() => createAppI18n(language), [language])
  // Keep the provider and children unkeyed. Changing the context must not
  // remount pages or discard local filters when URL language switching is added.
  return <I18nextProvider i18n={instance} defaultNS="common">{children}</I18nextProvider>
}
