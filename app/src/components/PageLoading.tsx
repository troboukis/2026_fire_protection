import { useAppI18n } from '../i18n/useAppI18n'

export default function PageLoading() {
  const { t } = useAppI18n()
  return <main className="page-loading">{t('common:loadingPage')}</main>
}
