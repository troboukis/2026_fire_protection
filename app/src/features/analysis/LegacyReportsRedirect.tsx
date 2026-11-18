import { useEffect } from 'react'
import { Navigate, useLocation } from 'react-router-dom'
import { documentHref, isStandaloneHref, languageFromPath, localizeHref, stripLanguagePath } from '../../routing/localizedHref'

function DocumentRedirect({ href }: { href: string }) {
  useEffect(() => { window.location.replace(documentHref(href, import.meta.env.BASE_URL)) }, [href])
  return null
}

export default function LegacyReportsRedirect() {
  const location = useLocation()
  const pathname = localizeHref(
    stripLanguagePath(location.pathname).replace(/^\/reports(?=\/|$)/, '/analysis'),
    languageFromPath(location.pathname),
  )
  if (isStandaloneHref(pathname)) return <DocumentRedirect href={pathname + location.search + location.hash} />
  return <Navigate replace to={{
    pathname,
    search: location.search,
    hash: location.hash,
  }} />
}
