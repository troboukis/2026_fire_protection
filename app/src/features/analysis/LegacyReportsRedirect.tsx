import { Navigate, useLocation } from 'react-router-dom'

export default function LegacyReportsRedirect() {
  const location = useLocation()
  return <Navigate replace to={{
    pathname: location.pathname.replace(/^\/reports(?=\/|$)/, '/analysis'),
    search: location.search,
    hash: location.hash,
  }} />
}
