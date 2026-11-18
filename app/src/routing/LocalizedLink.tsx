import { forwardRef } from 'react'
import { Link, NavLink, useLocation, type LinkProps, type NavLinkProps } from 'react-router-dom'
import { useAppI18n } from '../i18n/useAppI18n'
import { isStandaloneHref, localizeTo, stripLanguagePath } from './localizedHref'

const pathnameOf = (to: LinkProps['to']) => typeof to === 'string' ? to.split(/[?#]/, 1)[0] : to.pathname ?? ''

export const LocalizedLink = forwardRef<HTMLAnchorElement, LinkProps>(function LocalizedLink({ to, reloadDocument, ...props }, ref) {
  const { language } = useAppI18n()
  return <Link {...props} ref={ref} to={localizeTo(to, language)} reloadDocument={reloadDocument ?? isStandaloneHref(pathnameOf(to))} />
})

export const LocalizedNavLink = forwardRef<HTMLAnchorElement, NavLinkProps>(function LocalizedNavLink({ to, end, ...props }, ref) {
  const { language } = useAppI18n()
  const location = useLocation()
  const root = stripLanguagePath(pathnameOf(to)) === '/'
  let localized = localizeTo(to, language)
  // /en and /en/ are both valid roots. Match the current spelling so the home
  // tab remains active on either form without being active on nested pages.
  if (root && location.pathname === '/en') {
    localized = typeof localized === 'string' ? localized.replace(/^\/en\//, '/en') : { ...localized, pathname: '/en' }
  }
  return <NavLink {...props} ref={ref} to={localized} end={end ?? root} />
})
