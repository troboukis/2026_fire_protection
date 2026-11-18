import { useCallback } from 'react'
import { useNavigate, type NavigateFunction, type NavigateOptions, type To } from 'react-router-dom'
import { useAppI18n } from '../i18n/useAppI18n'
import { localizeTo } from './localizedHref'

export function useLocalizedNavigate(): NavigateFunction {
  const navigate = useNavigate()
  const { language } = useAppI18n()
  return useCallback(((to: To | number, options?: NavigateOptions) => {
    return typeof to === 'number' ? navigate(to) : navigate(localizeTo(to, language), options)
  }) as NavigateFunction, [navigate, language])
}
