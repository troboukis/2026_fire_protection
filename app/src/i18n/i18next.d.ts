import 'i18next'
import type { el } from './locales/el'

declare module 'i18next' {
  interface CustomTypeOptions {
    defaultNS: 'common'
    resources: typeof el
    returnNull: false
    strictKeyChecks: true
  }
}
