import { APP_TIME_ZONE, EMPTY_VALUE, LOCALES, type AppLanguage } from './languages'

type NumericValue = number | null | undefined
type DateValue = string | Date | null | undefined
type NumberOptions = Omit<Intl.NumberFormatOptions, 'style' | 'currency' | 'unit'>
type CurrencyOptions = Omit<Intl.NumberFormatOptions, 'style' | 'currency'>
type DateOptions = Pick<Intl.DateTimeFormatOptions, 'day' | 'month' | 'year' | 'weekday'>
type DateTimeOptions = DateOptions & Pick<Intl.DateTimeFormatOptions, 'hour' | 'minute' | 'second' | 'hour12' | 'timeZoneName'>

const dateParts: DateOptions = { day: '2-digit', month: 'short', year: 'numeric' }
const dateTimeParts: DateTimeOptions = { ...dateParts, hour: '2-digit', minute: '2-digit' }

function calendarDate(value: string): Date | null {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return null
  const date = new Date(`${value}T12:00:00Z`)
  return Number.isFinite(date.getTime()) && date.toISOString().slice(0, 10) === value ? date : null
}

function timestamp(value: DateValue): Date | null {
  if (value instanceof Date) return Number.isFinite(value.getTime()) ? value : null
  // A timestamp needs an explicit offset; do not guess the host's timezone or
  // invent a time for a date-only value. Reject normalized invalid dates too.
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,9})?)?(?:Z|[+-]\d{2}:\d{2})$/i.test(value)) return null
  if (!calendarDate(value.slice(0, 10))) return null
  const date = new Date(value)
  return Number.isFinite(date.getTime()) ? date : null
}

const finite = (value: NumericValue): value is number => typeof value === 'number' && Number.isFinite(value)

export function createFormatters(language: AppLanguage) {
  const locale = LOCALES[language]
  const number = new Intl.NumberFormat(locale)
  const currency = new Intl.NumberFormat(locale, { style: 'currency', currency: 'EUR' })
  const date = new Intl.DateTimeFormat(locale, { ...dateParts, timeZone: APP_TIME_ZONE })
  const day = new Intl.DateTimeFormat(locale, { ...dateParts, timeZone: 'UTC' })
  const dateTime = new Intl.DateTimeFormat(locale, { ...dateTimeParts, timeZone: APP_TIME_ZONE })

  return {
    number(value: NumericValue, options?: NumberOptions): string {
      if (!finite(value)) return EMPTY_VALUE
      return (options ? new Intl.NumberFormat(locale, { ...options, style: 'decimal' }) : number).format(value)
    },
    // Input is euros. The caller retains ownership of cents conversion, VAT,
    // denominators and rounding; switching language never changes the value.
    currency(value: NumericValue, options?: CurrencyOptions): string {
      if (!finite(value)) return EMPTY_VALUE
      return (options ? new Intl.NumberFormat(locale, { ...options, style: 'currency', currency: 'EUR' }) : currency).format(value)
    },
    date(value: DateValue, options?: DateOptions): string {
      const isCalendarDate = typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value)
      const parsed = isCalendarDate ? calendarDate(value) : timestamp(value)
      if (!parsed) return EMPTY_VALUE
      const formatter = options
        ? new Intl.DateTimeFormat(locale, { ...dateParts, ...options, timeZone: isCalendarDate ? 'UTC' : APP_TIME_ZONE })
        : isCalendarDate ? day : date
      return formatter.format(parsed)
    },
    dateTime(value: DateValue, options?: DateTimeOptions): string {
      const parsed = timestamp(value)
      if (!parsed) return EMPTY_VALUE
      return (options ? new Intl.DateTimeFormat(locale, { ...dateTimeParts, ...options, timeZone: APP_TIME_ZONE }) : dateTime).format(parsed)
    },
  }
}
