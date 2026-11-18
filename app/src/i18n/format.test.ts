import { describe, expect, it } from 'vitest'
import { createFormatters } from './format'

const el = createFormatters('el')
const en = createFormatters('en')

describe('localized numbers and euros', () => {
  it('changes separators without changing values or converting euros', () => {
    expect(el.number(1234567.89)).toBe('1.234.567,89')
    expect(en.number(1234567.89)).toBe('1,234,567.89')
    expect(el.currency(1234.56)).toBe('1.234,56\u00a0€')
    expect(en.currency(1234.56)).toBe('€1,234.56')
    expect(el.currency(123456)).toBe('123.456,00\u00a0€')
    expect(en.currency(123456)).toBe('€123,456.00')
  })

  it('keeps zero and negative values distinct from missing amounts', () => {
    expect(el.currency(0)).toBe('0,00\u00a0€')
    expect(en.currency(0)).toBe('€0.00')
    expect(el.currency(-123.45)).toBe('-123,45\u00a0€')
    expect(en.currency(-123.45)).toBe('-€123.45')
  })

  it('supports explicit display precision without changing the currency', () => {
    expect(el.currency(1234.56, { maximumFractionDigits: 0 })).toBe('1.235\u00a0€')
    expect(en.currency(1234.56, { maximumFractionDigits: 0 })).toBe('€1,235')
    expect(en.number(1.25, { minimumFractionDigits: 3 })).toBe('1.250')
    // @ts-expect-error Currency overrides are not permitted; runtime also keeps EUR.
    expect(en.currency(1234.56, { currency: 'USD' })).toBe('€1,234.56')
  })

  it.each([null, undefined, NaN, Infinity, -Infinity])('shows a dash for unavailable numeric input %s', (value) => {
    for (const format of [el, en]) {
      expect(format.number(value)).toBe('—')
      expect(format.currency(value)).toBe('—')
    }
  })
})

describe('calendar dates and Athens timestamps', () => {
  it('keeps date-only values on their original calendar day', () => {
    const options = { day: '2-digit', month: '2-digit', year: 'numeric' } as const
    expect(el.date('2026-01-01', options)).toBe('01/01/2026')
    expect(en.date('2026-01-01', options)).toBe('01/01/2026')
    expect(en.date('2024-02-29', options)).toBe('29/02/2024')
    expect(en.date('2026-12-31', options)).toBe('31/12/2026')
  })

  it('uses Athens time across the day boundary', () => {
    expect(en.dateTime('2026-01-01T23:30:00Z')).toBe('02 Jan 2026, 01:30')
    expect(en.date('2026-01-01T23:30:00Z')).toBe('02 Jan 2026')
  })

  it('applies winter and summer offsets without depending on the browser timezone', () => {
    expect(en.dateTime('2026-01-01T12:00:00Z')).toBe('01 Jan 2026, 14:00')
    expect(en.dateTime('2026-07-01T12:00:00Z')).toBe('01 Jul 2026, 15:00')
    expect(en.dateTime('2026-03-29T00:30:00Z')).toBe('29 Mar 2026, 02:30')
    expect(en.dateTime('2026-03-29T01:30:00Z')).toBe('29 Mar 2026, 04:30')
  })

  it('renders equivalent ISO offsets identically and leaves Date objects untouched', () => {
    expect(en.dateTime('2026-07-01T12:00:00Z')).toBe(en.dateTime('2026-07-01T15:00:00+03:00'))
    const date = new Date('2026-07-01T12:00:00Z')
    const original = date.getTime()
    expect(en.dateTime(date)).toBe('01 Jul 2026, 15:00')
    expect(date.getTime()).toBe(original)
  })

  it('matches the existing Greek layout timestamp formatting exactly', () => {
    const iso = '2026-11-17T12:30:00Z'
    const existing = new Intl.DateTimeFormat('el-GR', {
      day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit', timeZone: 'Europe/Athens',
    }).format(new Date(iso))
    expect(el.dateTime(iso)).toBe(existing)
  })

  it.each([null, undefined, '', 'not a date', '2025-02-29', '2026-02-30', '2026-13-01', '2026-11', '2026-02-30T12:00:00Z', new Date(NaN)])('does not invent a date for %s', (value) => {
    expect(el.date(value)).toBe('—')
    expect(en.dateTime(value)).toBe('—')
  })

  it.each(['2026-11-17', '2026-11-17T12:00:00'])('does not invent a timestamp or timezone for %s', (value) => {
    expect(en.dateTime(value)).toBe('—')
  })
})
