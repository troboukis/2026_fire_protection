import { describe, expect, it } from 'vitest'
import { parseDiavgeiaAmountCents, summarizeDiavgeiaAmounts } from './diavgeiaAmount'

describe('parseDiavgeiaAmountCents', () => {
  it.each([
    ['51.879,31', 5_187_931],
    ['538,77', 53_877],
    ['137135.39', 13_713_539],
    [' 11169.84 ', 1_116_984],
    ['1.234', 123_400],
    ['1.234.567', 123_456_700],
    ['12,3', 1_230],
    ['12.3', 1_230],
    ['12', 1_200],
    ['+12,34', 1_234],
  ])('parses amount %s to integer cents', (value, expected) => {
    expect(parseDiavgeiaAmountCents(value)).toBe(expected)
  })

  it('sums every beneficiary amount, including repeated values', () => {
    expect(parseDiavgeiaAmountCents("['2.807,86', '10.954,16']")).toBe(1_376_202)
    expect(parseDiavgeiaAmountCents("['992,00', '2.232,00', '992,00', '992,00', '992,00']")).toBe(620_000)
    expect(parseDiavgeiaAmountCents('["2.040,00", "488,38"]')).toBe(252_838)
    expect(parseDiavgeiaAmountCents('[0.1, 0.2]')).toBe(30)
  })

  it('preserves negative payments and recognizes zero as a present amount', () => {
    expect(parseDiavgeiaAmountCents('-85.940,84')).toBe(-8_594_084)
    expect(parseDiavgeiaAmountCents("['100,00', '-25,00']")).toBe(7_500)
    expect(parseDiavgeiaAmountCents('0,00')).toBe(0)
    expect(parseDiavgeiaAmountCents('0')).toBe(0)
    expect(parseDiavgeiaAmountCents("['0,00']")).toBe(0)
  })

  it.each([null, undefined, '', ' ', 'nan', 'None', 'null', 'NaT', '<NA>', '[]'])('rejects missing value %s', (value) => {
    expect(parseDiavgeiaAmountCents(value)).toBeNull()
  })

  it.each([
    'ποσό 1.000,00',
    '1.23,45',
    '1,234',
    '12.3456',
    '1,00 / 2,00',
    '{}',
    "['1,00', 'missing']",
    '["1,00", null]',
    '["1,00", false]',
    '[["1,00"]]',
    "['1,00',]",
  ])('rejects malformed or incomplete amount %s without taking a partial sum', (value) => {
    expect(parseDiavgeiaAmountCents(value)).toBeNull()
  })

  it('rounds finite native numbers and rejects unsafe or nonfinite values', () => {
    expect(parseDiavgeiaAmountCents(1.005)).toBe(101)
    expect(parseDiavgeiaAmountCents(-1.005)).toBe(-101)
    expect(parseDiavgeiaAmountCents(0)).toBe(0)
    expect(parseDiavgeiaAmountCents(Number.NaN)).toBeNull()
    expect(parseDiavgeiaAmountCents(Number.POSITIVE_INFINITY)).toBeNull()
    expect(parseDiavgeiaAmountCents(Number.MAX_SAFE_INTEGER)).toBeNull()
    expect(parseDiavgeiaAmountCents('9007199254740992')).toBeNull()
  })
})

describe('summarizeDiavgeiaAmounts', () => {
  it('sums document totals exactly and counts missing documents once', () => {
    const amounts = ['0,10', '0.20', "['2.807,86', '10.954,16']", '-85.940,84', '0,00', null, '[]', "['1,00', null]"]
    expect(summarizeDiavgeiaAmounts(amounts.map(spending_contractors_value => ({ spending_contractors_value })))).toEqual({
      totalCents: -7_217_852,
      missingCount: 3,
    })
  })

  it('includes every supplied document beyond the table page size', () => {
    const rows = Array.from({ length: 51 }, () => ({ spending_contractors_value: '0,01' }))
    expect(summarizeDiavgeiaAmounts(rows)).toEqual({ totalCents: 51, missingCount: 0 })
    expect(summarizeDiavgeiaAmounts([])).toEqual({ totalCents: 0, missingCount: 0 })
  })
})
