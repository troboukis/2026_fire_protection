import { describe, expect, it } from 'vitest'

import { buildAttachmentUrl, cleanValue, extractKimdisRefNumber } from './contractDocument'

describe('contractDocument helpers', () => {
  it('normalizes empty-like values to null', () => {
    expect(cleanValue(undefined)).toBeNull()
    expect(cleanValue(null)).toBeNull()
    expect(cleanValue('   ')).toBeNull()
    expect(cleanValue('—')).toBeNull()
    expect(cleanValue(' 26SYMV018574528 ')).toBe('26SYMV018574528')
  })

  it('extracts KIMDIS reference number from referenceNumber or contractNumber', () => {
    expect(extractKimdisRefNumber({
      referenceNumber: '26SYMV018574528',
      contractNumber: '',
    })).toBe('26SYMV018574528')

    expect(extractKimdisRefNumber({
      referenceNumber: 'Σύμβαση: 26symv018574528',
      contractNumber: 'irrelevant',
    })).toBe('26SYMV018574528')

    expect(extractKimdisRefNumber({
      referenceNumber: '',
      contractNumber: ' 26SYMV018509045 ',
    })).toBe('26SYMV018509045')
  })

  it('builds attachment URL with encoded reference', () => {
    expect(buildAttachmentUrl('26SYMV018574528'))
      .toBe('https://cerpp.eprocurement.gov.gr/khmdhs-opendata/contract/attachment/26SYMV018574528')
  })
})
