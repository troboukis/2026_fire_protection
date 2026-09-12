import { describe, expect, it } from 'vitest'
import { contractDecisions, defaultContractId, report, resolveSelection } from './antineroReport'

describe('AntiNERO report navigation and publication data', () => {
  it('opens every archived Aigaleo decision with its reviewed summary in date order', () => {
    const items = contractDecisions(defaultContractId)
    expect(items).toHaveLength(27)
    expect(items.every(item => item.reviewed && item.summary)).toBe(true)
    expect(items.map(item => item.date)).toEqual(items.map(item => item.date).sort())
    for (const item of items) {
      expect(resolveSelection(item.id, null).contract.id).toBe(defaultContractId)
    }
  })

  it('preserves the chosen contract when a decision references multiple contracts', () => {
    const shared = report.decisions.find(item => item.contractIds.length > 1)!
    for (const id of shared.contractIds) expect(resolveSelection(shared.id, id).contract.id).toBe(id)
    expect(resolveSelection(shared.id, 'unknown').contract.id).toBe(shared.contractIds[0])
  })

  it('resolves aliases and invalid links without creating extra contracts', () => {
    expect(resolveSelection('24SYMV0142117833', null).contract.id).toBe('24SYMV014217833')
    expect(resolveSelection('missing', null).contract.id).toBe(defaultContractId)
    expect(resolveSelection('missing', null).decision).toBeUndefined()
  })

  it('keeps amendments attached and does not invent decisions for empty searches', () => {
    expect(report.contracts.filter(item => !item.parentId)).toHaveLength(17)
    const children = report.contracts.filter(item => item.parentId)
    expect(children).toHaveLength(4)
    for (const child of children) expect(report.contracts.some(item => item.id === child.parentId && !item.parentId)).toBe(true)
    expect(contractDecisions('23SYMV013154974')).toEqual([])
  })

  it('provides a specific service, area, and document date for every contract panel', () => {
    for (const contract of report.contracts) {
      expect(contract.nodeContext.forestry).toBeTruthy()
      expect(contract.nodeContext.area).toBeTruthy()
      expect(contract.nodeContext.area).not.toBe('Δυτική Αττική · περιοχή έρευνας')
      expect(contract.documentDate).toMatch(/^\d{4}-\d{2}-\d{2}$/)
    }
    expect(report.contracts.find(item => item.id === '23SYMV013156865')?.documentDate).toBe('2023-07-24')
    expect(report.contracts.find(item => item.id === '26SYMV018936694')?.documentDate).toBe('2026-04-30')
  })

  it('publishes the corrected acceptance wording with primary source links', () => {
    const finding = report.findings.find(item => item.id === 'report-block-61')!
    expect(finding.text).toContain('με εξαίρεση τις φυτεύσεις')
    expect(finding.sources.some(source => source.id === 'ΡΩ5Π4653Π8-Ο4Χ')).toBe(true)
    expect(JSON.stringify(report)).not.toMatch(/original_text_sha256|body_block_index|vat_number|text_path/)
    expect(report.decisions.filter(item => !item.reviewed).every(item => item.summary === null)).toBe(true)
  })
})
