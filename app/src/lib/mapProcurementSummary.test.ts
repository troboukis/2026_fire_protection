import { describe, expect, it } from 'vitest'
import { buildMunicipalityMapSummary, type ProcurementMapRow } from './mapProcurementSummary'

describe('buildMunicipalityMapSummary', () => {
  it('excludes superseded contracts that would otherwise keep a municipality active', () => {
    const rows: ProcurementMapRow[] = [
      {
        municipality_key: '9148',
        canonical_owner_scope: 'municipality',
        contract_signed_date: '2025-08-13',
        start_date: '2025-08-13',
        end_date: '2026-02-13',
        cancelled: false,
        next_ref_no: '25SYMV017400753',
        prev_reference_no: null,
        reference_number: '25SYMV017397160',
      },
      {
        municipality_key: '9148',
        canonical_owner_scope: 'municipality',
        contract_signed_date: '2025-08-13',
        start_date: '2025-08-13',
        end_date: '2025-09-30',
        cancelled: false,
        next_ref_no: null,
        prev_reference_no: '25SYMV017397160',
        reference_number: '25SYMV017400753',
      },
    ]

    const summary = buildMunicipalityMapSummary(rows, 2026, new Map())

    expect(summary.procurementRowsForYear).toBe(0)
    expect(summary.nationalTotalCount).toBe(0)
    expect(summary.countByMunicipality.has('9148')).toBe(false)
  })

  it('keeps active municipal rows that are not cancelled or superseded', () => {
    const rows: ProcurementMapRow[] = [
      {
        municipality_key: '9148.0',
        canonical_owner_scope: 'organization',
        organization_key: '6077',
        contract_signed_date: '2025-12-20',
        start_date: '2025-12-20',
        end_date: '2026-03-31',
        cancelled: false,
        next_ref_no: null,
        prev_reference_no: null,
        reference_number: '26SYMV0001',
      },
    ]

    const summary = buildMunicipalityMapSummary(rows, 2026, new Map([['6077', 'municipality']]))

    expect(summary.procurementRowsForYear).toBe(1)
    expect(summary.nationalTotalCount).toBe(1)
    expect(summary.countByMunicipality.get('9148')).toBe(1)
  })
})
