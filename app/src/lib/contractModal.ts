import type { ContractModalContract } from '../components/ContractModal'
import { buildContractAuthorityLabel, type ContractAuthorityScope } from './contractAuthority'
import { buildDiavgeiaDocumentUrl } from './contractDocument'
import { summarizePaymentRows } from './paymentSummary'
import { supabase } from './supabase'

export type ContractModalFallback = {
  id: number
  beneficiary_name?: string | null
  cpv_value?: string | null
  organization_value?: string | null
  title?: string | null
  procedure_type_value?: string | null
  reference_number?: string | null
  beneficiary_gemi?: string | null
}

function clean(v: unknown): string {
  if (v == null) return ''
  const s = String(v).trim()
  if (!s || s.toLowerCase() === 'nan' || s.toLowerCase() === 'none') return ''
  return s
}

function fmtEur(n: number | null): string {
  if (n == null || Number.isNaN(n)) return '—'
  return n.toLocaleString('el-GR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 })
}

function fmtDateLabel(iso: string | null): string {
  if (!iso) return '—'
  const dt = new Date(iso)
  if (Number.isNaN(dt.getTime())) return '—'
  return new Intl.DateTimeFormat('el-GR', { day: '2-digit', month: 'short', year: 'numeric' }).format(dt)
}

function cleanText(v: unknown): string | null {
  const s = clean(v)
  return s || null
}

function firstPipePart(v: unknown): string | null {
  const s = cleanText(v)
  if (!s) return null
  return s.split('|').map((x) => x.trim()).filter(Boolean)[0] ?? null
}

export async function loadContractModal(row: ContractModalFallback): Promise<ContractModalContract> {
  const { data: procurement, error: procurementError } = await supabase
    .from('procurement')
    .select(`
      id,
      title,
      submission_at,
      short_descriptions,
      procedure_type_value,
      reference_number,
      contract_number,
      contract_signed_date,
      start_date,
      end_date,
      municipality_key,
      region_key,
      canonical_owner_scope,
      organization_vat_number,
      organization_key,
      assign_criteria,
      contract_type,
      award_procedure,
      units_operator,
      funding_details_cofund,
      funding_details_self_fund,
      funding_details_espa,
      funding_details_regular_budget,
      auction_ref_no,
      contract_related_ada,
      prev_reference_no,
      next_ref_no,
      budget,
      contract_budget,
      diavgeia_ada
    `)
    .eq('id', row.id)
    .single()

  if (procurementError) throw procurementError
  if (!procurement) throw new Error('Contract not found')

  const [
    paymentsResult,
    cpvResult,
    organizationResult,
    municipalityResult,
    regionResult,
  ] = await Promise.all([
    supabase
      .from('payment')
      .select('beneficiary_name, beneficiary_vat_number, signers, payment_ref_no, amount_without_vat, amount_with_vat')
      .eq('procurement_id', row.id),
    supabase
      .from('cpv')
      .select('cpv_key, cpv_value')
      .eq('procurement_id', row.id),
    supabase
      .from('organization')
      .select('organization_normalized_value, organization_value, authority_scope')
      .eq('organization_key', procurement.organization_key)
      .limit(1),
    procurement.municipality_key
      ? supabase
        .from('municipality')
        .select('municipality_normalized_value, municipality_value')
        .eq('municipality_key', procurement.municipality_key)
        .limit(1)
      : Promise.resolve({ data: [] }),
    procurement.region_key
      ? supabase
        .from('region')
        .select('region_normalized_value, region_value')
        .eq('region_key', procurement.region_key)
        .limit(1)
      : Promise.resolve({ data: [] }),
  ])

  for (const result of [paymentsResult, cpvResult, organizationResult, municipalityResult, regionResult]) {
    if ('error' in result && result.error) throw result.error
  }

  const paymentRows = (paymentsResult.data ?? []) as Array<{
    beneficiary_name: string | null
    beneficiary_vat_number: string | null
    signers: string | null
    payment_ref_no: string | null
    amount_without_vat: number | null
    amount_with_vat: number | null
  }>
  const paymentSummary = summarizePaymentRows(paymentRows)
  const amountWithoutVat = paymentSummary.amount_without_vat ?? procurement.contract_budget ?? procurement.budget ?? null
  const amountWithVat = paymentSummary.amount_with_vat

  const cpvItems = ((cpvResult.data ?? []) as Array<{ cpv_key: string | null; cpv_value: string | null }>)
    .map((item) => ({
      code: clean(item.cpv_key) || '—',
      label: clean(item.cpv_value) || '—',
    }))
    .filter((item) => item.code !== '—' || item.label !== '—')
    .reduce<Array<{ code: string; label: string }>>((acc, current) => {
      if (!acc.find((item) => item.code === current.code && item.label === current.label)) acc.push(current)
      return acc
    }, [])

  const primaryCpv = cpvItems[0] ?? null
  const organization = ((organizationResult.data ?? []) as Array<{
    organization_normalized_value: string | null
    organization_value: string | null
    authority_scope: ContractAuthorityScope | null
  }>)[0] ?? null
  const municipality = ((municipalityResult.data ?? []) as Array<{
    municipality_normalized_value: string | null
    municipality_value: string | null
  }>)[0] ?? null
  const region = ((regionResult.data ?? []) as Array<{
    region_normalized_value: string | null
    region_value: string | null
  }>)[0] ?? null
  const beneficiary = paymentSummary.beneficiary_name ?? row.beneficiary_name
  const whyText = firstPipePart(procurement.short_descriptions) ?? primaryCpv?.label ?? clean(row.cpv_value) ?? '—'
  const cpvLabel = primaryCpv?.label ?? clean(row.cpv_value) ?? '—'
  const contractRelatedAda = clean(procurement.contract_related_ada)
  const diavgeiaAda = clean(procurement.diavgeia_ada)
  const organizationName = clean(organization?.organization_normalized_value) || clean(organization?.organization_value) || clean(row.organization_value) || '—'
  const who = buildContractAuthorityLabel({
    canonicalOwnerScope: clean(procurement.canonical_owner_scope),
    organizationScope: clean(organization?.authority_scope),
    organizationName,
    municipalityLabel: clean(municipality?.municipality_normalized_value) || clean(municipality?.municipality_value),
    regionLabel: clean(region?.region_normalized_value) || clean(region?.region_value),
  })
  const modal: ContractModalContract = {
    id: String(procurement.id),
    who,
    what: clean(procurement.title) || clean(row.title) || '—',
    when: fmtDateLabel(cleanText(procurement.submission_at)),
    why: whyText,
    beneficiary: clean(beneficiary) || '—',
    contractType: clean(procurement.procedure_type_value) || clean(row.procedure_type_value) || '—',
    howMuch: fmtEur(amountWithoutVat),
    withoutVatAmount: fmtEur(amountWithoutVat),
    withVatAmount: fmtEur(amountWithVat),
    referenceNumber: clean(procurement.reference_number) || clean(row.reference_number) || '—',
    contractNumber: clean(procurement.contract_number) || '—',
    cpv: cpvLabel,
    cpvCode: primaryCpv?.code ?? '—',
    cpvItems,
    signedAt: fmtDateLabel(cleanText(procurement.contract_signed_date)),
    startDate: fmtDateLabel(cleanText(procurement.start_date)),
    endDate: fmtDateLabel(cleanText(procurement.end_date)),
    organizationVat: clean(procurement.organization_vat_number) || '—',
    beneficiaryVat: clean(paymentSummary.beneficiary_vat_number) || '—',
    beneficiaryGemi: clean(row.beneficiary_gemi) || null,
    signers: clean(paymentSummary.signers) || '—',
    assignCriteria: clean(procurement.assign_criteria) || '—',
    contractKind: clean(procurement.contract_type) || '—',
    awardProcedure: clean(procurement.award_procedure) || '—',
    unitsOperator: clean(procurement.units_operator) || '—',
    fundingCofund: clean(procurement.funding_details_cofund) || '—',
    fundingSelf: clean(procurement.funding_details_self_fund) || '—',
    fundingEspa: clean(procurement.funding_details_espa) || '—',
    fundingRegular: clean(procurement.funding_details_regular_budget) || '—',
    auctionRefNo: clean(procurement.auction_ref_no) || '—',
    paymentRefNo: clean(paymentSummary.payment_ref_no) || '—',
    shortDescription: firstPipePart(procurement.short_descriptions) ?? '—',
    rawBudget: fmtEur(procurement.budget),
    contractBudget: fmtEur(procurement.contract_budget),
    contractRelatedAda: contractRelatedAda || '—',
    previousReferenceNumber: clean(procurement.prev_reference_no) || '—',
    nextReferenceNumber: clean(procurement.next_ref_no) || '—',
    documentUrl: buildDiavgeiaDocumentUrl(contractRelatedAda, diavgeiaAda),
  }
  return modal
}
