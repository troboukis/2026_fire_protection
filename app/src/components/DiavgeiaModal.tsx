import { useProcurementI18n } from '../i18n/useProcurementI18n'
import { useEffect, useRef, useState, type ReactNode } from 'react'
import { createPortal } from 'react-dom'
import { LocalizedLink as Link } from '../routing/LocalizedLink'
import type { DiavgeiaDecisionCardView } from './DiavgeiaDecisionCard'
import ContractModal, { type ContractModalContract } from './ContractModal'
import { downloadContractDocument } from '../lib/contractDocument'
import { loadContractModal } from '../lib/contractModal'
import { supabase } from '../lib/supabase'

type Props = {
  decision: DiavgeiaDecisionCardView
  onClose: () => void
}

function isMunicipalityOrgType(value: string): boolean {
  return value.trim().toLocaleUpperCase('el-GR') === 'ΔΗΜΟΣ'
}

export default function DiavgeiaModal({ decision, onClose }: Props) {
  const { p, t, format, category, thematic, organization: organizationText } = useProcurementI18n()
  const [linkedContracts, setLinkedContracts] = useState<Array<{ id: number; reference_number: string }>>([])
  const [linksLoading, setLinksLoading] = useState(true)
  const [linksFailed, setLinksFailed] = useState(false)
  const [openingContractId, setOpeningContractId] = useState<number | null>(null)
  const [contractFailed, setContractFailed] = useState(false)
  const [selectedContract, setSelectedContract] = useState<ContractModalContract | null>(null)
  const requestVersion = useRef(0)

  useEffect(() => {
    let cancelled = false
    setLinkedContracts([])
    setLinksLoading(true)
    setLinksFailed(false)
    setSelectedContract(null)
    setOpeningContractId(null)
    setContractFailed(false)
    const loadLinks = async () => {
      try {
        const decisionId = decision.id.replace(/^diavgeia-/, '')
        const { data: links, error: linksError } = await supabase
          .from('diavgeia_procurement')
          .select('procurement_id')
          .eq('diavgeia_id', decisionId)
        if (linksError) throw linksError
        const ids = [...new Set((links ?? []).map((link) => link.procurement_id))]
        if (ids.length) {
          const { data, error } = await supabase
            .from('procurement')
            .select('id, reference_number')
            .in('id', ids)
            .order('reference_number')
          if (error) throw error
          if (!cancelled) setLinkedContracts(data ?? [])
        }
      } catch (error) {
        console.error('Failed to load linked contracts', error)
        if (!cancelled) setLinksFailed(true)
      } finally {
        if (!cancelled) setLinksLoading(false)
      }
    }
    void loadLinks()
    return () => {
      cancelled = true
      requestVersion.current += 1
    }
  }, [decision.id])

  const openContract = async (id: number) => {
    const version = ++requestVersion.current
    setOpeningContractId(id)
    setContractFailed(false)
    try {
      const contract = await loadContractModal({ id })
      if (requestVersion.current === version) setSelectedContract(contract)
    } catch (error) {
      console.error('Failed to load linked contract details', error)
      if (requestVersion.current === version) setContractFailed(true)
    } finally {
      if (requestVersion.current === version) setOpeningContractId(null)
    }
  }

  const organization = [category(decision.orgType), organizationText(decision.orgNameClean)]
    .filter((part) => part && part !== '—')
    .join(' · ') || '—'
  const documentUrl = decision.documentUrl ?? (decision.ada && decision.ada !== '—'
    ? `https://diavgeia.gov.gr/doc/${decision.ada}`
    : null)
  const isMunicipality = isMunicipalityOrgType(decision.orgType) && !!decision.municipalityKey
  const organizationValue = isMunicipality ? (
    <Link
      className="contract-modal__text-link"
      to={`/municipalities?municipality=${encodeURIComponent(decision.municipalityKey!)}`}
    >
      {organization}
    </Link>
  ) : organization
  const gridItems: Array<{ label: string; value: ReactNode }> = [
    { label: p('decisionDate'), value: decision.sortDate ? format.date(decision.sortDate) : decision.when },
    { label: p('organization'), value: organizationValue },
    { label: p('amount'), value: decision.amountEur !== undefined ? format.currency(decision.amountEur, { maximumFractionDigits: 0 }) : decision.amount ?? '—' },
    { label: p('protocolNumber'), value: decision.protocolNumber ?? '—' },
    { label: p('signers'), value: decision.spendingSigners ?? '—' },
    { label: p('contractorBeneficiary'), value: decision.spendingContractorsName ?? '—' },
    { label: p('taxId'), value: decision.spendingContractorsAfm ?? '—' },
    { label: p('thematicCategories'), value: thematic(decision.thematicCategories ?? '—') },
  ]

  if (linksLoading || linksFailed || linkedContracts.length) {
    gridItems.push({
      label: linkedContracts.length === 1 ? p('linkedContract') : p('linkedContracts'),
      value: linksLoading ? t('common:loading') : linksFailed ? p('linkedContractsFailed') : (
        <>
          {linkedContracts.map((contract) => (
            <div key={contract.id}>
              <button
                type="button"
                className="contract-modal__text-link"
                disabled={openingContractId !== null}
                aria-busy={openingContractId === contract.id}
                onClick={() => { void openContract(contract.id) }}
              >
                {contract.reference_number}
              </button>
            </div>
          ))}
          {contractFailed && <span role="status">{p('contractRetry')}</span>}
        </>
      ),
    })
  }

  if (selectedContract) {
    return createPortal(
      <ContractModal
        contract={selectedContract}
        onClose={() => setSelectedContract(null)}
        onDownloadPdf={() => { void downloadContractDocument(selectedContract) }}
      />,
      document.body,
    )
  }

  const modal = (
    <div className="contract-modal-backdrop" onClick={onClose}>
      <article className="contract-modal contract-modal--diavgeia" onClick={(e) => e.stopPropagation()}>
        <header className="contract-modal__header">
          <div>
            <span className="eyebrow">{t('navigation:diavgeia')}</span>
            <h2 lang="el">{decision.subject}</h2>
          </div>
          <div className="contract-modal__actions">
            {documentUrl && (
              <a
                className="contract-modal__icon-button"
                href={documentUrl}
                target="_blank"
                rel="noreferrer"
                aria-label={p('downloadDecision')}
                title={p('downloadDecision')}
              >
                <svg viewBox="0 0 20 20" aria-hidden="true">
                  <path d="M10 2.5v8.2" />
                  <path d="M6.8 8.9 10 12.1l3.2-3.2" />
                  <path d="M4 14.5h12" />
                </svg>
              </a>
            )}
            <button
              type="button"
              className="contract-modal__icon-button"
              onClick={onClose}
              aria-label={p('close')}
              title={p('close')}
            >
              ✕
            </button>
          </div>
        </header>

        <div className="contract-modal__highlight">
          <span className="contract-modal__amount">{decision.ada}</span>
          <span className="contract-modal__arrow">→</span>
          <span className="contract-modal__beneficiary">{category(decision.decisionTypeUid)}</span>
        </div>

        <div className="contract-modal__grid">
          {gridItems.map((item) => (
            <div key={item.label}>
              <span>{item.label}</span>
              <strong>{item.value}</strong>
            </div>
          ))}
        </div>
      </article>
    </div>
  )

  return createPortal(modal, document.body)
}
