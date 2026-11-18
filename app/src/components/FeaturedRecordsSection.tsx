import { useOverviewUi } from '../i18n/overview'
import { useLayoutEffect, useRef, useState, type ReactNode } from 'react'
import BeneficiaryLink from './BeneficiaryLink'
import ComponentTag from './ComponentTag'
import type { ContractModalContract } from './ContractModal'
import DataLoadingCard from './DataLoadingCard'
import type { LatestContractCardView } from './LatestContractCard'

export type FeaturedRecordContract = LatestContractCardView & ContractModalContract & {
  id: string
  what: string
  withoutVatAmount: string
}

export type BeneficiaryInsightRow = {
  sourceDates?: { start: string | null; end: string | null; durationDays: number | null }
  beneficiary: string
  beneficiaryVat?: string | null
  beneficiaryGemi?: string | null
  organization: string
  totalAmount: number
  contractCount: number
  cpv: string
  startDate: string
  endDate: string
  duration: string
  progressPct: number | null
  signer: string
  relevantContracts: FeaturedRecordContract[]
}

type Props = {
  year: string
  rows: BeneficiaryInsightRow[]
  loading: boolean
  formatEur: (n: number | null) => string
  onOpenContract: (contract: FeaturedRecordContract) => void
  eyebrowText?: string
  title?: string
  note?: ReactNode
  footerNote?: ReactNode
  emptyMessage?: string
  sectionId?: string
}

export default function FeaturedRecordsSection({
  year,
  rows,
  loading,
  formatEur,
  onOpenContract,
  eyebrowText: suppliedEyebrowText,
  title: suppliedTitle,
  note: suppliedNote,
  footerNote,
  emptyMessage: suppliedEmptyMessage,
  sectionId = 'records',
}: Props) {
  const ui = useOverviewUi()
  const eyebrowText = suppliedEyebrowText ?? ui('Δικαιούχοι / {{v0}}', { v0: year })
  const title = suppliedTitle ?? ui('Εταιρείες που έχουν κερδίσει δημόσιες συμβάσεις έργων πυροπροστασίας')
  const note = suppliedNote ?? ui('Οι ανάδοχοι ταξινομούνται με βάση το συνολικό ποσό των συμβάσεων που έχουν λάβει από δήμους, περιφέρειες και άλλους δημόσιους φορείς.')
  const emptyMessage = suppliedEmptyMessage ?? ui('Δεν βρέθηκαν δικαιούχοι για το {{v0}}.', { v0: year })
  const recordsGridRef = useRef<HTMLDivElement | null>(null)
  const recordsIdentity = rows.map(row => `${row.beneficiaryVat ?? row.beneficiary}:${row.relevantContracts.map(contract => contract.id).join(',')}`).join('|')
  const [canScrollPrev, setCanScrollPrev] = useState(false)
  const [canScrollNext, setCanScrollNext] = useState(false)

  useLayoutEffect(() => {
    const container = recordsGridRef.current
    if (!container) return

    let frameId: number | null = null

    const updatePager = () => {
      const scrollMax = container.scrollWidth - container.clientWidth
      setCanScrollPrev(container.scrollLeft > 24)
      setCanScrollNext(container.scrollLeft < scrollMax - 1)
    }

    const scheduleUpdatePager = () => {
      if (frameId != null) window.cancelAnimationFrame(frameId)
      frameId = window.requestAnimationFrame(updatePager)
    }

    container.scrollLeft = 0
    scheduleUpdatePager()
    const resetFrame = window.requestAnimationFrame(() => {
      container.scrollLeft = 0
      updatePager()
    })
    const resetTimeout = window.setTimeout(() => {
      container.scrollLeft = 0
      updatePager()
    }, 120)
    const observer = new ResizeObserver(scheduleUpdatePager)
    observer.observe(container)
    container.addEventListener('scroll', updatePager, { passive: true })
    window.addEventListener('resize', scheduleUpdatePager)
    window.addEventListener('orientationchange', scheduleUpdatePager)

    return () => {
      observer.disconnect()
      window.cancelAnimationFrame(resetFrame)
      window.clearTimeout(resetTimeout)
      if (frameId != null) window.cancelAnimationFrame(frameId)
      container.removeEventListener('scroll', updatePager)
      window.removeEventListener('resize', scheduleUpdatePager)
      window.removeEventListener('orientationchange', scheduleUpdatePager)
    }
  }, [recordsIdentity, loading])

  const scrollRecords = (direction: -1 | 1) => {
    const container = recordsGridRef.current
    if (!container) return

    const firstCard = container.querySelector<HTMLElement>('.record-card, .records-grid__loading-card')
    const step = firstCard?.getBoundingClientRect().width ?? container.clientWidth
    container.scrollBy({ left: direction * (step + 1), behavior: 'smooth' })
  }

  return (
    <section id={sectionId} className="records section-rule dev-tag-anchor">
      <div className="dev-tag-stack dev-tag-stack--right">
        <ComponentTag name="FeaturedRecordsSection" />
        <ComponentTag name="records section-rule" kind="CLASS" />
      </div>
      <div className="section-head dev-tag-anchor">
        <ComponentTag name="section-head" kind="CLASS" className="component-tag--overlay" />
        <div className="eyebrow">{eyebrowText}</div>
        <h2>{title}</h2>
        <div className="section-head__subtitle-row">
          <section className='ca-header-note'>{note}</section>
          <div className="records-pager" aria-label={ui("Πλοήγηση επιλεγμένων δικαιούχων")}>
            <button
              type="button"
              className="records-pager__button"
              aria-label={ui("Προηγούμενος δικαιούχος")}
              onClick={() => scrollRecords(-1)}
              disabled={loading || !canScrollPrev}
            >
              ‹
            </button>
            <button
              type="button"
              className="records-pager__button"
              aria-label={ui("Επόμενος δικαιούχος")}
              onClick={() => scrollRecords(1)}
              disabled={loading || !canScrollNext}
            >
              ›
            </button>
          </div>
        </div>
      </div>

      <div className="records-grid records-grid--horizontal dev-tag-anchor" ref={recordsGridRef}>
        <ComponentTag
          name="records-grid records-grid--horizontal"
          kind="CLASS"
          className="component-tag--overlay"
          style={{ left: 'auto', right: '0.45rem' }}
        />
        {loading && (
          <DataLoadingCard
            className="records-grid__loading-card"
            message={ui("Ανακτώνται οι δικαιούχοι και οι συμβάσεις τους για το {{v0}}.", { v0: year })}
          />
        )}

        {!loading && rows.length === 0 && (
          <article className="record-card">
            <h3>{emptyMessage}</h3>
          </article>
        )}

        {!loading && rows.map((row, idx) => (
          (() => {
            const progressPct = row.progressPct == null ? null : Math.max(0, Math.min(100, row.progressPct))

            return (
              <article
                className="record-card"
                key={`${row.beneficiary}-${idx}`}
              >
                <div className="record-card__year" aria-hidden="true">{year}</div>
                <div className="record-card__header">
                  <div className="record-card__authority">#{idx + 1}</div>
                  <div className="record-card__id">{ui("Συμβάσεις: ")}{row.contractCount.toLocaleString(ui.locale)}</div>
                </div>
                <h3>
                  <BeneficiaryLink
                    name={row.beneficiary}
                    gemi={row.beneficiaryGemi}
                    className="beneficiary-link beneficiary-link--heading"
                  />
                </h3>
                <div className="record-card__amount">{formatEur(row.totalAmount)}</div>
                <div className="record-card__tags" aria-label={ui("Μεταδεδομένα δικαιούχου")}>
                  <span>CPV: {ui.description(row.cpv)}</span>
                  <span>{ui("Έναρξη: ")}{row.sourceDates ? ui.format.date(row.sourceDates.start) : row.startDate}</span>
                </div>
                <div className="record-duration">
                  <div className="record-duration__head">
                    <span>{ui("Διάρκεια: ")}{row.sourceDates ? row.sourceDates.durationDays == null ? '—' : ui('{{v0}} ημέρες', { v0: Math.round(row.sourceDates.durationDays) }) : row.duration}</span>
                    <span>{ui("Λήξη: ")}{row.sourceDates ? ui.format.date(row.sourceDates.end) : row.endDate}</span>
                  </div>
                  {progressPct != null && (
                    <div className="record-duration__track" aria-label={ui("Πρόοδος διάρκειας έργου")}>
                      <div
                        className="record-duration__fill"
                        style={{ width: `${progressPct}%` }}
                      />
                      <div
                        className="record-duration__today"
                        style={{ left: `${progressPct}%` }}
                        title={ui("Σήμερα")}
                      />
                    </div>
                  )}
                </div>
                {row.relevantContracts.length > 0 && (
                  <div className="record-contract-amounts" aria-label={ui("Σχετικές συμβάσεις")}>
                    <div className="record-contract-amounts__title">{ui("Σχετικές συμβάσεις")}</div>
                    <ul>
                      {row.relevantContracts.map((contract) => (
                        <li key={`${row.beneficiary}-contract-${contract.id}`}>
                          <button
                            type="button"
                            className="record-contract-link"
                            lang="el"
                            onClick={() => onOpenContract(contract)}
                          >
                            {contract.what} - {contract.sourceValues ? ui.format.currency(contract.sourceValues.withoutVat, { maximumFractionDigits: 0 }) : contract.withoutVatAmount}
                          </button>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
                <div className="record-card__footer">
                  <div>
                    <span className="label">{ui("Οργανισμός")}</span>
                    <strong>{ui.organization(row.organization)}</strong>
                  </div>
                  <div>
                    <span className="label">{ui("Υπογράφων")}</span>
                    <strong>{row.signer}</strong>
                  </div>
                </div>
              </article>
            )
          })()
        ))}
      </div>

      {footerNote && (
        <div className="records__footnote">{footerNote}</div>
      )}
    </section>
  )
}
