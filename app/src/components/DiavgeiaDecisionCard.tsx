export type DiavgeiaDecisionCardView = {
  id: string
  orgType: string
  orgNameClean: string
  subject: string
  when: string
  ada: string
  decisionTypeUid: string
  amount?: string | null
  documentUrl?: string | null
  sortDate?: string | null
}

type Props = {
  item: DiavgeiaDecisionCardView
}

export default function DiavgeiaDecisionCard({ item }: Props) {
  const organization = [item.orgType, item.orgNameClean].filter((part) => part && part !== '—').join(' · ') || '—'
  const documentUrl = item.documentUrl ?? (item.ada && item.ada !== '—' ? `https://diavgeia.gov.gr/doc/${item.ada}` : null)

  return (
    <article className="wire-item diavgeia-decision-card">
      <div className="wire-item__head">
        <span className="eyebrow wire-item__org">Διαύγεια</span>
        <span className="wire-item__date">{item.when}</span>
      </div>
      <h2>{item.subject}</h2>
      <div className="wire-item__rule" aria-hidden="true" />
      <p className="wire-item__subtitle">{organization}</p>
      <div className="wire-item__footer">
        <p className="wire-item__type">{item.ada}</p>
        <p className="wire-item__type">{item.decisionTypeUid}</p>
        {item.amount && <p className="diavgeia-decision-card__amount">{item.amount}</p>}
      </div>
      {documentUrl && (
        <p className="wire-item__link">
          <a href={documentUrl} target="_blank" rel="noreferrer">
            Άνοιγμα απόφασης
          </a>
        </p>
      )}
    </article>
  )
}
