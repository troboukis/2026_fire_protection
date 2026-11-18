import { analysisMonthLabel } from '../../../i18n/analysisFormatting'
import { useOverviewUi } from '../../../i18n/overview'
import { logError } from '../../../lib/logger'
// Στατιστική ανάλυση συμβάσεων — D3 charts, χωρίς ΦΠΑ, από το 2024 έως την τρέχουσα χρονιά
import { useEffect, useMemo, useState } from 'react'
import { isAbortError } from '../../../lib/isAbortError'
import { supabase } from '../../../lib/supabase'
import ComponentTag from '../../../components/ComponentTag'
import DataLoadingCard from '../../../components/DataLoadingCard'
import DirectAwardHistogram from './DirectAwardHistogram'
import TopAuthoritiesSection from './TopAuthoritiesSection'
import { BarChart, HBar, ZoomableSunburst, type BarMetric } from './AnalysisCharts'
import {
  ANALYSIS_END,
  ANALYSIS_START,
  CURRENT_YEAR,
  buildAnalysisDataFromRows,
  buildProcedureAuthoritySunburstData,
  buildTopCpvList,
  toSectionRow,
  toneForPct,
  type AnalysisData,
  type BarItem,
  type ContractAnalysisRpcPayload,
  type SectionRow,
  type SunburstDatum,
  type TopCpvItem,
  type TopOrgItem,
} from './analysisData'

// ── Κύριο component ───────────────────────────────────────────────────
export default function StatisticalAnalysis() {
  const ui = useOverviewUi()
  const [barMetric,    setBarMetric]    = useState<BarMetric>('total')
  const [analysisPeriod, setAnalysisPeriod] = useState<'all' | string>(String(CURRENT_YEAR))
  const [analysis, setAnalysis] = useState<AnalysisData | null>(null)
  const [analysisError, setAnalysisError] = useState<string | null>(null)
  const availableAnalysisYears = useMemo(
    () => Array.from({ length: CURRENT_YEAR - 2024 + 1 }, (_, i) => String(2024 + i)).reverse(),
    [],
  )

  useEffect(() => {
    let cancelled = false
    const controller = new AbortController()
    ;(async () => {
      try {
        const { data, error } = await supabase
          .rpc('get_contract_analysis', { p_year_start: 2024 })
          .abortSignal(controller.signal)
        if (error) throw error

        const payload = (data ?? {}) as ContractAnalysisRpcPayload
        const sectionRows = Array.isArray(payload.sectionRows)
          ? payload.sectionRows.map(toSectionRow).filter((row): row is SectionRow => row !== null)
          : []

        if (cancelled) return
        setAnalysis(buildAnalysisDataFromRows(sectionRows))
        setAnalysisError(null)
      } catch (e) {
        if (isAbortError(e)) return
        if (cancelled) return
        logError('Failed to load contract statistics', e)
        setAnalysisError(e instanceof Error ? e.message : 'Αποτυχία φόρτωσης ανάλυσης')
      }
    })()

    return () => {
      cancelled = true
      controller.abort()
    }

  }, [])

  const monthly = useMemo(() => analysis?.monthly ?? [], [analysis])

  const sectionFiltered = useMemo(() => {
    if (!analysis) {
      return {
        contractTypeData: [] as BarItem[],
        procedureData: [] as BarItem[],
        topOrgs: [] as TopOrgItem[],
        totalSpendM: 0,
        topCpv: [] as TopCpvItem[],
        sunburstData: null as SunburstDatum | null,
        directAwardAmounts: [] as number[],
        directAwardTotal: 0,
      }
    }

    const periodStart = analysisPeriod === 'all' ? ANALYSIS_START : `${analysisPeriod}-01-01`
    const periodEnd = analysisPeriod === 'all' ? ANALYSIS_END : `${analysisPeriod}-12-31`

    const rows = analysis.sectionRows.filter((row) => {
      if (!row.signedDate) return false
      return row.signedDate >= periodStart && row.signedDate <= periodEnd
    })

    const totalAmount = rows.reduce((sum, row) => sum + row.amount, 0)
    const contractTypeMap = new Map<string, { count: number; total: number }>()
    const procedureMap = new Map<string, { count: number; total: number }>()
    const cpvAggMap = new Map<string, { count: number; procedures: Map<string, number> }>()
    const topOrgMap = new Map<string, { contracts: number; total: number }>()
    const directAwardAmounts: number[] = []

    for (const row of rows) {
      const ct = contractTypeMap.get(row.contractType) ?? { count: 0, total: 0 }
      ct.count += 1
      ct.total += row.amount
      contractTypeMap.set(row.contractType, ct)

      const pr = procedureMap.get(row.procedure) ?? { count: 0, total: 0 }
      pr.count += 1
      pr.total += row.amount
      procedureMap.set(row.procedure, pr)

      const org = topOrgMap.get(row.authorityLabel) ?? { contracts: 0, total: 0 }
      org.contracts += 1
      org.total += row.amount
      topOrgMap.set(row.authorityLabel, org)

      for (const cpv of row.cpvs) {
        const ca = cpvAggMap.get(cpv) ?? { count: 0, procedures: new Map<string, number>() }
        ca.count += 1
        ca.procedures.set(row.procedure, (ca.procedures.get(row.procedure) ?? 0) + 1)
        cpvAggMap.set(cpv, ca)
      }

      if (row.procedure === 'Απευθείας Ανάθεση') {
        directAwardAmounts.push(row.amount)
      }
    }

    const toBarData = (
      map: Map<string, { count: number; total: number }>,
      labels: string[],
    ): BarItem[] =>
      labels.map((label) => {
        const v = map.get(label) ?? { count: 0, total: 0 }
        const pct = totalAmount > 0 ? (v.total / totalAmount) * 100 : 0
        return {
          label,
          value: v.count,
          total_m: v.total / 1_000_000,
          pct: Number(pct.toFixed(1)),
          tone: toneForPct(pct),
        }
      }).sort((a, b) => b.pct - a.pct || b.total_m - a.total_m || a.label.localeCompare(b.label, 'el'))

    const contractTypeData = toBarData(contractTypeMap, ['Υπηρεσίες', 'Προμήθειες', 'Έργα', 'Λοιπές'])
    const procedureData = toBarData(procedureMap, ['Απευθείας Ανάθεση', 'Ανοιχτή Διαδικασία', 'Διαπραγμάτευση', 'Άλλη'])

    const topCpv = buildTopCpvList(cpvAggMap)
    const topOrgs = [...topOrgMap.entries()]
      .map(([name, item]) => ({
        name,
        contracts: item.contracts,
        total_m: item.total / 1_000_000,
      }))
      .sort((a, b) => b.total_m - a.total_m || b.contracts - a.contracts || a.name.localeCompare(b.name, 'el'))
      .slice(0, 8)

    return {
      contractTypeData,
      procedureData,
      topOrgs,
      totalSpendM: totalAmount / 1_000_000,
      topCpv,
      sunburstData: buildProcedureAuthoritySunburstData(rows),
      directAwardAmounts,
      directAwardTotal: directAwardAmounts.length,
    }
  }, [analysis, analysisPeriod])

  const analysisPeriodLabel = analysisPeriod === 'all' ? `2024–${CURRENT_YEAR}` : analysisPeriod

  const chartNote = useMemo(() => {
    if (monthly.length === 0) return ui("Δεν υπάρχουν διαθέσιμα μηνιαία δεδομένα.")

    if (barMetric === 'total') {
      const byTotalDesc = [...monthly].sort((a, b) => b.total_k - a.total_k)
      const top1 = byTotalDesc[0]
      const top2 = byTotalDesc[1] ?? byTotalDesc[0]
      const low = [...monthly]
        .filter((m) => m.total_k > 0)
        .sort((a, b) => a.total_k - b.total_k)[0] ?? [...monthly].sort((a, b) => a.total_k - b.total_k)[0]

      const top1M = (top1.total_k / 1000).toLocaleString(ui.locale, { maximumFractionDigits: 1 })
      const top2M = (top2.total_k / 1000).toLocaleString(ui.locale, { maximumFractionDigits: 1 })
      const lowM = (low.total_k / 1000).toLocaleString(ui.locale, { maximumFractionDigits: 1 })

      return ui("Τα μεγαλύτερα ποσά καταγράφονται στους μήνες {{v0}} (€ {{v1}}M) και {{v2}} (€ {{v3}}M), ενώ η χαμηλότερη δαπάνη εμφανίζεται τον {{v4}} (€ {{v5}}M).", { v0: analysisMonthLabel(top1.label, ui), v1: top1M, v2: analysisMonthLabel(top2.label, ui), v3: top2M, v4: analysisMonthLabel(low.label, ui), v5: lowM })
    }

    const byCountDesc = [...monthly].sort((a, b) => b.count - a.count)
    const top1 = byCountDesc[0]
    const top2 = byCountDesc[1] ?? byCountDesc[0]
    const low = [...monthly]
      .filter((m) => m.count > 0)
      .sort((a, b) => a.count - b.count)[0] ?? [...monthly].sort((a, b) => a.count - b.count)[0]

    return ui("Οι περισσότερες συμβάσεις υπογράφονται τους μήνες {{v0}} ({{v1}}) και {{v2}} ({{v3}}), ενώ ο μήνας με τη χαμηλότερη δραστηριότητα είναι ο {{v4}} ({{v5}}).", { v0: analysisMonthLabel(top1.label, ui), v1: top1.count.toLocaleString(ui.locale), v2: analysisMonthLabel(top2.label, ui), v3: top2.count.toLocaleString(ui.locale), v4: analysisMonthLabel(low.label, ui), v5: low.count.toLocaleString(ui.locale) })
  }, [barMetric, monthly, ui])

  const analysisLoading = !analysis && !analysisError

  if (analysisLoading) {
    return (
      <section id="analysis" className="ca-section section-rule" aria-label={ui("Στατιστική Ανάλυση")}>
        <ComponentTag name="StatisticalAnalysis" />
        <div className="ca-header section-head">
          <div className="eyebrow">{ui("Δημόσιες Συμβάσεις Πυροπροστασίας")}</div>
          <h2>{ui("Στατιστική Ανάλυση")}</h2>
          <p className="ca-header-note">
            {ui("Ανακτώνται οι δημοσιευμένες συμβάσεις και οι συγκεντρωτικές μετρήσεις για το διάστημα 2024 έως σήμερα.")}</p>
        </div>
        <DataLoadingCard message={ui("Υπολογίζονται οι δείκτες, τα γραφήματα και οι κατανομές της ανάλυσης.")} />
      </section>
    )
  }

  return (
    <section id="analysis" className="ca-section section-rule" aria-label={ui("Στατιστική Ανάλυση")}>
      <ComponentTag name="StatisticalAnalysis" />

      {/* ── Header ── */}
      <div className="ca-header section-head">
        <div className="eyebrow">{ui("Δημόσιες Συμβάσεις Πυροπροστασίας")}</div>
        <h2>{ui("Στατιστική Ανάλυση")}</h2>
        <p className="ca-header-note">
          {ui("Ανάλυση ")}{ui.language === 'en' ? 'of ' : ''}<strong>{analysis ? analysis.totalContracts.toLocaleString(ui.locale) : '…'} {ui("δημοσιευμένων συμβάσεων")}</strong> {ui("που καλύπτουν τα έτη 2024 έως και σήμερα. Η συνολική δαπάνη για υπηρεσίες καθαρισμού, προμήθειες εξοπλισμού και έργα υποδομής υπολογίζεται στα ")}<strong>€ {analysis ? (analysis.totalAmount / 1_000_000).toFixed(1) : '…'}M </strong>{ui("(χωρίς ΦΠΑ).")}</p>
        {analysisError && <p className="ca-empty-note">{ui("Η ενότητα ανάλυσης δεν είναι διαθέσιμη προσωρινά.")}</p>}
      </div>

      {/* ── Bar Chart ── */}
      <div className="ca-chart-block">
        <div className="ca-chart-head">
          <div className="eyebrow">{ui("Εξέλιξη ανά μήνα")}</div>
          <div className="ca-metric-toggle">
            <button
              className={`ca-toggle-btn${barMetric === 'total' ? ' ca-toggle-btn--active' : ''}`}
              onClick={() => setBarMetric('total')}
            >{ui("Δαπάνη (χ.ΦΠΑ)")}</button>
            <button
              className={`ca-toggle-btn${barMetric === 'count' ? ' ca-toggle-btn--active' : ''}`}
              onClick={() => setBarMetric('count')}
            >{ui("Αριθμός Συμβάσεων")}</button>
          </div>
          <div className="ca-chart-legend">
            <span className="ca-legend-dot ca-legend-dot--fire" aria-hidden="true" />
            <span>{ui("Αντιπυρική περίοδος (Μαΐ–Αυγ)")}</span>
          </div>
        </div>
        <BarChart metric={barMetric} monthly={monthly} />
        <p className="ca-chart-note">{chartNote}</p>
      </div>

      {/* ── Type & Procedure breakdowns ── */}
      <ComponentTag name="ContractTypeProcedureSection" />
      <div className="ca-chart-head" style={{ padding: '0.75rem 1rem 0.35rem 1rem' }}>
       
        <div className="ca-metric-toggle">
          {availableAnalysisYears.map((year) => (
            <button
              key={year}
              className={`ca-toggle-btn${analysisPeriod === year ? ' ca-toggle-btn--active' : ''}`}
              onClick={() => setAnalysisPeriod(year)}
            >{year}</button>
          ))}
          <button
            className={`ca-toggle-btn${analysisPeriod === 'all' ? ' ca-toggle-btn--active' : ''}`}
            onClick={() => setAnalysisPeriod('all')}
          >{`2024–${CURRENT_YEAR}`}</button>
        </div>
      </div>
      <div className="ca-double-grid">
        <div className="ca-breakdown-block">
          <div className="eyebrow">{ui("Τύποι συμβάσεων")}</div>
          <div className="ca-bars">
            {sectionFiltered.contractTypeData.map(item => <HBar key={item.label} item={item} />)}
          </div>
        </div>
        <div className="ca-breakdown-block">
          <div className="eyebrow">{ui("Διαδικασία ανάθεσης")}</div>
          <div className="ca-bars">
            {sectionFiltered.procedureData.map(item => <HBar key={item.label} item={item} />)}
          </div>
        </div>
      </div>

      <DirectAwardHistogram
        amounts={sectionFiltered.directAwardAmounts}
        totalCount={sectionFiltered.directAwardTotal}
        periodLabel={analysisPeriodLabel}
        embedded
      />

      <div className="ca-findings" style={{ borderTop: '1px solid var(--line)' }}>
        <div className="eyebrow">GOOD TO KNOW</div>
        <div className="ca-findings__grid">
          {[
            {
              num: '01',
              title: ui("Υπηρεσίες"),
              text: ui("Συμβάσεις παροχής υπηρεσιών από τρίτους προς την αναθέτουσα αρχή. Δεν παράγουν υλικό αποτέλεσμα (έργο) ούτε μεταβιβάζουν κυριότητα αγαθού."),
            },
            {
              num: '02',
              title: ui("Προμήθειες"),
              text: ui("Συμβάσεις αγοράς, χρηματοδοτικής μίσθωσης ή μίσθωσης αγαθών/εξοπλισμού που μεταβιβάζονται ή παραχωρούνται στην αναθέτουσα αρχή."),
            },
            {
              num: '03',
              title: ui("Έργα"),
              text: ui("Συμβάσεις εκτέλεσης οικοδομικών ή τεχνικών εργασιών (κατασκευή, ανακαίνιση, αποκατάσταση), με αποτέλεσμα ακίνητο τεχνικό έργο."),
            },
            {
              num: '04',
              title: ui("Απευθείας Ανάθεση"),
              text: ui("Η πιο απλοποιημένη διαδικασία, χωρίς πλήρη διαγωνιστική προκήρυξη, όπου η αναθέτουσα αρχή επιλέγει απευθείας ανάδοχο."),
            },
            {
              num: '05',
              title: ui("Ανοιχτή Διαδικασία"),
              text: ui("Κάθε ενδιαφερόμενος οικονομικός φορέας μπορεί να υποβάλει προσφορά μετά από δημοσίευση στο ΚΗΜΔΗΣ και όπου απαιτείται στην ΕΕ."),
            },
            {
              num: '06',
              title: ui("Διαπραγμάτευση"),
              text: ui("Η αναθέτουσα αρχή διαπραγματεύεται με έναν ή περισσότερους φορείς, μόνο σε ειδικές περιπτώσεις που προβλέπει ο νόμος."),
            },
          ].map(f => (
            <article className="ca-finding-card" key={f.num}>
              <div className="ca-finding-num">{f.num}</div>
              <div className="ca-finding-body">
                <strong>{f.title}</strong>
                <p>{f.text}</p>
              </div>
            </article>
          ))}
        </div>
      </div>

      <TopAuthoritiesSection rows={sectionFiltered.topOrgs} totalSpendM={sectionFiltered.totalSpendM} loading={false} />

      {/* ── Top CPV ── */}
      <div className="ca-table-block">
        <ComponentTag name="TopCpvSection" />
        <div className="ca-sunburst-block">
          <div className="ca-sunburst-copy">
            <div className="eyebrow">{ui("ΑΝΑΘΕΣΕΙΣ")}</div>
            <strong>{ui("Διαδραστικό sunburst που απεικονίζει τη σχέση διαδικασίας ανάθεσης και φορέα")}</strong>
            <p>
              {ui("Κέντρο: διαδικασία ανάθεσης — Εξωτερικός δακτύλιος: συγκεκριμένοι φορείς, όπως δήμοι, περιφέρειες και οργανισμοί. Το μέγεθος των πεδίων αποτυπώνει το πλήθος και όχι το συνολικό ποσό των συμβάσεων. Κλικ σε τμήμα για εστίαση, κλικ στο κέντρο για επαναφορά.")}</p>
          </div>
          <ZoomableSunburst data={sectionFiltered.sunburstData} />
        </div>
        <div className="ca-cpv-grid">
          {sectionFiltered.topCpv.map((c, i) => (
            <article className="ca-cpv-card" key={c.cpv}>
              <div className="ca-cpv-rank">#{i + 1}</div>
              <div className="ca-cpv-desc">{ui.description(c.desc)}</div>
              <div className="ca-cpv-stats">
                <div>
                  <span className="label">{ui("Συμβάσεις")}</span>
                  <strong>{c.count}</strong>
                </div>
                <div>
                  <span className="label">{ui("Κύρια διαδικασία")}</span>
                  <strong><span className="ca-accent">{ui.language === 'el' ? c.mainProcedurePct.toFixed(1).replace('.', ',') : c.mainProcedurePct.toFixed(1)}%</span> {ui(c.mainProcedure)}</strong>
                </div>
              </div>
            </article>
          ))}
        </div>
        <p className="ca-sub-note">
          {ui("Για κάθε CPV εμφανίζεται το πλήθος συμβάσεων και η επικρατέστερη διαδικασία ανάθεσης.")}</p>
      </div>

      <div className="ca-footer-note">
        <span className="eyebrow">{ui("Πηγή δεδομένων")}</span>
        <span>
          {ui("Kεντρικό Ηλεκτρονικό Μητρώο ΔΗμοσίων Συμβάσεων · ")}{analysis ? analysis.totalContracts.toLocaleString(ui.locale) : '…'} {ui("εγγραφές · Δυναμική ημερήσια ανανέωση δεδομένων · Τιμές χωρίς ΦΠΑ")}</span>
      </div>
    </section>
  )
}
