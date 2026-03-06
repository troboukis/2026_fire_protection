type ContractPdfCpvItem = { code: string; label: string }

export type ContractPdfData = {
  id: string
  who: string
  what: string
  when: string
  why: string
  beneficiary: string
  contractType: string
  withoutVatAmount: string
  withVatAmount: string
  referenceNumber: string
  contractNumber: string
  cpv: string
  cpvCode: string
  cpvItems?: ContractPdfCpvItem[]
  signedAt: string
  startDate: string
  endDate: string
  organizationVat: string
  beneficiaryVat: string
  shortDescription: string
}

function escapeHtml(v: string): string {
  return v
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
}

function buildCpvHtml(contract: ContractPdfData): string {
  const cpvItems = (contract.cpvItems ?? []).filter((x) => x.code || x.label)
  if (!cpvItems.length) return `${escapeHtml(contract.cpv)} (${escapeHtml(contract.cpvCode)})`
  return cpvItems
    .map((x) => `${escapeHtml(x.label)} (${escapeHtml(x.code)})`)
    .join('<br/>')
}

function buildPdfTemplate(contract: ContractPdfData): string {
  const title = escapeHtml(contract.what)
  const who = escapeHtml(contract.who)
  const when = escapeHtml(contract.when)
  const why = escapeHtml(contract.why)
  const amount = escapeHtml(contract.withoutVatAmount)
  const beneficiary = escapeHtml(contract.beneficiary)
  const contractType = escapeHtml(contract.contractType)
  const ref = escapeHtml(contract.referenceNumber)
  const contractNo = escapeHtml(contract.contractNumber)
  const cpvHtml = buildCpvHtml(contract)
  const orgVat = escapeHtml(contract.organizationVat)
  const benVat = escapeHtml(contract.beneficiaryVat)
  const signedAt = escapeHtml(contract.signedAt)
  const startDate = escapeHtml(contract.startDate)
  const endDate = escapeHtml(contract.endDate)
  const withVat = escapeHtml(contract.withVatAmount)
  const description = escapeHtml(contract.shortDescription)
  return `<!doctype html>
<html lang="el">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Project ΠΥΡ</title>
  <style>
    :root{--paper:#f7f5ee;--line:#cfc8bb;--line-strong:#9d9688;--ink:#111;--ink-soft:#4d4d4d;--ink-faint:#89857c;--accent:#d3482d;}
    *{box-sizing:border-box;}
    body{margin:0;padding:24px;background:var(--paper);color:var(--ink);font-family:"IBM Plex Sans","Helvetica Neue",Arial,sans-serif;}
    .sheet{max-width:980px;margin:0 auto;border:1px solid var(--line-strong);background:var(--paper);padding:20px;}
    .head{display:flex;justify-content:space-between;gap:16px;border-bottom:1px solid var(--line);padding-bottom:12px;}
    .eyebrow{font-family:"IBM Plex Mono","SFMono-Regular",Menlo,monospace;color:var(--ink-soft);font-size:11px;letter-spacing:.08em;text-transform:uppercase;}
    h1{margin:6px 0 0;font-size:34px;line-height:1.08;}
    .subtitle{margin:14px 0 0;font-family:"IBM Plex Mono","SFMono-Regular",Menlo,monospace;color:var(--ink-soft);font-size:15px;line-height:1.4;}
    .highlight{margin-top:14px;border:1px solid var(--line);padding:10px 12px;background:linear-gradient(90deg,rgba(211,72,45,.08),transparent 35%);}
    .amount{color:var(--accent);font-weight:700;font-size:32px;line-height:1.1;}
    .route{margin-top:6px;font-family:"IBM Plex Mono","SFMono-Regular",Menlo,monospace;color:var(--ink-soft);font-size:16px;letter-spacing:.04em;text-transform:uppercase;}
    .kind{margin-top:6px;font-family:"IBM Plex Mono","SFMono-Regular",Menlo,monospace;color:var(--ink-faint);font-size:12px;text-transform:uppercase;}
    .grid{margin-top:14px;display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:1px;background:var(--line);}
    .cell{background:var(--paper);padding:8px 10px;}
    .label{display:block;font-family:"IBM Plex Mono","SFMono-Regular",Menlo,monospace;color:var(--ink-faint);font-size:10px;letter-spacing:.07em;text-transform:uppercase;}
    .value{margin-top:4px;display:block;font-size:14px;line-height:1.35;word-break:break-word;}
    .source{margin-top:14px;padding-top:10px;border-top:1px solid var(--line);font-family:"IBM Plex Mono","SFMono-Regular",Menlo,monospace;font-size:11px;color:var(--ink-soft);}
    @page { size: A4; margin: 12mm; }
    @media print { body{padding:0;} .sheet{border:1px solid var(--line); box-shadow:none;} }
  </style>
</head>
<body>
  <article class="sheet">
    <header class="head">
      <div>
        <span class="eyebrow">${who}</span>
        <h1>${title}</h1>
      </div>
      <div class="eyebrow">${when}</div>
    </header>
    <p class="subtitle">${why}</p>
    <section class="highlight">
      <div class="amount">${amount}</div>
      <div class="route">→ ${beneficiary}</div>
      <div class="kind">${contractType}</div>
    </section>
    <section class="grid">
      <div class="cell"><span class="label">Κωδ. Αναφοράς</span><span class="value">${ref}</span></div>
      <div class="cell"><span class="label">Κωδ. Σύμβασης</span><span class="value">${contractNo}</span></div>
      <div class="cell"><span class="label">CPV</span><span class="value">${cpvHtml}</span></div>
      <div class="cell"><span class="label">Ποσό με ΦΠΑ</span><span class="value">${withVat}</span></div>
      <div class="cell"><span class="label">Υπογραφή</span><span class="value">${signedAt}</span></div>
      <div class="cell"><span class="label">Έναρξη / Λήξη</span><span class="value">${startDate} - ${endDate}</span></div>
      <div class="cell"><span class="label">Φορέας ΑΦΜ</span><span class="value">${orgVat}</span></div>
      <div class="cell"><span class="label">Δικαιούχος ΑΦΜ</span><span class="value">${benVat}</span></div>
      <div class="cell"><span class="label">Περιγραφή</span><span class="value">${description}</span></div>
    </section>
    <footer class="source">PROJECT ΠΥΡ, ΠΗΓΗ: https://portal.eprocurement.gov.gr/</footer>
  </article>
</body>
<script>
  window.addEventListener('load', function () {
    setTimeout(function () {
      window.print();
    }, 120);
  });
</script>
</html>`
}

export function openContractPdfPrintView(contract: ContractPdfData): void {
  const popup = window.open('', '_blank', 'width=1024,height=900')
  if (!popup) return
  try {
    popup.document.open()
    popup.document.write(buildPdfTemplate(contract))
    popup.document.close()
    popup.document.title = 'Project ΠΥΡ'
    try {
      popup.history.replaceState({}, '', `/project-pyr-print/${contract.id}`)
    } catch {
      // Ignore if browser blocks history manipulation in popup context.
    }
    popup.focus()
  } catch {
    popup.close()
  }
}
