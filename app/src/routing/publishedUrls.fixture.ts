// Compatibility baseline captured before adding English routes.
// Keep published Greek paths and query names stable as language support grows.
export const publishedPages = [
  { path: '/', route: '/', html: 'index.html' },
  { path: '/maps', route: '/maps', html: 'index.html' },
  { path: '/municipalities', route: '/municipalities', html: 'index.html' },
  { path: '/environment-ministry', route: '/environment-ministry', html: 'index.html' },
  { path: '/contracts', route: '/contracts', html: 'index.html' },
  { path: '/diavgeia', route: '/diavgeia', html: 'index.html' },
  { path: '/analysis', route: '/analysis', html: 'analysis/index.html' },
  { path: '/analysis/statistics', route: '/analysis/statistics', html: 'analysis/statistics/index.html' },
  { path: '/analysis/antinero-west-attica', route: '/analysis/antinero-west-attica', html: 'analysis/antinero-west-attica/index.html' },
  { path: '/analysis/yearly_report', route: '/analysis/yearly_report', html: 'analysis/yearly_report/index.html' },
  { path: '/analysis/methodology_guide', route: '/analysis/methodology_guide', html: 'analysis/methodology_guide/index.html' },
  { path: '/analysis/west-attica-fire-2026/', route: null, html: 'public/analysis/west-attica-fire-2026/index.html' },
  { path: '/media', route: '/media', html: 'index.html' },
  { path: '/privacy', route: '/privacy', html: 'index.html' },
  { path: '/terms', route: '/terms', html: 'index.html' },
] as const

export const publishedDeepLinks = [
  { url: '/municipalities?municipality=9230', route: '/municipalities' },
  {
    url: '/contracts?dateFrom=2024-01-01&dateTo=2026-11-17&organizationKey=100&organizationKey=200&regionKey=4&municipalityKey=9230',
    route: '/contracts',
  },
  {
    url: '/contracts?q=%CE%B1%CF%80%CE%BF%CF%88%CE%AF%CE%BB%CF%89%CF%83%CE%B7+NOT+%CE%BA%CF%84%CE%AF%CF%81%CE%B9%CE%BF&procedure=%CE%91%CE%A0%CE%95%CE%A5%CE%98%CE%95%CE%99%CE%91%CE%A3&minAmount=1000',
    route: '/contracts',
  },
  { url: '/diavgeia?municipalityKey=9230&allDates=1&q=%CE%B1%CE%BD%CF%84%CE%B9%CF%80%CF%85%CF%81', route: '/diavgeia' },
  { url: '/diavgeia?dateFrom=2026-01-01&dateTo=2026-11-17', route: '/diavgeia' },
  {
    url: '/analysis/antinero-west-attica?node=24SYMV014192335&contract=24SYMV014192335',
    route: '/analysis/antinero-west-attica',
  },
  {
    url: '/analysis/antinero-west-attica?connection=amendment%3A23SYMV013156865',
    route: '/analysis/antinero-west-attica',
  },
  { url: '/analysis/yearly_report#symvaseis', route: '/analysis/yearly_report' },
  { url: '/analysis/yearly_report#xronos', route: '/analysis/yearly_report' },
  { url: '/analysis/yearly_report#plirwmes', route: '/analysis/yearly_report' },
  { url: '/analysis/yearly_report#methodologia', route: '/analysis/yearly_report' },
  { url: '/#about', route: '/' },
  { url: '/#latest', route: '/' },
  { url: '/#historicalmap', route: '/' },
] as const

export const legacyReportLinks = [
  { from: '/reports', to: '/analysis' },
  { from: '/reports/', to: '/analysis/' },
  { from: '/reports/statistics/', to: '/analysis/statistics/' },
  {
    from: '/reports/antinero-west-attica?node=24SYMV014192335&contract=24SYMV014192335',
    to: '/analysis/antinero-west-attica?node=24SYMV014192335&contract=24SYMV014192335',
  },
  {
    from: '/reports/antinero-west-attica?node=%CE%A1%CE%A95%CE%A04653%CE%A08-%CE%9F4%CE%A7&contract=24SYMV014192335&tag=one&tag=two#%CF%80%CE%B7%CE%B3%CE%AD%CF%82',
    to: '/analysis/antinero-west-attica?node=%CE%A1%CE%A95%CE%A04653%CE%A08-%CE%9F4%CE%A7&contract=24SYMV014192335&tag=one&tag=two#%CF%80%CE%B7%CE%B3%CE%AD%CF%82',
  },
  { from: '/reports/yearly_report?utm_source=shared#methodologia', to: '/analysis/yearly_report?utm_source=shared#methodologia' },
] as const
