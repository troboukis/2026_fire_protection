import { fallbackNavigation, getPageMetadata, metadataElements, metadataPages, metadataText, SITE_ORIGIN, type PageMetadata } from './pageMetadata'

export const escapeHtml = (text: string) => text.replace(/[&<>"']/g, value => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[value]!)
const attribute = (tag: string, name: string) => tag.match(new RegExp(`\\b${name}\\s*=\\s*(["'])(.*?)\\1`, 'i'))?.[2]
export function isManagedMetadataTag(tag: string): boolean {
  if (/^<meta\b/i.test(tag)) {
    const key = attribute(tag, 'name') ?? attribute(tag, 'property') ?? ''
    return key === 'description' || /^(?:og:|twitter:)/i.test(key)
  }
  return /^<link\b/i.test(tag) && (attribute(tag, 'rel') === 'canonical' || attribute(tag, 'rel') === 'alternate' && !!attribute(tag, 'hreflang'))
}

export function applyPageMetadata(html: string, metadata: PageMetadata): string {
  const end = html.indexOf('</head>')
  if (end < 0) throw new Error('Page metadata requires an HTML head')
  const head = html.slice(0, end)
    .replace(/<html\b([^>]*?)\blang\s*=\s*(["']).*?\2([^>]*)>/i, `<html$1lang="${metadata.language}"$3>`)
    .replace(/(?:^[\t ]*)?<title\b[^>]*>[\s\S]*?<\/title>[\t ]*(?:\r?\n)?/gim, '')
    .replace(/(?:^[\t ]*)?<(?:meta|link)\b[^>]*>[\t ]*(?:\r?\n)?/gim, tag => isManagedMetadataTag(tag.trimStart()) ? '' : tag)
  const tags = [`    <title>${escapeHtml(metadata.title)}</title>`, ...metadataElements(metadata).map(({ tag, attributes }) =>
    `    <${tag} ${Object.entries(attributes).map(([key, value]) => `${key}="${escapeHtml(value)}"`).join(' ')} />`)]
  return head.trimEnd() + '\n' + tags.join('\n') + '\n  ' + html.slice(end)
}

export function staticFallback(metadata: PageMetadata, base = '/'): string {
  const title = metadata.title.replace(/\s*\|\s*FireWatch$/, '')
  const navigation = fallbackNavigation(metadata, base).map(item => `<li><a href="${escapeHtml(item.href)}"${item.lang ? ` lang="${item.lang}"` : ''}>${escapeHtml(item.title)}</a></li>`).join('')
  return `<main data-firewatch-static-fallback><header${metadata.greekOnly ? ' lang="el"' : ''}><h1>${escapeHtml(title)}</h1><p>${escapeHtml(metadata.description)}</p></header><nav aria-label="${escapeHtml(metadataText('Ενότητες FireWatch', metadata.language))}"><ul>${navigation}</ul></nav></main>`
}

// Only generated shells use this fallback. Existing rich Greek entry bodies
// remain intact; React still mounts the same application in every shell.
export function fillStaticFallback(html: string, metadata: PageMetadata, base = '/'): string {
  return html.replace(/<div id="root">\s*(?:<main(?: class="page-loading"| data-firewatch-static-fallback)[^>]*>[\s\S]*?<\/main>)?\s*<\/div>/, `<div id="root">${staticFallback(metadata, base)}</div>`)
}

export function metadataSitemap(): string {
  const entries = metadataPages.flatMap(page => (['el', 'en'] as const).map(language => {
    const metadata = getPageMetadata(language === 'en' ? `/en${page.path}` : page.path)!
    const links = metadata.alternates.map(({ language: alternate, href }) => `    <xhtml:link rel="alternate" hreflang="${alternate}" href="${escapeHtml(href)}" />`).join('\n')
    return `  <url>\n    <loc>${escapeHtml(metadata.canonical)}</loc>\n${links}\n    <changefreq>${page.changefreq}</changefreq>\n    <priority>${page.priority.toFixed(1)}</priority>\n  </url>`
  }))
  return `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:xhtml="http://www.w3.org/1999/xhtml">\n${entries.join('\n')}\n</urlset>\n`
}

export function metadataRewrites() {
  const rewrites: { source: string; destination: string }[] = []
  for (const page of metadataPages) for (const language of ['el', 'en'] as const) {
    const path = language === 'en' ? `/en${page.path}` : page.path
    if (path === '/') continue
    const destination = '/' + (language === 'en' ? 'en/' : '') + (page.path === '/' ? '' : page.path.replace(/^\//, '').replace(/\/$/, '') + '/') + 'index.html'
    for (const source of new Set([path.replace(/\/$/, ''), path.replace(/\/$/, '') + '/'])) rewrites.push({ source, destination })
  }
  for (const page of metadataPages.filter(page => page.path === '/analysis' || page.path.startsWith('/analysis/') && page.path !== '/analysis/west-attica-fire-2026/')) {
    for (const language of ['el', 'en'] as const) {
      const path = (language === 'en' ? '/en' : '') + page.path.replace(/^\/analysis/, '/reports')
      const destination = '/' + (language === 'en' ? 'en/' : '') + page.path.slice(1) + '/index.html'
      for (const source of [path, path + '/']) rewrites.push({ source, destination })
    }
  }
  return rewrites
}

export { SITE_ORIGIN }
