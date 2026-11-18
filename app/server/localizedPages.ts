import { readFile } from 'node:fs/promises'
import { resolve } from 'node:path'
import type { Plugin } from 'vite'
import { isAppPagePath, languageFromPath, STANDALONE_PATH } from '../src/routing/localizedHref'
import { getPageMetadata, metadataDocument, metadataPages } from '../src/seo/pageMetadata'
import { applyPageMetadata, fillStaticFallback, metadataSitemap } from '../src/seo/metadataHtml'

export function standaloneHtml(html: string, navigationScript: string, pathname?: string): string {
  const metadata = pathname ? getPageMetadata(pathname) : null
  const prepared = metadata ? fillStaticFallback(applyPageMetadata(html, metadata), metadata) : html
  return prepared.replace('</body>', `  <script type="module" src="${navigationScript}"></script>\n  </body>`)
}

export function previewDocumentUrl(requestUrl: string, base = '/'): string {
  const url = new URL(requestUrl, 'http://localhost')
  const pathname = base === '/' ? url.pathname : url.pathname.slice(base.replace(/\/$/, '').length)
  const document = metadataDocument(pathname)
  if (document) return `${base}${document}${url.search}`
  if (languageFromPath(pathname) === 'en' && (isAppPagePath(pathname) || (/^\/en\/reports(?:\/|$)/.test(pathname) && !/\.[^/]+$/.test(pathname)))) {
    return `${base}en/index.html${url.search}`
  }
  return requestUrl
}

export function localizedPages(appRoot: string): Plugin {
  const standaloneIndex = resolve(appRoot, `public${STANDALONE_PATH}/index.html`)
  const navigationEntry = resolve(appRoot, 'src/routing/standaloneNavigation.ts')
  let base = '/'
  return {
    name: 'localized-firewatch-pages',
    configResolved(config) { base = config.base },
    transformIndexHtml: {
      order: 'post',
      handler(html, context) {
        const metadata = getPageMetadata(context.path.replace(/\/index\.html$/, '/'))
        return metadata ? applyPageMetadata(html, metadata) : html
      },
    },
    configureServer(server) {
      server.middlewares.use(async (req, res, next) => {
        if (!req.url || req.method !== 'GET') return next()
        const url = new URL(req.url, 'http://localhost')
        const pathname = base === '/' ? url.pathname : url.pathname.slice(base.replace(/\/$/, '').length)
        const metadata = getPageMetadata(pathname)
        if (!metadata) return next()
        try {
          let html: string
          if (metadata.page.path.replace(/\/$/, '') === STANDALONE_PATH) {
            html = standaloneHtml(await readFile(standaloneIndex, 'utf8'), `${base}src/routing/standaloneNavigation.ts`, pathname)
          } else {
            const source = metadata.language === 'el' && metadata.page.sourceEntry ? metadata.page.sourceEntry : 'en/index.html'
            html = applyPageMetadata(await readFile(resolve(appRoot, source), 'utf8'), metadata)
            if (metadata.language === 'en' || !metadata.page.sourceEntry) html = fillStaticFallback(html, metadata, base)
            html = await server.transformIndexHtml(url.pathname, html)
          }
          res.statusCode = 200
          res.setHeader('Content-Type', 'text/html; charset=utf-8')
          res.setHeader('Cache-Control', 'no-store')
          res.end(html)
        } catch (error) { next(error as Error) }
      })
    },
    configurePreviewServer(server) {
      server.middlewares.use((req, _res, next) => {
        if (!req.url || req.method !== 'GET') return next()
        req.url = previewDocumentUrl(req.url, base)
        next()
      })
    },
    generateBundle: {
      order: 'post',
      async handler(_options, bundle) {
        const entry = Object.values(bundle).find(item => item.type === 'chunk' && item.facadeModuleId === navigationEntry)
        if (!entry) throw new Error('Standalone language navigation entry is missing')
        const templates = new Map(Object.entries(bundle).flatMap(([file, item]) => item.type === 'asset' && file.endsWith('.html') ? [[file, String(item.source)] as const] : []))
        const standalone = await readFile(standaloneIndex, 'utf8')
        for (const page of metadataPages) for (const language of ['el', 'en'] as const) {
          const pathname = language === 'en' ? `/en${page.path}` : page.path
          const metadata = getPageMetadata(pathname)!
          const fileName = metadataDocument(pathname)!
          let html: string
          if (page.path.replace(/\/$/, '') === STANDALONE_PATH) {
            html = standaloneHtml(standalone, `${base}${entry.fileName}`, pathname)
          } else {
            const source = language === 'el' && page.sourceEntry ? page.sourceEntry : 'en/index.html'
            const template = templates.get(source)
            if (!template) throw new Error(`Missing built HTML template: ${source}`)
            html = applyPageMetadata(template, metadata)
            if (language === 'en' || !page.sourceEntry) html = fillStaticFallback(html, metadata, base)
          }
          const existing = bundle[fileName]
          if (existing?.type === 'asset') existing.source = html
          else this.emitFile({ type: 'asset', fileName, source: html })
        }
        this.emitFile({ type: 'asset', fileName: 'sitemap.xml', source: metadataSitemap() })
      },
    },
  }
}
