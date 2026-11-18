import { getPageMetadata, metadataElements } from './pageMetadata'
import { isManagedMetadataTag } from './metadataHtml'

let activeDocument: Document | undefined
let activeCanonical: string | undefined

export function syncDocumentMetadata(pathname: string): void {
  if (typeof document === 'undefined' || !document.head) return
  const metadata = getPageMetadata(pathname)
  if (!metadata) return
  if (activeDocument === document && activeCanonical === metadata.canonical && document.title === metadata.title) return
  document.documentElement.lang = metadata.language
  document.title = metadata.title
  for (const element of document.head.querySelectorAll('meta, link')) {
    if (isManagedMetadataTag(element.outerHTML)) element.remove()
  }
  for (const { tag, attributes } of metadataElements(metadata)) {
    const element = document.createElement(tag)
    for (const [key, value] of Object.entries(attributes)) element.setAttribute(key, value)
    document.head.appendChild(element)
  }
  activeDocument = document
  activeCanonical = metadata.canonical
}
