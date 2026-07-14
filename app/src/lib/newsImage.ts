const WORDPRESS_UPLOAD_PATH = '/wp-content/uploads/'

function buildWordPressCdnUrl(sourceUrl: URL, width: number): string {
  const cdnUrl = new URL(`https://i0.wp.com/${sourceUrl.hostname}${sourceUrl.pathname}`)
  cdnUrl.searchParams.set('w', String(width))
  cdnUrl.searchParams.set('quality', '72')
  return cdnUrl.toString()
}

export type NewsImageSources = {
  src: string
  srcSet?: string
}

export function buildNewsImageSources(imageUrl: string): NewsImageSources {
  try {
    const sourceUrl = new URL(imageUrl)
    const isWordPressUpload = (
      sourceUrl.protocol === 'https:'
      && sourceUrl.pathname.includes(WORDPRESS_UPLOAD_PATH)
    )
    if (!isWordPressUpload) return { src: imageUrl }

    const small = buildWordPressCdnUrl(sourceUrl, 320)
    const large = buildWordPressCdnUrl(sourceUrl, 640)
    return {
      src: small,
      srcSet: `${small} 320w, ${large} 640w`,
    }
  } catch {
    return { src: imageUrl }
  }
}
