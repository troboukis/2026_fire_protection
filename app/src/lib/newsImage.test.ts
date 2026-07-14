import { describe, expect, it } from 'vitest'
import { buildNewsImageSources } from './newsImage'

describe('buildNewsImageSources', () => {
  it('requests compact WordPress CDN variants for publisher images', () => {
    const result = buildNewsImageSources(
      'https://www.in.gr/wp-content/uploads/2026/07/fire-photo-620x350.jpg',
    )

    expect(result.src).toBe(
      'https://i0.wp.com/www.in.gr/wp-content/uploads/2026/07/fire-photo-620x350.jpg?w=320&quality=72',
    )
    expect(result.srcSet).toContain('w=640&quality=72')
  })

  it('leaves non-WordPress image URLs unchanged', () => {
    const imageUrl = 'https://cdn.example.com/fire.jpg'
    expect(buildNewsImageSources(imageUrl)).toEqual({ src: imageUrl })
  })

  it('leaves invalid URLs unchanged', () => {
    expect(buildNewsImageSources('not a URL')).toEqual({ src: 'not a URL' })
  })
})
