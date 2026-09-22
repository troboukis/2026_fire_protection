import { useEffect, useState, type CSSProperties } from 'react'
import { Link } from 'react-router-dom'
import ComponentTag from '../../../components/ComponentTag'
import './AnalysisPromo.css'

const slides = [
  {
    eyebrow: 'AntiNERO / Δυτική Αττική',
    title: 'Πώς επηρέασαν τα AntiNERO έργα τη μεγάλη πυρκαγιά της Δυτικής Αττικής το 2026;',
    detail: 'WWF, Flame και FireWatch παρουσιάζουν μία ανάλυση της μεγαλύτερης πυρκαγιάς του 2026.',
    to: '/analysis/west-attica-fire-2026/',
    visual: 'map',
    reloadDocument: true,
  },
  {
    eyebrow: 'Στατιστική ανάλυση',
    title: 'Μία στις δύο συμβάσεις με Απευθείας Ανάθεση',
    detail: 'Η πλειονότητα των συμβάσεων έχει αξία που φτάνει στο νόμιμο όριο των απευθείας αναθέσεων, δηλαδή τις 30.000 ευρώ.',
    to: '/analysis/statistics',
    visual: 'statistics',
    reloadDocument: false,
  },
  {
    eyebrow: 'Δυτική Αττική',
    title: 'Εξερευνήστε το δίκτυο συμβάσεων έργων AntiNERO',
    detail: 'Πότε έπρεπε να ολοκληρωθεί κάθε έργο και πότε ολοκληρώθηκε στην πράξη, σύμφωνα με τα διαθέσιμα δημόσια έγγραφα στη Διαύγεια.',
    to: '/analysis/antinero-west-attica',
    visual: 'network',
    reloadDocument: false,
  },
] as const

const SLIDE_DURATION_MS = 6000

// Compact previews for the analysis slides.
function PromoIllustration({ visual }: { visual: typeof slides[number]['visual'] }) {
  return (
    <svg className="analysis-promo__illustration" viewBox="0 0 560 140" aria-hidden="true" focusable="false">
      {visual === 'statistics' ? (
        <g>
          {[36, 58, 48, 82, 71, 105, 122].map((height, index) => (
            <rect
              key={index}
              className="analysis-promo__bar"
              x={40 + index * 70}
              y={135 - height}
              width="42"
              height={height}
              style={{ '--item-delay': `${index * 75}ms` } as CSSProperties}
            />
          ))}
          <path className="analysis-promo__baseline" d="M24 135H540" />
        </g>
      ) : visual === 'map' ? (
        <g>
          <path className="analysis-promo__map-land" d="M28 108L55 86L47 63L84 44L124 51L151 27L202 36L230 56L281 47L317 25L363 36L391 63L436 54L472 73L520 67L541 94L515 121L470 116L434 130L390 114L350 123L312 102L270 118L225 103L184 126L143 108L105 121L72 106Z" />
          <path className="analysis-promo__map-contour" d="M57 88C112 71 157 84 207 63S302 46 357 59S452 98 516 79" />
          <path className="analysis-promo__map-contour" d="M76 105C133 90 166 105 224 84S330 68 387 83S454 111 499 103" />
          <path className="analysis-promo__map-burn" d="M83 78C105 51 150 43 185 56C214 67 230 87 213 103C193 121 151 109 124 112C94 116 66 99 83 78Z" />
          <path className="analysis-promo__map-route" pathLength="1" d="M70 98C130 72 174 89 231 69S340 48 397 72S468 88 521 78" />
          {[[102, 77], [154, 92], [202, 68], [275, 60], [344, 58], [411, 78], [481, 82]].map(([cx, cy], index) => (
            <circle
              key={index}
              className="analysis-promo__map-point"
              cx={cx}
              cy={cy}
              r={index < 3 ? 6 : 4}
              style={{ '--item-delay': `${index * 85}ms` } as CSSProperties}
            />
          ))}
        </g>
      ) : (
        <g>
          <path className="analysis-promo__connections" pathLength="1" d="M60 75L188 28L314 70L470 24M60 75L198 120L314 70L486 117M188 28L198 120M314 70L506 72M470 24L506 72L486 117" />
          {[[30, 75], [94, 28], [99, 120], [157, 70], [235, 24], [253, 72], [243, 117]].map(([cx, cy], index) => (
            <circle
              key={index}
              className={`analysis-promo__node${index === 3 ? ' analysis-promo__node--central' : ''}`}
              cx={cx * 2}
              cy={cy}
              r={index === 3 ? 13 : 7}
              style={{ '--item-delay': `${index * 85}ms` } as CSSProperties}
            />
          ))}
        </g>
      )}
    </svg>
  )
}

export default function AnalysisPromo() {
  const [activeIndex, setActiveIndex] = useState(0)
  const [paused, setPaused] = useState(false)
  const [hovered, setHovered] = useState(false)
  const [focused, setFocused] = useState(false)
  const [reducedMotion, setReducedMotion] = useState(() => (
    typeof window !== 'undefined' && window.matchMedia('(prefers-reduced-motion: reduce)').matches
  ))
  const slide = slides[activeIndex]

  useEffect(() => {
    const media = window.matchMedia('(prefers-reduced-motion: reduce)')
    const update = () => setReducedMotion(media.matches)
    update()
    media.addEventListener('change', update)
    return () => media.removeEventListener('change', update)
  }, [])

  useEffect(() => {
    if (paused || hovered || focused || reducedMotion) return
    const timer = window.setTimeout(() => {
      setActiveIndex((index) => (index + 1) % slides.length)
    }, SLIDE_DURATION_MS)
    return () => window.clearTimeout(timer)
  }, [activeIndex, paused, hovered, focused, reducedMotion])

  return (
    <section
      className="analysis-promo"
      aria-label="Προτεινόμενες αναλύσεις"
      aria-roledescription="καρουζέλ"
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      onFocusCapture={() => setFocused(true)}
      onBlurCapture={(event) => {
        if (!event.currentTarget.contains(event.relatedTarget as Node | null)) setFocused(false)
      }}
    >
      <ComponentTag name="AnalysisPromo" />
      <div className="analysis-promo__header">
        <span className="analysis-promo__eyebrow">{slide.eyebrow}</span>
        <div className="analysis-promo__controls" role="group" aria-label="Επιλογή ανάλυσης">
          {slides.map((item, index) => (
            <button
              key={item.to}
              type="button"
              className="analysis-promo__selector"
              aria-label={item.title}
              aria-pressed={index === activeIndex}
              onClick={() => setActiveIndex(index)}
            >
              {String(index + 1).padStart(2, '0')}
            </button>
          ))}
          {!reducedMotion && (
            <button
              type="button"
              className="analysis-promo__pause"
              aria-label={paused ? 'Συνέχεια αυτόματης εναλλαγής' : 'Παύση αυτόματης εναλλαγής'}
              onClick={() => setPaused((value) => !value)}
            >
              <svg viewBox="0 0 16 16" aria-hidden="true" focusable="false">
                {paused ? <path d="M5 3L13 8L5 13Z" /> : <path d="M4 3H6V13H4ZM10 3H12V13H10Z" />}
              </svg>
            </button>
          )}
        </div>
      </div>
      <div className="analysis-promo__viewport" aria-live={paused || focused || reducedMotion ? 'polite' : 'off'} aria-atomic="true">
        <Link key={slide.to} className="analysis-promo__slide" to={slide.to} reloadDocument={slide.reloadDocument}>
          <div className="analysis-promo__copy">
            <h2>{slide.title}</h2>
            <p>{slide.detail}</p>
          </div>
          <div className="analysis-promo__visual">
            <PromoIllustration visual={slide.visual} />
            <span className="analysis-promo__action">
              <span>Περισσότερα</span>
              <span className="analysis-promo__arrow" aria-hidden="true">↗</span>
            </span>
          </div>
        </Link>
      </div>
    </section>
  )
}
