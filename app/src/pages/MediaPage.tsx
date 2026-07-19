import type { ReactNode } from 'react'

type MediaItem = {
  title: string
  source: string
  date?: string
  href?: string
  image?: string
}

const articles: MediaItem[] = [
  {
    title: 'FireWatch: Η πλατφόρμα που αποκαλύπτει τι έγινε πριν από κάθε δασική πυρκαγιά',
    source: 'ThessPost',
    href: 'https://thesspost.gr/fire-watch-i-platforma-pou-apokalyptei-ti-egine-prin-apo-kathe-dasiki-pyrkagia/',
    image: 'https://thesspost.gr/wp-content/uploads/2025/08/pyrkagia-fotia.webp',
  },
  {
    title: 'Ο εμπνευστής του FireWatch στο tvxs: «Φέτος δαπανήθηκε ποσό 48% μικρότερο από πέρυσι για την πυροπροστασία»',
    source: 'TVXS',
    href: 'https://tvxs.gr/news/ellada/o-empneystis-toy-firewatch-sto-tvxs-fetos-dapanithike-poso-48-mikrotero-apo-perysi-gia-tin-pyroprostasia/',
    image: `${import.meta.env.BASE_URL}cover_16_9_social.jpg`,
  },
  {
    title: '8 ανησυχητικά στοιχεία για την πυροπροστασία στην Ελλάδα, σύμφωνα με το FireWatch',
    source: 'OneMan',
    href: 'https://www.oneman.gr/synentefxeis/8-anisixitika-stoixeia-gia-tin-piroprostasia-stin-ellada-simfona-me-to-firewatch/',
    image: 'https://media.oneman.gr/onm-images/npa048921723398101-1.jpg',
  },
  {
    title: 'FireWatch: Οι δημόσιες συμβάσεις για την πυροπροστασία και τα δεδομένα για τις πυρκαγιές, σε μία εφαρμογή',
    source: 'iMEdD Lab',
    href: 'https://lab.imedd.org/firewatch-oi-dimosies-symvaseis-gia-tin-pyroprostasia-kai-ta-dedomena-gia-tis-pyrkagies-se-mia-efarmogi/',
    image: 'https://lab.imedd.org/wp-content/uploads/2026/06/firewatch_main_image.jpg',
  },
  {
    title: 'FireWatch, μία σούπερ εφαρμογή για τις δασικές πυρκαγιές',
    source: 'e-tetRadio',
    href: 'https://www.e-tetradio.gr/Article/45872/firewatch-mia-soyper-efarmogh-gia-tis-dasikes-pyrkagies',
    image: 'https://www.e-tetradio.gr/img/path/cc8eccde-5f5d-4ccc-a259-5872680986d8_684491817_10163560741714845_7440036278199405789_n.jpg',
  },
  {
    title: 'FireWatch: από τα δημόσια δεδομένα σε ένα εργαλείο πυροπροστασίας',
    source: 'WIRED Greece',
    href: 'https://wired.com.gr/article/firewatch-pos-o-thanasis-troboukis-metetrepse-ta-dimosia-dedomena-se-ergaleio-pyroprostasias/',
    image: 'https://wired.com.gr/wp-content/uploads/2026/05/firefighter.jpg',
  },
  {
    title: 'FireWatch: Τα δεδομένα πίσω από τις φωτιές',
    source: 'ROSA',
    href: 'https://www.rosa.gr/videos/firewatch-ta-dedomena-piso-apo-tis-foties/',
    image: `${import.meta.env.BASE_URL}cover_16_9.png`,
  },
  {
    title: 'Άρθρο για την πυροπροστασία',
    source: 'Τα Νέα',
    date: '30 Μαΐου 2026',
    image: `${import.meta.env.BASE_URL}cover_16_9_social.jpg`,
  },
]

const radioShows: MediaItem[] = [
  {
    title: 'ΙΝΤΡΙΓΚΕΣ ΣΤΟ ΚΟΚΚΙΝΟ 105.5 με τον Αντώνη Ρηγόπουλο',
    source: 'Στο Κόκκινο 105.5',
    date: '20 Ιουνίου 2026',
  },
  {
    title: 'Στον αέρα με τη Νίκυ Λυμπεράκη',
    source: 'Παραπολιτικά 90.1',
    date: '5 Μαΐου 2026',
    href: 'https://www.mixcloud.com/parapolitika901/%CF%83%CF%84%CE%BF%CE%BD-%CE%B1%CE%B5%CF%81%CE%B1-05-05-2026/',
  },
  {
    title: '#NaMaste @ΔΕΥΤΕΡΟ με την Ιωάννα Νιαώτη',
    source: 'Δεύτερο Πρόγραμμα',
    date: '24 Μαΐου 2026',
    href: 'https://www.ertecho.gr/radio/deftero/show/namaste-deytero-deytero/ondemand/1300432/namaste-deytero-me-tin-ioanna-niaoti-24-05-2026/',
  },
  {
    title: 'Μένουμε Αθήνα με τη Μαρία Παύλου',
    source: 'Αθήνα 98.4',
    date: '4 Ιουλίου 2026',
    href: 'https://www.mixcloud.com/%CE%91%CE%98%CE%97%CE%9D%CE%91984/%CE%BC%CE%AD%CE%BD%CE%BF%CF%85%CE%BC%CE%B5-%CE%B1%CE%B8%CE%AE%CE%BD%CE%B1-04-07-2026/',
  },
]

const televisionShows: MediaItem[] = [
  {
    title: 'UPDATE',
    source: 'ΕΡΤ',
    date: '2 Ιουλίου 2026',
  },
  {
    title: 'Mega News',
    source: 'MEGA',
    date: '1 Ιουλίου 2026',
    href: 'https://www.megatv.com/etvshows/2422339/01-07-26-4/',
  },
]

function CardShell({ item, className, children }: { item: MediaItem; className: string; children: ReactNode }) {
  if (!item.href) return <article className={className}>{children}</article>

  return (
    <a className={className} href={item.href} target="_blank" rel="noopener noreferrer">
      {children}
    </a>
  )
}

function ExternalLinkIcon() {
  return (
    <span className="media-card__action media-card__action--icon" aria-hidden="true">
      <svg viewBox="0 0 24 24" focusable="false">
        <path d="M14 5h5v5M19 5l-9 9M18 13v6H5V6h6" />
      </svg>
    </span>
  )
}

function ArticleCard({ item }: { item: MediaItem }) {
  return (
    <CardShell item={item} className={`media-card media-card--article${item.href ? '' : ' media-card--static'}`}>
      <div className="media-card__image-wrap">
        <img className="media-card__image" src={item.image} alt="" loading="lazy" decoding="async" referrerPolicy="no-referrer" />
      </div>
      <div className="media-card__body">
        <div className="media-card__meta">
          <span>{item.source}</span>
          {item.date ? <time>{item.date}</time> : null}
        </div>
        <h3>{item.title}</h3>
        {item.href ? <ExternalLinkIcon /> : null}
      </div>
    </CardShell>
  )
}

function BroadcastVisual({ kind, station }: { kind: 'radio' | 'tv'; station: string }) {
  return (
    <div className={`media-broadcast-visual media-broadcast-visual--${kind}`} aria-hidden="true">
      <div className="media-broadcast-visual__signal">
        {Array.from({ length: 17 }, (_, index) => <i key={index} />)}
      </div>
      <span className="media-broadcast-visual__kind">{kind === 'radio' ? 'ON AIR' : 'TV'}</span>
      <strong>{station}</strong>
      <span className="media-broadcast-visual__frequency">{kind === 'radio' ? 'FM / AUDIO' : 'LIVE / NEWS'}</span>
    </div>
  )
}

function BroadcastCard({ item, kind }: { item: MediaItem; kind: 'radio' | 'tv' }) {
  return (
    <CardShell item={item} className={`media-card media-card--broadcast${item.href ? '' : ' media-card--static'}`}>
      <BroadcastVisual kind={kind} station={item.source} />
      <div className="media-card__body">
        <div className="media-card__meta">
          <span>{item.source}</span>
          {item.date ? <time>{item.date}</time> : null}
        </div>
        <h3>{item.title}</h3>
        {item.href ? <ExternalLinkIcon /> : null}
      </div>
    </CardShell>
  )
}

export default function MediaPage() {
  return (
    <main className="media-page">
      <header className="media-page__hero section-rule">
        <div className="eyebrow">FireWatch / Media</div>
        <h2>Δημοσιεύσεις</h2>
        <p>Άρθρα, συνεντεύξεις και εμφανίσεις σε ραδιόφωνο και τηλεόραση.</p>
      </header>

      <section className="media-section section-rule" aria-label="Τύπος και διαδίκτυο">
        <header className="media-section__header">
          <div className="eyebrow">01 / Τύπος &amp; διαδίκτυο</div>
        </header>
        <div className="media-grid media-grid--articles">
          {articles.map((item) => <ArticleCard key={`${item.source}-${item.title}`} item={item} />)}
        </div>
      </section>

      <section className="media-section section-rule" aria-label="Ραδιόφωνο">
        <header className="media-section__header">
          <div className="eyebrow">02 / Ραδιόφωνο</div>
        </header>
        <div className="media-grid">
          {radioShows.map((item) => <BroadcastCard key={`${item.source}-${item.title}`} item={item} kind="radio" />)}
        </div>
      </section>

      <section className="media-section section-rule" aria-label="Τηλεόραση">
        <header className="media-section__header">
          <div className="eyebrow">03 / Τηλεόραση</div>
        </header>
        <div className="media-grid">
          {televisionShows.map((item) => <BroadcastCard key={`${item.source}-${item.title}`} item={item} kind="tv" />)}
        </div>
      </section>
    </main>
  )
}
