// Lists and matching rules of the FireWatch collection pipeline, shown in the
// methodology guide. They mirror the Python sources: PRIMARY/SECONDARY CPVs and
// title keywords in src/fetch_kimdis_procurements.py, negative keywords in
// src/keyword_exclusions.py. Update both together.
// Editorial choice (2026-11-18): the guide leaves out the pipeline's stricter rule for
// nine very generic Secondary CPVs (TITLE_REQUIRED_CPV_KEYS / FIRE_TITLE_KEYWORDS);
// they are listed here as ordinary Secondary CPVs.

export const PRIMARY_CPVS: Record<string, string> = {
  '75251100-1': 'Υπηρεσίες καταπολέμησης πυρκαγιών',
  '75251110-4': 'Υπηρεσίες πρόληψης πυρκαγιών',
  '75251120-7': 'Υπηρεσίες καταπολέμησης δασοπυρκαγιών',
  '77200000-2': 'Υπηρεσίες δασοκομίας',
  '77312000-0': 'Υπηρεσίες εκκαθάρισης από αγριόχορτα',
  '77314000-4': 'Υπηρεσίες συντήρησης οικοπέδων',
  '77340000-5': 'Κλάδεμα δένδρων και θάμνων',
  '45343000-3': 'Εργασίες εγκαταστάσεων πρόληψης πυρκαγιάς',
  '77314100-5': 'Υπηρεσίες χορτοκάλυψης',
  '34144200-0': 'Οχήματα για τις υπηρεσίες εκτάκτου ανάγκης',
  '34144212-7': 'Υδροφόρες πυροσβεστικών οχημάτων',
  '35111000-5': 'Πυροσβεστικός εξοπλισμός',
  '35111200-7': 'Υλικά πυρόσβεσης',
  '44480000-8': 'Ποικίλος εξοπλισμός πυροπροστασίας',
  '45343100-4': 'Εργασίες πυροπροστασίας',
  '60442000-8': 'Υπηρεσίες πυρόσβεσης δασικών πυρκαγιών από τον αέρα',
  '77231600-4': 'Υπηρεσίες αναδάσωσης',
  '35111510-3': 'Πυροσβεστικά εργαλεία χειρός',
}

export const SECONDARY_CPVS: Record<string, string> = {
  '71320000-7': 'Υπηρεσίες εκπόνησης τεχνικών μελετών',
  '43210000-8': 'Μηχανήματα χωματουργικών εργασιών',
  '90522400-6': 'Καθαρισμός και επεξεργασία εδάφους',
  '90530000-1': 'Λειτουργία χώρου απόθεσης απορριμμάτων',
  '35125300-2': 'Κάμερες ασφαλείας',
  '16000000-5': 'Γεωργικά μηχανήματα',
  '35110000-8': 'Εξοπλισμός πυρόσβεσης',
  '79714000-2': 'Υπηρεσίες επιτήρησης',
  '77231000-8': 'Υπηρεσίες διαχείρισης δασοκομίας',
  '45233141-9': 'Συντήρηση οδών',
  '45240000-1': 'Κατασκευαστικές εργασίες για υδατικά έργα',
  '45232152-2': 'Έργα αντιπλημμυρικής / αποχετευτικής υποδομής',
  '77231500-3': 'Υπηρεσίες παρακολούθησης ή αξιολόγησης δασών',
  '77231300-1': 'Υπηρεσίες διαχείρισης δασών',
  '79933000-3': 'Υπηρεσίες υποστήριξης στον τομέα του σχεδιασμού',
  '77231700-5': 'Υπηρεσίες δασικής επέκτασης',
  '77231900-7': 'Υπηρεσίες δασικού τομεακού σχεδιασμού',
  '90910000-9': 'Υπηρεσίες καθαρισμού',
  '77341000-2': 'Κλάδεμα δένδρων',
  '45520000-8': 'Ενοικίαση εξοπλισμού χωματουργικών εργασιών με χειριστή',
  '71242000-6': 'Προετοιμασία έργων και σχεδίων',
  '44611500-1': 'Δεξαμενές νερού',
  '42131160-5': 'Δίκτυο κρουνών υδροληψίας',
  '90721800-5': 'Υπηρεσίες προστασίας από φυσικούς υπαρκτούς ή δυνητικούς κινδύνους',
  '79415200-8': 'Υπηρεσίες παροχής συμβουλών σε θέματα σχεδιασμού',
  '35810000-5': 'Ατομικός εξοπλισμός',
  '50532300-6': 'Υπηρεσίες επισκευής και συντήρησης γεννητριών',
  '45330000-9': 'Υδραυλικές εργασίες',
  '77310000-6': 'Φύτευση και συντήρηση χώρων πρασίνου',
  '35111400-9': 'Εξοπλισμός πυρασφάλειας',
  '42122130-0': 'Αντλίες νερού',
  '31521320-3': 'Ηλεκτρικοί φορητοί φανοί',
  '35111320-4': 'Φορητοί πυροσβεστήρες',
  '44482200-4': 'Κρουνοί υδροληψίας για πυρόσβεση',
  '45233120-6': 'Έργα οδοποιίας',
  '50800000-3': 'Διάφορες υπηρεσίες επισκευής και συντήρησης',
  '77230000-1': 'Υπηρεσίες σχετιζόμενες με τη δασοκομία',
  '32441100-7': 'Τηλεμετρικό σύστημα παρακολούθησης',
  '32580000-2': 'Εξοπλισμός δικτύου δεδομένων',
  '48420000-8': 'Πακέτα λογισμικού διαχείρισης εγκαταστάσεων',
  '60424100-7': 'Ενοικίαση αεροσκαφών με πλήρωμα',
  '50211310-3': 'Υπηρεσίες γενικής επισκευής κινητήρων αεροσκαφών',
  '50211100-8': 'Υπηρεσίες συντήρησης αεροσκαφών',
  '45000000-7': 'Κατασκευαστικές εργασίες',
  '34731000-0': 'Μέρη αεροσκαφών',
  '34731500-5': 'Μέρη κινητήρων αεροσκαφών',
  '35642000-7': 'Ανταλλακτικά για στρατιωτικό αεροδιαστημικό εξοπλισμό',
  '43200000-5': 'Μηχανήματα χωματουργικών εργασιών και εκσκαφών',
  '90610000-6': 'Υπηρεσίες καθαρισμού και σάρωσης οδών',
}

export const POSITIVE_KEYWORDS = ['πυροπροστασ', 'Πολιτικής προστασίας', 'αντιπυρικ', 'δασικών πυρκαγιών', 'δασικών δρόμων', 'κατασβεσ', 'αεροπυροσβ', 'ΠΥΡΟΣΒ']

// Value: maximum number of words allowed between consecutive roots of a phrase.
export const NEGATIVE_KEYWORDS: Record<string, number> = {
  'σχολει': 2, 'σχολικ': 2, 'πανεπιστημ': 2, 'γηπεδ': 2, 'πιστοποιητικ πυρασφαλ': 2, 'κηποτεχνικ συντηρησ': 2, 'συντηρησ κηπ': 2,
  'οακα': 2, 'παραγωγ φυτ': 2, 'φυτωριακ εργασ': 2, 'δασικ φυτωρι': 2, 'καυσιμ θερμανσ': 2, 'κολυμβητηρ': 2, 'server room': 2,
  'προσχολικ': 2, 'διασχολικ': 2, 'γυμνασ': 2, 'λυκει': 2, 'αρσ επικινδυνοτ εγκαταλειμ κτιρι': 2,
  'συμβασ παροχ καθαριοτ κτιριακ εγκαταστασ': 2, 'υπηρεσ καθαρισ χωρ κτιρι': 2, 'πλημμυρ φαινομ': 2, 'χλοοταπητ': 2,
  'μουσει': 2, 'συστηματ πυρασφαλ': 2, 'αναβαθμισ συστηματ πυροσβεσ': 2, 'εγκαταστ συστηματ πυροσβεσ': 2,
  'πυροσβεστηρ αυτοκινητ': 2, 'αδεσποτ ζω': 2, 'κλειστ γυμναστηρ': 2, 'εγκαταστ εξοπλισμ συστηματ ενεργητικ παθητικ πυροπροστασ': 2,
  'χιονοπτ': 2, 'καθαρισ δημοτικ κοιμητηρ': 2, 'daniel': 2, 'ντανιελ': 2, 'προμηθ τοποθετ πυροσβεστικ φωλ': 2,
  'πυροπροστασ κτιριακ εγκαταστασ': 2, 'νοσοκομειο': 0, 'σχολ': 0, 'παντειο': 0, 'επιθεωρηση εργασιασ': 0, 'αναγομ': 0,
  'αντλητικ': 0, 'επαλ': 0, 'βρεφο': 0, 'παιδικ': 0, 'συντηρηση χωρων πρασινου': 0, 'sports4all': 0, 'κινηματογραφο': 0,
  'aqua montis': 0, 'αναδειξη υφισταμενων διαδρομων': 0, 'αναδειξη υφοσταμενων διαδρομων': 0, 'νηπιαγ': 2,
  'δημοτικ σχολει': 2, 'θεατρ': 2, 'πυροσβεστηρ': 2, 'γνθ': 0, 'γ ν θ': 0, 'ανακαιν': 2, 'νοσοκομ': 2, 'αλατοδιανομ': 2, 'εκχιονισμ': 2,
}

export const NEGATIVE_GROUPS: [string, string[]][] = [
  ['Εκπαίδευση', ['σχολει', 'σχολικ', 'σχολ', 'προσχολικ', 'διασχολικ', 'δημοτικ σχολει', 'νηπιαγ', 'βρεφο', 'παιδικ', 'γυμνασ', 'λυκει', 'επαλ', 'πανεπιστημ', 'παντειο']],
  ['Υγεία', ['νοσοκομειο', 'νοσοκομ', 'γνθ', 'γ ν θ']],
  ['Αθλητισμός, πολιτισμός', ['γηπεδ', 'κολυμβητηρ', 'κλειστ γυμναστηρ', 'οακα', 'sports4all', 'μουσει', 'θεατρ', 'κινηματογραφο', 'aqua montis']],
  ['Πυρασφάλεια κτιρίων', ['πιστοποιητικ πυρασφαλ', 'συστηματ πυρασφαλ', 'αναβαθμισ συστηματ πυροσβεσ', 'εγκαταστ συστηματ πυροσβεσ', 'εγκαταστ εξοπλισμ συστηματ ενεργητικ παθητικ πυροπροστασ', 'πυροπροστασ κτιριακ εγκαταστασ', 'πυροσβεστηρ', 'πυροσβεστηρ αυτοκινητ', 'αναγομ', 'προμηθ τοποθετ πυροσβεστικ φωλ', 'server room', 'αντλητικ']],
  ['Κτίρια, καθαριότητα', ['αρσ επικινδυνοτ εγκαταλειμ κτιρι', 'συμβασ παροχ καθαριοτ κτιριακ εγκαταστασ', 'υπηρεσ καθαρισ χωρ κτιρι', 'ανακαιν', 'καθαρισ δημοτικ κοιμητηρ']],
  ['Αστικό πράσινο, φυτώρια', ['κηποτεχνικ συντηρησ', 'συντηρησ κηπ', 'χλοοταπητ', 'συντηρηση χωρων πρασινου', 'παραγωγ φυτ', 'φυτωριακ εργασ', 'δασικ φυτωρι', 'αναδειξη υφισταμενων διαδρομων', 'αναδειξη υφοσταμενων διαδρομων']],
  ['Χιόνι, πλημμύρες, θέρμανση', ['χιονοπτ', 'εκχιονισμ', 'αλατοδιανομ', 'πλημμυρ φαινομ', 'καυσιμ θερμανσ', 'daniel', 'ντανιελ']],
  ['Άλλα', ['αδεσποτ ζω', 'επιθεωρηση εργασιασ']],
]

export const COPY_PAYLOADS = {
  primary: PRIMARY_CPVS,
  secondary: SECONDARY_CPVS,
  positive: POSITIVE_KEYWORDS,
  negative: { note: 'value = max words allowed between consecutive roots', keywords: NEGATIVE_KEYWORDS },
}

// Same normalization and matching as src/keyword_exclusions.py.
export function negativeTokens(value: string): string[] {
  return value.normalize('NFD').replace(/\p{Mn}/gu, '').toLowerCase().replace(/ς/g, 'σ').split(/[^\p{L}\p{N}_]+/u).filter(Boolean)
}

export function matchPrefix(text: string[], keyword: string[], gap: number): number[] | null {
  if (!text.length || !keyword.length || keyword.length > text.length) return null
  for (let start = 0; start < text.length; start++) {
    if (!text[start].startsWith(keyword[0])) continue
    let paths = [[start]]
    for (const root of keyword.slice(1)) {
      const next: number[][] = []
      for (const path of paths) {
        const last = path[path.length - 1]
        for (let index = last + 1; index < Math.min(text.length, last + gap + 2); index++) {
          if (text[index].startsWith(root)) next.push([...path, index])
        }
      }
      paths = next
      if (!paths.length) break
    }
    if (paths.length) return paths[0]
  }
  return null
}

// Same normalization as fetch_kimdis_procurements.normalize_string.
export function normalizeTitle(value: string): string {
  return value.toLowerCase().normalize('NFD').replace(/\p{Mn}/gu, '').split(/\s+/).filter(Boolean).join(' ')
}

// The tester covers the three basic steps; the very generic Secondary CPV exception is described in the text only.
export type TesterCpv = 'primary' | 'secondary' | 'none'
export type TesterInput = { title: string; organization: string; cpv: TesterCpv }
export type TesterResult = {
  verdict: 'keep' | 'drop'
  // The step at which the contract is dropped (1: no listed CPV, 2: negative keyword, 3: no positive keyword).
  droppedAt: 1 | 2 | 3 | null
  negativeHits: { words: string; field: 'title' | 'organization' }[]
  positiveHits: string[]
  requiresPositive: boolean
}

const words = (value: string) => value.split(/[^\p{L}\p{N}_]+/u).filter(Boolean)

// Original words of the title that contain a positive keyword root, for display.
function positiveWords(title: string, keyword: string): string | null {
  const parts = title.split(/\s+/).filter(Boolean)
  const root = normalizeTitle(keyword)
  // Shortest run of words first, so that only the matching words are shown.
  for (let length = 1; length <= root.split(' ').length + 1; length++) {
    for (let start = 0; start + length <= parts.length; start++) {
      const phrase = parts.slice(start, start + length).join(' ')
      if (normalizeTitle(phrase).includes(root)) return phrase.replace(/^[^\p{L}\p{N}]+|[^\p{L}\p{N}]+$/gu, '')
    }
  }
  return null
}

export function evaluateTitle({ title, organization, cpv }: TesterInput): TesterResult {
  const negativeHits: TesterResult['negativeHits'] = []
  for (const [text, field] of [[title, 'title'], [organization, 'organization']] as const) {
    const tokens = negativeTokens(text)
    const original = words(text)
    for (const [keyword, gap] of Object.entries(NEGATIVE_KEYWORDS)) {
      const match = matchPrefix(tokens, negativeTokens(keyword), gap)
      const shown = match && match.map(index => original[index] ?? tokens[index]).join(' … ')
      if (shown && !negativeHits.some(hit => hit.words === shown && hit.field === field)) negativeHits.push({ words: shown, field })
    }
  }
  const requiresPositive = cpv === 'secondary'
  const normalized = normalizeTitle(title)
  const positiveHits = [...new Set(POSITIVE_KEYWORDS
    .filter(keyword => normalized.includes(normalizeTitle(keyword)))
    .map(keyword => positiveWords(title, keyword) ?? keyword))]
  const droppedAt = cpv === 'none' ? 1 : negativeHits.length ? 2 : requiresPositive && !positiveHits.length ? 3 : null
  return { verdict: droppedAt ? 'drop' : 'keep', droppedAt, negativeHits, positiveHits, requiresPositive }
}

export const TESTER_PRESETS: { label: string; title: string; cpv: TesterCpv }[] = [
  { label: 'Αποψίλωση οικοπέδων', title: 'Προληπτικός καθαρισμός βλάστησης οικοπέδων', cpv: 'primary' },
  { label: 'Κατάσβεση', title: 'Εργασίες συνδρομής στην κατάσβεση πυρκαγιάς', cpv: 'secondary' },
  { label: 'Επισκευή δρόμου', title: 'Αποκατάσταση βατότητας δρόμου', cpv: 'secondary' },
  { label: 'Γήπεδο', title: 'Προμήθεια πυροσβεστικού εξοπλισμού για το δημοτικό γήπεδο', cpv: 'primary' },
  { label: 'Σχολικά κτίρια', title: 'Συντήρηση συστημάτων πυροπροστασίας σχολικών κτιρίων', cpv: 'secondary' },
]

export const CODE_EFFIS = `import requests

URL = "https://api.effis.emergency.copernicus.eu/rest/2/burntareas/current/"
params = {
    "country": "EL",                  # κωδικός χώρας: EL, IT, ES, PT, FR, HR, CY ...
    "firedate__gte": "2025-01-01",
    "firedate__lte": "2025-12-31",
    "ordering": "-id",
    "limit": 100,                     # εγγραφές ανά σελίδα
}

rows, url = [], URL
while url:
    r = requests.get(url, params=params if url == URL else None, timeout=60)
    r.raise_for_status()
    data = r.json()
    rows.extend(data["results"])
    url = data["next"]                # επόμενη σελίδα ή None
    if url:
        url = url.replace("http://", "https://", 1)

print(len(rows), "πυρκαγιές")
print(rows[0]["firedate"], rows[0]["area_ha"], rows[0]["commune"])`

export const CODE_FIRMS = `import io
import requests
import pandas as pd

MAP_KEY = "ΤΟ_ΚΛΕΙΔΙ_ΣΑΣ"               # δωρεάν: firms.modaps.eosdis.nasa.gov/api/map_key
SOURCE = "VIIRS_SNPP_NRT"              # ή VIIRS_NOAA20_NRT, VIIRS_NOAA21_NRT, MODIS_NRT
AREA = "19,34,30,42"                   # δυτικά, νότια, ανατολικά, βόρεια (εδώ: Ελλάδα)
DAYS = 1                               # τελευταίες 1 έως 10 ημέρες

url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{MAP_KEY}/{SOURCE}/{AREA}/{DAYS}"
r = requests.get(url, timeout=60)
r.raise_for_status()
df = pd.read_csv(io.StringIO(r.text))

print(df[["latitude", "longitude", "acq_date", "acq_time", "confidence", "frp"]].head())`

export const CODE_JOIN = `import geopandas as gpd
from shapely.geometry import shape

# rows: τα αποτελέσματα του EFFIS από τον πρώτο κώδικα
fires = gpd.GeoDataFrame(
    rows,
    geometry=[shape(r["shape"]) if r.get("shape") else shape(r["centroid"]) for r in rows],
    crs="EPSG:4326",
)
munis = gpd.read_file("municipalities.geojson")   # όρια δήμων της χώρας σας

polys = fires[fires.geom_type.isin(["Polygon", "MultiPolygon"])]
parts = gpd.overlay(polys.to_crs(3035), munis.to_crs(3035), how="intersection")
parts["overlap_ha"] = parts.area / 10_000         # καμένα εκτάρια μέσα σε κάθε δήμο`
