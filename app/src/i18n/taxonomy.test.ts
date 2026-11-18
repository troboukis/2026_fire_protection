import { describe, expect, it } from 'vitest'
import { taxonomyUi } from './taxonomy'
import cpvCodes from './cpvCodes.json'
import { buildCpvFilterIndex, matchesContractCpv } from '../lib/cpvFilter'

const en = taxonomyUi('en')
const el = taxonomyUi('el')

describe('controlled source categories', () => {
  it('keeps commitments, approvals, awards and payments distinct', () => {
    expect(en.label('ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ')).toBe('Budget commitment')
    expect(en.label('ΕΓΚΡΙΣΗ ΔΑΠΑΝΗΣ')).toBe('Expenditure approval')
    expect(en.label('ΚΑΤΑΚΥΡΩΣΗ')).toBe('Contract award')
    expect(en.label('ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ ΠΛΗΡΩΜΗΣ')).toBe('Payment finalisation')
    expect(en.label('Βάσει βέλτιστης σχέσης ποιότητας – κόστους')).toBe('Best quality–cost ratio')
    expect(en.label('Βάσει βέλτιστης σχέσης ποιότητας – τιμής')).toBe('Best price–quality ratio')
  })

  it('corrects ambiguous machine translations in their taxonomy context', () => {
    expect(en.label('ΕΝΕΡΓΕΙΑ')).toBe('Energy')
    expect(en.label('ΜΕΤΑΦΟΡΕΣ')).toBe('Transport')
    expect(en.label('ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ')).toBe('Association of Municipalities')
    expect(en.label('περ. α) γγ)  παρ. 2 άρθρο  26')).toBe('Article 26(2), point α), subpoint γγ)')
  })

  it('changes known organization prefixes while preserving proper names and source values', () => {
    const source = 'ΔΗΜΟΣ ΜΑΡΑΘΩΝΟΣ'
    expect(en.organization(source)).toBe('Municipality of Marathon')
    expect(el.organization(source)).toBe(source)
    expect(en.organization('ΑΛΛΟΣ ΦΟΡΕΑΣ · Δοκιμή')).toBe('Other body · Δοκιμή')
    expect(en.organization('ΔΗΜΟΤΙΚΟ ΒΡΕΦΟΚΟΜΕΙΟ · Δοκιμή')).toBe('Municipal nursery · Δοκιμή')
    expect(en.organization('Υπουργείο Περιβάλλοντος και Ενέργειας')).toBe('Ministry of Environment and Energy')
    expect(en.organization('Άγνωστος νόμιμος φορέας')).toBe('Άγνωστος νόμιμος φορέας')
  })

  it('parses source thematic lists without splitting commas within a category', () => {
    const source = "['ΠΕΡΙΒΑΛΛΟΝ', 'ΓΕΩΡΓΙΑ, ΔΑΣΟΚΟΜΙΑ ΚΑΙ ΑΛΙΕΙΑ']"
    expect(en.thematic(source)).toBe('Environment · Agriculture, forestry and fisheries')
    expect(en.thematic('["ΕΝΕΡΓΕΙΑ", "ΜΕΤΑΦΟΡΕΣ"]')).toBe('Energy · Transport')
    expect(el.thematic(source)).toBe(source)
    expect(en.thematic("[unknown('ΠΕΡΙΒΑΛΛΟΝ')]" )).toBe("[unknown('ΠΕΡΙΒΑΛΛΟΝ')]")
  })
})

describe('CPV presentation and filtering', () => {
  it('uses official code labels and preserves shortened label-only meanings', () => {
    expect(en.cpv('35110000-8', 'Εξοπλισμός πυρόσβεσης')).toBe('Firefighting, rescue and safety equipment')
    expect(en.description('Εξοπλισμός πυρόσβεσης')).toBe('Fire-fighting equipment')
    expect(en.description('Εργαλεία')).toBe('Tools')
    expect(en.description('Επισκευή')).toBe('Repair')
    expect(en.description('Αξίνες')).toBe('Picks')
    expect(en.description('Εργαλεία | Εξοπλισμός πυρόσβεσης')).toBe('Tools | Fire-fighting equipment')
    expect(Object.keys(cpvCodes)).toHaveLength(666)
    for (const [code, text] of Object.entries(cpvCodes)) {
      expect(code).toMatch(/^\d{8}-\d$/)
      expect(text).not.toMatch(/[Α-Ωα-ω]/)
    }
  })

  it('retains codes, membership, original descriptions and Greek input exactly', () => {
    const rows = [{ cpv_key: '35110000-8', cpv_value: 'Εξοπλισμός πυρόσβεσης', procurement_id: 42 }]
    const before = structuredClone(rows)
    const index = buildCpvFilterIndex(rows)
    expect(en.cpv(rows[0].cpv_key, rows[0].cpv_value)).toContain('Firefighting')
    expect(matchesContractCpv(42, '35110000-8', index)).toBe(true)
    expect(matchesContractCpv(43, '35110000-8', index)).toBe(false)
    expect(index.get('35110000-8')?.label).toBe('35110000-8 · Εξοπλισμός πυρόσβεσης')
    expect(rows).toEqual(before)
    const original = '  ΕΞΟΠΛΙΣΜΟΣ ΠΥΡΟΣΒΕΣΗΣ  '
    expect(el.description(original)).toBe(original)
    expect(el.cpv('35110000-8', original)).toBe(original)
    expect(el.label('  ΜΕΤΑΦΟΡΕΣ  ')).toBe('  ΜΕΤΑΦΟΡΕΣ  ')
  })

  it('preserves unknown future categories and arbitrary record content', () => {
    const text = 'Άγνωστη περιγραφή: σύμβαση πυροπροστασίας 2027'
    expect(en.description(text)).toBe(text)
    expect(en.cpv('99999999-9', text)).toBe(text)
    expect(en.cpv('toString', text)).toBe(text)
    expect(en.label(text)).toBe(text)
  })
})
