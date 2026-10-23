import pandas as pd
import pytest

from src.keyword_exclusions import text_has_exclusion
from src.fetch_diavgeia import filter_excluded_org_rows
from ingest.stage2_load_erd import diav_rows


@pytest.mark.parametrize('title', [
    'Πυροπροστασία σχολείου', 'ΣΧΟΛΙΚΩΝ ΚΤΙΡΙΩΝ', 'πανεπιστημιακές εγκαταστάσεις',
    'γηπέδου', 'πιστοποιητικών ενεργητικής πυρασφάλειας',
    'ΚΗΠΟΤΕΧΝΙΚΗΣ ΣΥΝΤΗΡΗΣΗΣ', 'Συντήρηση δημοτικών κήπων', 'ΟΑΚΑ',
    'Παραγωγή φυτών', 'Φυτωριακές εργασίες', 'δασικού φυτωρίου',
    'ΚΑΥΣΙΜΩΝ ΘΕΡΜΑΝΣΗΣ', 'κολυμβητηρίου', 'Server Room',
    'Προσχολικής αγωγής', 'ΔΙΑΣΧΟΛΙΚΩΝ ΑΓΩΝΩΝ', 'Γυμνασίου', 'Λυκείου',
    'Πυροπροστασία νηπιαγωγείου', 'ΔΗΜΟΤΙΚΟΥ ΣΧΟΛΕΙΟΥ', 'Θεατρικής σκηνής',
    'Προμήθεια πυροσβεστήρων', 'ΓΝΘ', 'Γ.Ν.Θ.', 'Ανακαίνιση κτιρίου',
    'Νοσοκομειακού εξοπλισμού', 'Αλατοδιανομέων', 'Εκχιονισμού οδών',
])
def test_requested_roots_cover_inflections_and_case(title):
    for variant in (title, title.upper(), title.lower()):
        assert text_has_exclusion(variant)


@pytest.mark.parametrize('title', [
    'Συντήρηση αντιπυρικών ζωνών', 'Καύσιμα πυροσβεστικών οχημάτων',
    'Δασικές εργασίες', 'Καθαρισμός και απομάκρυνση ξερών φυτών',
    'Συντήρηση δασικού οδικού δικτύου και κήπων', 'server εξοπλισμός',
    'ανασχολικός', None, float('nan'),
])
def test_unrelated_titles_are_retained(title):
    assert not text_has_exclusion(title)


def test_persisted_diavgeia_csv_excludes_subject_without_org_column():
    df = pd.DataFrame({'subject': ['Συντήρηση κήπων', 'Αντιπυρικές ζώνες']})
    assert filter_excluded_org_rows(df)['subject'].tolist() == ['Αντιπυρικές ζώνες']


def test_stage2_does_not_reinsert_excluded_diavgeia():
    df = pd.DataFrame({'subject': ['ΚΑΥΣΙΜΩΝ ΘΕΡΜΑΝΣΗΣ']})
    assert diav_rows(df, {}, {}, {}, {}, {}) == []


@pytest.mark.parametrize('subject, organization, excluded', [
    ('Επανέκδοση πιστοποιητικών πυροπροστασίας των παιδικών σταθμών Παραμυθιάς και Χόικας.', 'ΣΟΥΛΙΟΥ', True),
    ('1ος ΛΟΓΑΡΙΑΣΜΟΣ ΕΡΓΟΥ ΓΙΑ ΕΚΠΟΝΗΣΗ ΜΕΛΕΤΩΝ ΠΑΘΗΤΙΚΗΣ & ΕΝΕΡΓΗΤΙΚΗΣ ΠΥΡΟΠΡΟΣΤΑΣΙΑΣ ΚΤΙΡΙΩΝ ΤΟΥ ΔΗΜΟΥ ΚΗΦΙΣΙΑΣ', 'ΚΗΦΙΣΙΑΣ', False),
    ('1ος λογ/σμός μελέτης για υλοποίηση μέτρων & μέσων πυροπροστασίας σχολ. μονάδων Δ. Χερσονήσου', 'ΧΕΡΣΟΝΗΣΟΥ', True),
    ('ΧΕΠ ΕΚΔΟΣΗ ΧΡΗΜΑΤΙΚΟΥ ΕΝΤΑΛΜΑΤΟΣ 26/.02232', 'Π.Γ.Ν. «ΑΤΤΙΚΟΝ» - Γ.Ν.Δ.Α. «Η ΑΓΙΑ ΒΑΡΒΑΡΑ»', False),
    ('Επιθεώρηση - έλεγχος, συντήρηση και αναγόμωση πυροσβεστήρων', 'ΣΙΦΝΟΥ', True),
    ('ΣΥΜΒΑΣΗ ΠΡΟΜΗΘΕΙΑΣ ΠΥΡΟΣΒΕΣΤΗΡΩΝ ΓΙΑ ΤΑ ΚΤΙΡΙΑ ΤΟΥ ΔΗΜΟΥ ΠΥΡΓΟΥ', 'ΔΗΜΟΣ ΠΥΡΓΟΥ', True),
])
def test_building_examples_use_only_the_preserved_keyword_list(subject, organization, excluded):
    assert text_has_exclusion(subject, organization) is excluded


@pytest.mark.parametrize('subject, organization', [
    ('Καθαρισμός οικοπέδων και αποψίλωση για την πυροπροστασία', 'ΧΑΛΑΝΔΡΙΟΥ'),
    ('ΕΚΤΕΛΕΣΗ ΕΡΓΑΣΙΩΝ ΚΑΘΑΡΙΣΜΟΥ ΠΕΡΙΜΕΤΡΙΚΑ ΤΟΥ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΧΩΡΟΥ ΤΟΥ ΚΤΙΡΙΟΥ', 'ΚΕΝΤΡΟ ΚΟΙΝΩΝΙΚΗΣ ΠΡΟΝΟΙΑΣ ΠΕΡΙΦΕΡΕΙΑΣ ΑΤΤΙΚΗΣ'),
    ('Προμήθεια πυροσβεστικών οχημάτων για την πολιτική προστασία', 'ΔΗΜΟΣ Χ'),
    ('Μελέτες πυροπροστασίας του αρχαιολογικού χώρου Ραμνούντος', 'ΥΠΟΥΡΓΕΙΟ ΠΟΛΙΤΙΣΜΟΥ'),
    ('Μίσθωση υδροφόρων οχημάτων αντιπυρικής περιόδου 2026', 'ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ'),
    ('Μελέτη, Προμήθεια, Εγκατάσταση Συστημάτων Online Monitoring (OLMS) σε Αυτομετασχηματιστές', 'ΑΔΜΗΕ ΑΕ'),
    (None, None),
    (float('nan'), float('nan')),
])
def test_forest_and_outdoor_work_is_retained(subject, organization):
    assert not text_has_exclusion(subject, organization)


def test_persisted_diavgeia_csv_keeps_childcare_keyword_exclusion():
    df = pd.DataFrame({
        'subject': ['Επανέκδοση πιστοποιητικών πυροπροστασίας των παιδικών σταθμών', 'Αντιπυρικές ζώνες'],
        'org_name_clean': ['ΣΟΥΛΙΟΥ', 'ΣΟΥΛΙΟΥ'],
    })
    assert filter_excluded_org_rows(df)['subject'].tolist() == ['Αντιπυρικές ζώνες']


def test_stage2_excludes_fire_extinguishers_under_the_requested_keyword_policy():
    df = pd.DataFrame({'subject': ['Συντήρηση πυροσβεστήρων δημοτικών κτιρίων'], 'org_name_clean': ['ΚΟΡΥΔΑΛΛΟΥ']})
    assert diav_rows(df, {}, {}, {}, {}, {}) == []
