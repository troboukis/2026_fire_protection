"""Offline regression tests for the single KIMDIS/Diavgeia keyword policy."""

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.keyword_exclusions import (
    DEFAULT_EXCLUDE_KEYWORDS,
    matching_exclusion_keywords,
    normalize_keyword_tokens,
    text_has_exclusion,
    text_matches_keyword_prefix,
)


@pytest.mark.parametrize("title", [
    "Πυροπροστασία σχολείου", "ΣΧΟΛΙΚΩΝ ΚΤΙΡΙΩΝ", "Καθαρισμός σχολ. μονάδων",
    "πανεπιστημιακές εγκαταστάσεις", "γηπέδου",
    "πιστοποιητικών ενεργητικής πυρασφάλειας", "ΚΗΠΟΤΕΧΝΙΚΗΣ ΣΥΝΤΗΡΗΣΗΣ",
    "Συντήρηση δημοτικών κήπων", "ΟΑΚΑ", "Παραγωγή φυτών", "Φυτωριακές εργασίες",
    "δασικού φυτωρίου", "ΚΑΥΣΙΜΩΝ ΘΕΡΜΑΝΣΗΣ", "κολυμβητηρίου", "Server Room",
    "Προσχολικής αγωγής", "ΔΙΑΣΧΟΛΙΚΩΝ ΑΓΩΝΩΝ", "Γυμνασίου", "Λυκείου",
    "ΝΟΣΟΚΟΜΕΙΟ", "ΠΑΝΤΕΙΟ", "Επιθεώρηση Εργασίας", "Αναγόμωση πυροσβεστήρων",
    "αντλητικά συγκροτήματα", "ΕΠΑΛ", "βρεφονηπιακού", "παιδικού σταθμού",
    "Συντήρηση χώρων πρασίνου", "SPORTS4ALL", "κινηματογράφου",
    "AQUA MONTIS", "ΑΝΑΔΕΙΞΗ ΥΦΙΣΤΑΜΕΝΩΝ ΔΙΑΔΡΟΜΩΝ",
    "ΑΝΑΔΕΙΞΗ ΥΦΟΣΤΑΜΕΝΩΝ ΔΙΑΔΡΟΜΩΝ",
])
def test_preexisting_exclusions_are_retained(title):
    assert text_has_exclusion(title)
    assert text_has_exclusion(title.upper())
    assert text_has_exclusion(title.lower())


@pytest.mark.parametrize("title", [
    "Πυροπροστασία γυμνασίου", "Λυκειακές εγκαταστάσεις", "Νηπιαγωγείου",
    "Δημοτικό σχολείο", "Θεατρικής σκηνής", "Πανεπιστημιακού χώρου",
    "Συντήρηση πυροσβεστήρων πολιτικής προστασίας", "Προμήθεια επινώτιων πυροσβεστήρων",
    "ΓΝΘ", "Γ.Ν.Θ.", "Γ Ν Θ", "Ανακαίνισης κτιρίου", "Νοσοκομειακού εξοπλισμού",
    "Αλατοδιανομέων", "Εκχιονισμού οδών",
])
def test_requested_exclusions_cover_inflections_case_and_accents(title):
    assert text_has_exclusion(title)
    assert text_has_exclusion(title.upper())
    assert text_has_exclusion(title.lower())
    assert text_has_exclusion(" ".join(normalize_keyword_tokens(title)))


@pytest.mark.parametrize("title", [
    "Σύστημα πυρανίχνευσης δάσους", "Εγκατάσταση συστήματος πυρανίχνευσης σε δάσος",
    "Ενεργητική πυροπροστασία δασών", "Μελέτη πυροπροστασίας δασικού χώρου",
    "Προμήθεια πυροσβεστικών οχημάτων", "Προμήθεια πυροσβεστικού εξοπλισμού",
    "Συντήρηση αντιπυρικών ζωνών", "Καύσιμα πυροσβεστικών οχημάτων",
    "Καθαρισμός και απομάκρυνση ξερών φυτών", "Δασικές εργασίες",
    "Συντήρηση κτιρίου", "Προμήθεια πυράντοχων θυρών", "Φωτισμός ασφαλείας",
    "Προστασία λιμένα", "ανασχολικός", None, float("nan"), "",
    "Αναδάσωση", "ΑΝΑΔΑΣΩΣΕΩΝ", "αναδασώσεων",
])
def test_no_removed_building_heuristic_or_outdoor_override(title):
    assert not text_has_exclusion(title)


def test_original_exclusion_still_applies_to_forest_context():
    assert text_has_exclusion("Πυροπροστασία δάσους δίπλα σε σχολείο")
    assert text_has_exclusion("Αναγόμωση πυροσβεστήρων δασοπροστασίας")


def test_normalization_handles_accents_case_punctuation_and_final_sigma():
    assert normalize_keyword_tokens("ΕΠΙΘΕΏΡΗΣΗ — Εργασίας") == ["επιθεωρηση", "εργασιασ"]
    assert text_matches_keyword_prefix("Πιστοποιητικών ενεργητικής πυρασφαλείας", "πιστοποιητικ πυρασφαλ")


def test_matching_requires_prefixes_order_and_distinct_tokens():
    assert not text_matches_keyword_prefix("ανασχολικός", "σχολ")
    assert not text_matches_keyword_prefix("κήπων συντήρηση", "συντηρησ κηπ")
    assert not text_matches_keyword_prefix("συντήρηση", "συντηρησ συντηρησ")
    assert text_matches_keyword_prefix("συντήρηση και συντήρηση", "συντηρησ συντηρησ")


def test_original_root_phrases_retain_the_maximum_two_word_gap():
    assert text_matches_keyword_prefix("Συντήρηση κήπων", "συντηρησ κηπ")
    assert text_matches_keyword_prefix("Συντήρηση των δημοτικών κήπων", "συντηρησ κηπ")
    assert not text_matches_keyword_prefix("Συντήρηση όλων των δημοτικών κήπων", "συντηρησ κηπ")


@pytest.mark.parametrize("phrase, with_gap", [
    ("Επιθεώρηση Εργασίας", "Επιθεώρηση της Εργασίας"),
    ("Συντήρηση χώρων πρασίνου", "Συντήρηση κοινόχρηστων χώρων πρασίνου"),
    ("Συντήρηση χώρων πρασίνου", "Συντήρηση χώρων αστικού πρασίνου"),
    ("Aqua Montis", "Aqua του Montis"),
    ("Ανάδειξη υφιστάμενων διαδρομών", "Ανάδειξη των υφιστάμενων διαδρομών"),
    ("Ανάδειξη υφιστάμενων διαδρομών", "Ανάδειξη υφιστάμενων δασικών διαδρομών"),
    ("Ανάδειξη υφοστάμενων διαδρομών", "Ανάδειξη των υφοστάμενων διαδρομών"),
])
def test_former_kimdis_phrases_require_adjacent_words(phrase, with_gap):
    assert text_matches_keyword_prefix(phrase, phrase)
    assert text_has_exclusion(phrase)
    assert not text_matches_keyword_prefix(with_gap, phrase)
    assert not text_has_exclusion(with_gap)


def test_adjacent_matching_still_ignores_punctuation_case_and_accents():
    assert text_has_exclusion("ΣΥΝΤΉΡΗΣΗ — ΧΏΡΩΝ, ΠΡΑΣΊΝΟΥ")
    assert not text_has_exclusion("Συντήρηση δημοτικών χώρων αστικού πρασίνου")


def test_hospital_acronym_requires_adjacent_letters():
    assert text_has_exclusion("Πληρωμή Γ.Ν.Θ. Αγίου Παύλου")
    assert not text_has_exclusion("Γ συνέχεια Ν Θ")
    assert not text_has_exclusion("ΑΓΝΘ")


def test_phrase_matching_explores_all_possible_intermediate_positions():
    assert text_matches_keyword_prefix("α β β γ δ", "α β δ")


def test_phrase_cannot_span_title_organization_and_description():
    assert not text_has_exclusion("Συντήρηση", "κήπων")
    assert not text_has_exclusion("Επιθεώρηση", None, "Εργασίας")
    assert text_has_exclusion("Αντιπυρικές ζώνες", "ΔΗΜΟΣ", "Συντήρηση δημοτικών κήπων")


def test_matches_report_each_term_once_in_list_order():
    assert matching_exclusion_keywords("Σχολείου", "Σχολείου") == ("σχολει", "σχολ")
    assert matching_exclusion_keywords(None, float("nan"), "") == ()


def test_common_policy_has_no_normalized_duplicates():
    assert isinstance(DEFAULT_EXCLUDE_KEYWORDS, dict)
    normalized = [tuple(normalize_keyword_tokens(term)) for term in DEFAULT_EXCLUDE_KEYWORDS]
    assert len(normalized) == len(set(normalized))
    assert all(term == " ".join(normalize_keyword_tokens(term)) for term in DEFAULT_EXCLUDE_KEYWORDS)


def test_antinero_kavala_description_with_reforested_areas_is_retained():
    title = "ΣΥΜΒΑΣΗ ΓΙΑ ΤΗΝ ΕΚΤΕΛΕΣΗ ΤΟΥ ΕΡΓΟΥ ANTINERO III ΔΧ ΚΑΒΑΛΑΣ"
    description = (
        "Έργα αντιπυρικής προστασίας σε δημόσιες δασικές εκτάσεις, δασικά συμπλέγματα "
        "και περιοχές αναδασώσεων, αρμοδιότητας του Δασαρχείου Καβάλας, "
        "σύμφωνα με τις διατάξεις του αρ. 16 παρ. 5 του ν. 998/1979."
    )
    assert not text_has_exclusion(title, description, "ΥΠΟΥΡΓΕΙΟ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΚΑΙ ΕΝΕΡΓΕΙΑΣ")


@pytest.mark.parametrize("term", DEFAULT_EXCLUDE_KEYWORDS)
def test_every_configured_term_has_an_auditable_match(term):
    assert term in matching_exclusion_keywords(term)
