"""Hospital issuer exclusions must survive every supported source/ingest path."""

from datetime import date

import pandas as pd
import pytest

from ingest import stage2_load_erd as loader
from src import fetch_diavgeia as diavgeia
from src import fetch_kimdis_procurements as kimdis
from src import filter_relevance
from src.organization_exclusions import (
    is_hospital_organization,
    record_has_hospital_organization,
)


HOSPITAL_NAMES = (
    "Γενικό Νοσοκομείο Λάρισας",
    "Προμήθειες Νοσοκομείου Χανίων",
    "Διοίκηση Νοσοκομείων",
    "Νοσοκομεία Λασιθίου",
    "ΠΕΡ.ΓΕΝ. ΝΟΣΟΚΟΜΕΙO ΑΘΗΝΩΝ 'ΙΠΠΟΚΡΑΤΕΙΟ'",  # Latin O.
    "Π.Γ.Ν. «ΑΤΤΙΚΟΝ» - Γ.Ν.Δ.Α. «Η ΑΓΙΑ ΒΑΡΒΑΡΑ»",
    "ΓΝ ΑΓ.ΝΙΚΟΛΑΟΥ-ΓΝ-ΚΥ ΙΕΡΑΠΕΤΡΑΣ",
    "Γ.Ν. ΛΑΣΙΘΙΟΥ",
    "Γ Ν ΛΑΣΙΘΙΟΥ",
    "Γ. ΟΓΚΟΛΟΓΙΚΟ ΝΟΣ. ΚΗΦΙΣΙΑΣ «ΟΙ ΑΓΙΟΙ ΑΝΑΡΓΥΡΟΙ»",
    "ΠΕΡΙΦ. ΠΑΝΕΠΙΣΤ. ΓΕΝ. ΝΟΣΟΚ. ΛΑΡΙΣΑΣ",
    "Γ. ΠΑΝΑΡΚΑΔΙΚΟ ΝΟΣ. ΤΡΙΠΟΛΗΣ «Η ΕΥΑΓΓΕΛΙΣΤΡΙΑ»",
    "ΠΡΩΤΟ ΓΕΝ. ΝΟΣ. ΘΕΣ/ΝΙΚΗΣ «ΑΓΙΟΣ ΠΑΥΛΟΣ»",
    "ΠΑΘΟΛΟΓΙΚΟ ΝΟΣ. ΑΘΗΝΩΝ ΣΠΗΛΙΟΠΟΥΛΕΙΟ «Η ΑΓΙΑ ΕΛΕΝΗ»",
    "ΝΟΣΗΛΕΥΤΙΚΟ ΙΔΡΥΜΑ ΜΕΤΟΧΙΚΟΥ ΤΑΜΕΙΟΥ ΣΤΡΑΤΟΥ",
    "ΕΘΝΙΚΟ ΚΕΝΤΡΟ ΑΠΟΚΑΤΑΣΤΑΣΗΣ",
)


@pytest.mark.parametrize("name", HOSPITAL_NAMES)
def test_hospital_names_cover_observed_labels_and_normalization(name):
    for variant in (name, name.lower(), name.upper()):
        assert is_hospital_organization(variant)


@pytest.mark.parametrize("name", [
    "ΚΕΝΤΡΟ ΤΕΚΜΗΡΙΩΣΗΣ ΚΑΙ ΚΟΣΤΟΛΟΓΗΣΗΣ ΝΟΣΟΚΟΜΕΙΑΚΩΝ ΥΠΗΡΕΣΙΩΝ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ",
    "ΥΠΟΥΡΓΕΙΟ ΥΓΕΙΑΣ",
    "ΔΙΟΙΚΗΣΗ 1ΗΣ ΥΓΕΙΟΝΟΜΙΚΗΣ ΠΕΡΙΦΕΡΕΙΑΣ",
    "ΔΗΜΟΣ ΛΑΡΙΣΑΙΩΝ",
    "ΚΕΝΤΡΟ ΥΓΕΙΑΣ Χ",
    "ΕΠΙΤΡΟΠΗ ΕΠΟΠΤΕΙΑΣ ΚΑΙ ΕΛΕΓΧΟΥ ΠΑΙΓΝΙΩΝ",
    "ΕΡΕΥΝΗΤΙΚΟ ΚΕΝΤΡΟ ΤΗΣ ΓΝΩΣΗΣ",
    None,
    float("nan"),
    pd.NA,
])
def test_other_health_entities_and_missing_names_are_retained(name):
    assert not is_hospital_organization(name)


@pytest.mark.parametrize("fields", [
    {"organization": {"label": "Γ.Ν. ΛΑΣΙΘΙΟΥ", "uid": "123"}},
    {"organization": {"value": "Γ.Ν. ΛΑΣΙΘΙΟΥ", "key": "123"}},
    {"organization": '{"label": "Γ.Ν. ΛΑΣΙΘΙΟΥ", "other": null}'},
    {"organization": "{'label': 'Γ.Ν. ΛΑΣΙΘΙΟΥ', 'uid': '123'}"},
    {"organization_value": "Γ.Ν. ΛΑΣΙΘΙΟΥ"},
    {"organization_normalized_value": "Γ.Ν. ΛΑΣΙΘΙΟΥ"},
    {"org": "Γ.Ν. ΛΑΣΙΘΙΟΥ"},
    {"org_name_clean": "ΛΑΣΙΘΙΟΥ", "org_type": "ΝΟΣΟΚΟΜΕΙΟ"},
])
def test_record_fields_cover_api_persisted_and_type_only_issuers(fields):
    assert record_has_hospital_organization(fields)


def test_hospital_mentions_in_subject_or_title_do_not_identify_the_issuer():
    assert not record_has_hospital_organization({
        "organization": {"label": "ΔΗΜΟΣ ΛΑΡΙΣΑΙΩΝ"},
        "title": "Καθαρισμός αντιπυρικής ζώνης δίπλα σε νοσοκομείο",
        "subject": "Πυροπροστασία περιοχής νοσοκομείου",
        "shortDescriptions": "Δρόμος πρόσβασης στο Γ.Ν. Λάρισας",
    })


@pytest.mark.parametrize("name", HOSPITAL_NAMES)
def test_collectors_and_loader_block_the_same_hospital_issuers(monkeypatch, name):
    def forbidden(**kwargs):
        pytest.fail("Hospital rows must be rejected before resolving database context")

    monkeypatch.setattr(loader, "resolve_procurement_context", forbidden)
    monkeypatch.setattr(loader, "resolve_diavgeia_context", forbidden)
    collector = kimdis.ProcurementCollector(
        cpvs={}, start_date=date(2026, 1, 1), end_date=date(2026, 1, 2), max_window_days=1
    )
    # Organization exclusions also hold if a custom keyword list or title override
    # would otherwise admit the contract.
    collector.exclude_keywords = ()
    monkeypatch.setattr(kimdis, "matches_reviewed_title_override", lambda *args: True)
    title = "ΣΥΜΦΩΝΗΤΙΚΟ"
    item = {
        "title": title,
        "organization": {"value": name},
        "objectDetailsList": [{
            "shortDescription": "Εξοπλισμός",
            "cpvs": [{"key": "35125300-2"}],
        }],
    }
    assert collector.is_excluded(item)
    assert not collector.is_excluded({
        **item, "organization": {"value": "ΔΗΜΟΣ ΛΑΡΙΣΑΙΩΝ"},
    })
    assert diavgeia.record_has_excluded_org({"organization": {"label": name}})
    assert diavgeia.filter_excluded_org_rows(pd.DataFrame([{
        "subject": title, "org_name_clean": name,
    }])).empty
    assert loader.procurement_rows(pd.DataFrame([{
        "title": title, "organization_value": name,
    }]), {}, {}, {}, {}, {}, {}, {}) == []
    assert loader.diav_rows(pd.DataFrame([{
        "subject": title, "org_name_clean": name,
    }]), {}, {}, {}, {}, {}) == []


def test_stale_csv_hospital_type_and_raw_label_cannot_bypass_filtering():
    frame = pd.DataFrame([
        {"ada": "HOSPITAL-TYPE", "org_type": "ΝΟΣΟΚΟΜΕΙΟ", "org_name_clean": "Χ"},
        {"ada": "HOSPITAL-RAW", "organization": "{'label': 'Γ.Ν. ΛΑΣΙΘΙΟΥ'}"},
        {"ada": "MUNICIPALITY", "org_type": "ΔΗΜΟΣ", "org_name_clean": "ΛΑΡΙΣΑΙΩΝ"},
    ])
    assert diavgeia.filter_excluded_org_rows(frame)["ada"].tolist() == ["MUNICIPALITY"]
    rows = loader.diav_rows(frame, {}, {}, {}, {}, {})
    assert [row[3] for row in rows] == ["MUNICIPALITY"]


def test_pdf_relevance_cannot_readmit_a_hospital_and_keeps_other_health_entities():
    frame = pd.DataFrame([
        {"ada": "HOSPITAL", "subject": "Λοιπές προμήθειες", "organization": {"label": "Γ.Ν. ΛΑΣΙΘΙΟΥ"}},
        {"ada": "TYPE", "subject": "Λοιπές προμήθειες", "org_name_clean": "Χ", "org_type": "ΝΟΣΟΚΟΜΕΙΟ"},
        {"ada": "OTHER", "subject": "Λοιπές προμήθειες", "org_name_clean": "ΚΕΝΤΡΟ ΤΕΚΜΗΡΙΩΣΗΣ ΝΟΣΟΚΟΜΕΙΑΚΩΝ ΥΠΗΡΕΣΙΩΝ"},
    ])
    enriched, stats = filter_relevance.apply_relevance_filter(
        frame,
        {ada: "Πρόληψη δασικών πυρκαγιών" for ada in frame["ada"]},
        filter_relevance.build_keyword_specs(["δασικών πυρκαγιών"]),
    )
    assert enriched["pdf_match"].tolist() == [True, True, True]
    assert enriched["is_relevant"].tolist() == [False, False, True]
    assert stats["rows_relevant"] == 1
