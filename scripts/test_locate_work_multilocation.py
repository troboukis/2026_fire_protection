import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.locate_work import Document


def document():
    instance = object.__new__(Document)
    instance.debug = False
    return instance


def finding(point, page=1, work="καθαρισμός", **extra):
    return {
        "point_name_raw": point,
        "point_name_canonical": point,
        "work": work,
        "page": page,
        "excerpt": f"{work} στην περιοχή {point}",
        **extra,
    }


def test_same_work_in_separate_locations_survives_geocoding():
    doc = document()
    doc.data = doc._deduplicate_findings_pre_geocode([
        finding("Ωρωπός, Αττική"),
        finding("Μαραθώνας, Αττική"),
        finding("Πόρος, Αττική"),
    ])
    queries = []

    def geocode(query):
        queries.append(query)
        return {"lat": 37.0 + len(queries), "lon": 23.0,
                "formatted_address": query, "place_id": str(len(queries))}

    doc.geolocatePoint = geocode
    rows = doc.geolocateWork()

    assert len(rows) == 3
    assert queries == [
        "Ωρωπός, Αττική, Ελλάδα",
        "Μαραθώνας, Αττική, Ελλάδα",
        "Πόρος, Αττική, Ελλάδα",
    ]


def test_repeated_location_retains_all_evidence_pages_after_geocoding():
    doc = document()
    doc.data = doc._deduplicate_findings_pre_geocode([
        finding("Ωρωπός, Αττική", 2),
        finding("Ωρωπός, Αττική", 7, "κλάδεμα"),
    ])
    doc.geolocatePoint = lambda query: {
        "lat": 38.3, "lon": 23.7, "formatted_address": query, "place_id": "oropos",
    }

    rows = doc.geolocateWork()

    assert len(rows) == 1
    assert rows[0]["pages"] == [2, 7]
    assert rows[0]["work"] == "καθαρισμός, κλάδεμα"


def test_different_locations_with_same_google_coordinates_stay_separate():
    rows = document()._deduplicate_findings_by_coords([
        finding("Συστάδα 8α, Βίτσι", lat=40.649167, lon=21.385556),
        finding("Συστάδα 8β, Βίτσι", lat=40.649167, lon=21.385556),
    ])

    assert len(rows) == 2


def test_full_document_including_locations_after_page_ten(monkeypatch):
    pages = [SimpleNamespace(extract_text=lambda: "Διοικητικό κείμενο " * 12) for _ in range(10)]
    pages.append(SimpleNamespace(extract_text=lambda: "Εργασίες στη δεύτερη περιοχή: Πόρος. " * 8))
    monkeypatch.setattr("natural_pdf.PDF", lambda data: SimpleNamespace(pages=pages))
    doc = document()
    doc.doc = b"fake pdf"

    assert "--- PAGE 11 ---" in doc.readDocument()
    assert "Πόρος" in doc.file


def test_ocr_scanned_page_with_only_kimdis_header(monkeypatch):
    page = SimpleNamespace(extract_text=lambda: "26SYMV019348528 2026-06-22")
    monkeypatch.setattr("natural_pdf.PDF", lambda data: SimpleNamespace(pages=[page]))
    monkeypatch.setattr("pdf2image.convert_from_bytes", lambda *args, **kwargs: ["page image"])
    monkeypatch.setattr("pytesseract.image_to_string", lambda image, **kwargs: "Εργασίες στις περιοχές Κατερίνης και Δίου.")
    doc = document()
    doc.doc = b"fake pdf"

    assert "Κατερίνης και Δίου" in doc.readDocument()
