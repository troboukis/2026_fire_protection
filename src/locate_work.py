
import io
import json
import os
import re
import sys
from pathlib import Path

import psycopg2
import requests
from natural_pdf import PDF
from openai import OpenAI

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.map_copernicus_to_municipalities import resolve_database_url


class Document:
    BASE_URL = "https://cerpp.eprocurement.gov.gr/khmdhs-opendata"

    def __init__(self, ref_number: str, db_path: str | None = None, debug: bool = True):
        self.ref_number = ref_number.strip().upper()
        self.db_path = db_path
        self.debug = debug

        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        if not self.openai_api_key:
            raise ValueError("Δεν βρέθηκε το OPENAI_API_KEY στο .env")

        self.google_api_key = os.getenv("GOOGLE_GEOCODING_API_KEY")
        if not self.google_api_key:
            raise ValueError("Missing GOOGLE_GEOCODING_API_KEY")

        self.doc: bytes | None = None
        self.file: str | None = None
        self.data: list[dict] | None = None

        if self.debug:
            print(f"[INIT] ref_number={self.ref_number}")
            print(f"[INIT] db_path={self.db_path}")

    def getDocument(self) -> bytes:
        url = f"{self.BASE_URL}/contract/attachment/{self.ref_number}"

        if self.debug:
            print(f"[GET DOCUMENT] URL: {url}")

        response = requests.get(url, timeout=60)

        if self.debug:
            print(f"[GET DOCUMENT] status_code={response.status_code}")
            print(f"[GET DOCUMENT] content-type={response.headers.get('Content-Type')}")
            print(f"[GET DOCUMENT] content-length={len(response.content)} bytes")

        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "")
        if "pdf" not in content_type.lower():
            print(response.text[:1000])
            raise ValueError("Το αρχείο δεν είναι PDF")

        self.doc = response.content

        if self.debug:
            print("[GET DOCUMENT] Το PDF αποθηκεύτηκε στο self.doc")

        return self.doc

    def saveLocalCopy(self, path: str | None = None) -> str:
        if self.doc is None:
            self.getDocument()

        path = path or f"{self.ref_number}.pdf"

        with open(path, "wb") as f:
            f.write(self.doc)

        if self.debug:
            print(f"[SAVE LOCAL COPY] Αποθηκεύτηκε το PDF στο: {path}")

        return path

    def inspectDocument(self):
        if self.doc is None:
            self.getDocument()
    
        pdf = PDF(io.BytesIO(self.doc))
    
        print(f"[INSPECT] total_pages={len(pdf.pages)}")
    
        for i, page in enumerate(pdf.pages[:3], start=1):
            text = page.extract_text()
            print(f"[INSPECT] page={i}, chars={len(text) if text else 0}")
    

    def readDocument(self) -> str:
        import io
        from natural_pdf import PDF
        import pytesseract
        from pdf2image import convert_from_bytes
    
        if self.doc is None:
            self.getDocument()
    
        pdf = PDF(io.BytesIO(self.doc))
        native_pages = {}
        ocr_needed = []
    
        for i, page in enumerate(pdf.pages, start=1):
            try:
                text = page.extract_text()
            except Exception:
                text = ""
    
            text = text.strip() if text else ""
    
            if text:
                native_pages[i] = text
            else:
                ocr_needed.append(i)
    
        ocr_pages = {}
    
        if ocr_needed:
            poppler_path = os.getenv("POPPLER_PATH", "").strip() or None
            images = convert_from_bytes(self.doc, dpi=300, poppler_path=poppler_path)
    
            for page_num in ocr_needed:
                img = images[page_num - 1]
                text = pytesseract.image_to_string(img, lang="ell+eng").strip()
                if text:
                    ocr_pages[page_num] = text
    
        all_pages = []
    
        total_pages = len(pdf.pages)
        for i in range(1, total_pages + 1):
            text = native_pages.get(i) or ocr_pages.get(i) or ""
            if text:
                all_pages.append(f"\n--- PAGE {i} ---\n{text}")
    
        self.file = "\n".join(all_pages).strip()
        return self.file

    def locateWork(self) -> list[dict]:
        if self.file is None:
            if self.debug:
                print("[LOCATE WORK] self.file is None, καλώ readDocument()")
            self.readDocument()

        client = OpenAI(api_key=self.openai_api_key)
        chunks = self._build_page_aware_chunks(self.file, max_chars=18000)

        if self.debug:
            print(f"[LOCATE WORK] total_chunks={len(chunks)}")

        findings = []

        for idx, chunk in enumerate(chunks, start=1):
            if self.debug:
                print(f"[LOCATE WORK] processing chunk={idx}, pages={chunk['pages']}, chars={len(chunk['text'])}")

            chunk_results = self._extract_fireprotection_points(
                client=client,
                chunk_text=chunk["text"],
                chunk_pages=chunk["pages"],
            )

            if self.debug:
                print(f"[LOCATE WORK] chunk={idx}, findings={len(chunk_results)}")

            if chunk_results:
                findings.extend(chunk_results)

        self.data = self._deduplicate_findings_pre_geocode(findings)

        if self.debug:
            print(f"[LOCATE WORK] unique_findings={len(self.data)}")

        return self.data

    def ingestData(self) -> int:
        if self.data is None:
            if self.debug:
                print("[INGEST DATA] self.data is None, καλώ locateWork()")
            self.locateWork()

        db_url = self._resolve_database_url()
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        cur.execute("DELETE FROM public.works WHERE reference_number = %s", (self.ref_number,))

        inserted = 0
        for row in self.data:
            cur.execute("""
                INSERT INTO public.works (
                    reference_number,
                    point_name_raw,
                    point_name_canonical,
                    work,
                    lat,
                    lon,
                    page,
                    pages,
                    excerpt,
                    formatted_address,
                    place_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                self.ref_number,
                row.get("point_name_raw"),
                row.get("point_name_canonical"),
                row.get("work"),
                self._normalize_coord(row.get("lat")),
                self._normalize_coord(row.get("lon")),
                row.get("page"),
                row.get("pages"),
                row.get("excerpt"),
                row.get("formatted_address"),
                row.get("place_id"),
            ))
            inserted += 1

        conn.commit()
        cur.close()
        conn.close()

        if self.debug:
            print(f"[INGEST DATA] inserted_rows={inserted}")

        return inserted

    def downloadContract(self) -> tuple[bytes, str]:
        if self.debug:
            print("[DOWNLOAD CONTRACT] Καλώ getDocument()")

        pdf_bytes = self.getDocument()
        filename = f"{self.ref_number}.pdf"

        if self.debug:
            print(f"[DOWNLOAD CONTRACT] filename={filename}, bytes={len(pdf_bytes)}")

        return pdf_bytes, filename

    def _build_page_aware_chunks(self, text: str, max_chars: int = 18000) -> list[dict]:
        page_pattern = r"--- PAGE (\d+) ---\n"
        matches = list(re.finditer(page_pattern, text))

        if not matches:
            if self.debug:
                print("[CHUNKING] Δεν βρέθηκαν page markers")
            return [{"pages": [], "text": text}]

        sections = []
        for i, match in enumerate(matches):
            page_num = int(match.group(1))
            start = match.start()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
            page_block = text[start:end]

            sections.append({
                "page": page_num,
                "text": page_block,
            })

        chunks = []
        current_pages = []
        current_text_parts = []
        current_len = 0

        for section in sections:
            section_text = section["text"]
            section_len = len(section_text)

            if current_text_parts and current_len + section_len > max_chars:
                chunks.append({
                    "pages": current_pages,
                    "text": "\n".join(current_text_parts),
                })
                current_pages = []
                current_text_parts = []
                current_len = 0

            current_pages.append(section["page"])
            current_text_parts.append(section_text)
            current_len += section_len

        if current_text_parts:
            chunks.append({
                "pages": current_pages,
                "text": "\n".join(current_text_parts),
            })

        if self.debug:
            print(f"[CHUNKING] created_chunks={len(chunks)}")

        return chunks

    def _extract_fireprotection_points(
        self,
        client,
        chunk_text: str,
        chunk_pages: list[int],
    ) -> list[dict]:
    
        schema = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "point_name_raw": {"type": "string"},
                            "point_name_canonical": {"type": "string"},
                            "work": {"type": "string"},
                            "lat": {"type": ["number", "null"]},
                            "lon": {"type": ["number", "null"]},
                            "page": {"type": "integer"},
                            "excerpt": {"type": "string"},
                        },
                        "required": [
                            "point_name_raw",
                            "point_name_canonical",
                            "work",
                            "lat",
                            "lon",
                            "page",
                            "excerpt",
                        ],
                    },
                }
            },
            "required": ["items"],
        }
    
        prompt = f"""
    Διάβασε το παρακάτω απόσπασμα από σύμβαση που αφορά εργασίες σχετικές με προστασία από πυρκαγιές.
    
    Στόχος:
    Εντόπισε ΜΟΝΟ αποσπάσματα όπου αναφέρονται συγκεκριμένες εργασίες, παρεμβάσεις ή δράσεις
    που σχετίζονται άμεσα με πυροπροστασία, όπως:
    - αποψιλώσεις
    - κλαδέματα
    - καθαρισμοί οικοπέδων ή περιβάλλοντος χώρου
    - απομάκρυνση ξερής ή καύσιμης ύλης
    - διάνοιξη ή συντήρηση αντιπυρικών ζωνών
    - άλλες σαφείς εργασίες πρόληψης πυρκαγιάς
    
    Για κάθε εύρημα εξήγαγε:
    - point_name_raw
    - point_name_canonical
    - work
    - lat
    - lon
    - page
    - excerpt
    
    Οδηγίες για τα πεδία:
    
    1. point_name_raw:
       Η αυτούσια ή σχεδόν αυτούσια φράση του κειμένου που προσδιορίζει το σημείο όπου θα γίνουν οι εργασίες.
    
    2. point_name_canonical:
       Η πιο καθαρή, σύντομη και γεωκωδικοποιήσιμη μορφή του ίδιου σημείου.
       Κράτησε μόνο στοιχεία γεωγραφικού προσδιορισμού όπως:
       - οδό
       - αριθμό
       - περιοχή
       - οικισμό
       - δήμο
       - πόλη
       - περιφερειακή ενότητα
    
    3. Στο point_name_canonical αφαίρεσε μη γεωγραφικά στοιχεία όπως:
       - "στις εγκαταστάσεις"
       - "του αναδόχου"
       - "της εταιρείας"
       - "του φορέα"
       - "της ΑΔΜΗΕ Α.Ε."
       - "περιβάλλων χώρος"
       - "χώρος έργου"
    
    4. Αν το ίδιο σημείο εμφανίζεται με μικρές διαφορές διατύπωσης μέσα στο ίδιο chunk,
       επέστρεψε μόνο μία εγγραφή.
    
    5. Αν υπάρχουν πολλές διευθύνσεις που ανήκουν στο ίδιο εύρημα,
       κράτησέ τες μαζί στο ίδιο point_name_canonical και μην τις σπας σε πολλές εγγραφές,
       εκτός αν το κείμενο περιγράφει σαφώς διαφορετικές εργασίες σε διαφορετικά σημεία.
    
    6. work:
       Σύντομη και σαφής περιγραφή της συγκεκριμένης εργασίας πυροπροστασίας.
    
    7. lat και lon:
       Σε αυτό το βήμα να είναι πάντα null.
       Μην κάνεις geocoding και μην επινοείς συντεταγμένες.
    
    8. page:
       Ο αριθμός σελίδας από τα markers --- PAGE X --- όπου εντοπίζεται το point_name_raw.
    
    9. excerpt:
       Σύντομο αυτούσιο απόσπασμα από το κείμενο που να τεκμηριώνει καθαρά
       και το σημείο και την εργασία.
    
    Κανόνες:
    1. Μην επινοείς στοιχεία.
    2. Επέστρεψε item μόνο αν υπάρχει σαφές σημείο ή γεωγραφικός προσδιορισμός.
    3. Αν δεν υπάρχει σαφές σημείο, μην επιστρέψεις item.
    4. Μην συμπεριλάβεις γενικές διοικητικές, οικονομικές ή νομικές αναφορές.
    5. Μην συμπεριλάβεις γενικές περιγραφές έργου χωρίς σαφή σύνδεση με συγκεκριμένο σημείο.
    6. Αν δεν υπάρχει σχετικό εύρημα με σαφές σημείο, επέστρεψε items: [].
    7. Το point_name_canonical πρέπει να γράφεται πάντα σε ενιαία μορφή.
    8. Αν το ίδιο εύρημα περιλαμβάνει περισσότερους από έναν αριθμούς της ίδιας οδού, γράψε την οδό μία φορά και μετά όλους τους αριθμούς χωρισμένους με κόμμα.
    9. Παράδειγμα σωστής μορφής: "Ασκληπιού 22, 24, Κρυονέρι, Αττική".
    10. Μην χρησιμοποιείς εναλλάξ σύμβολα όπως "&", ";", "/" ή τη λέξη "και" για να ενώσεις διευθύνσεις στο point_name_canonical. Χρησιμοποίησε μόνο κόμμα.
    11. Αν το ίδιο σημείο εμφανίζεται επανειλημμένα στο ίδιο ή σε διαφορετικά αποσπάσματα, χρησιμοποίησε ακριβώς την ίδια point_name_canonical μορφή κάθε φορά.
    12. Το work πρέπει να είναι σύντομη κανονικοποιημένη περιγραφή της εργασίας και όχι πλήρης αναλυτική πρόταση.
    13. Για όμοιες αναφορές χρησιμοποίησε σταθερές γενικές μορφές work όπως:
    - "αποψίλωση και καθαρισμός περιβάλλοντος χώρου"
    - "κλάδεμα"
    - "καθαρισμός οικοπέδου"
    - "διάνοιξη αντιπυρικής ζώνης"
    14. Μην επιστρέφεις διαφορετικές εγγραφές μόνο και μόνο επειδή αλλάζει ελαφρά η διατύπωση του ίδιου σημείου ή της ίδιας εργασίας.
    15. Αν το ίδιο σημείο και η ίδια εργασία επαναλαμβάνονται, επέστρεψε μία μόνο εγγραφή ανά chunk.
    
    Οι σελίδες αυτού του chunk είναι: {chunk_pages}
    
    ΚΕΙΜΕΝΟ:
    {chunk_text}
    """.strip()
    
        if self.debug:
            print(f"[OPENAI] sending chunk for pages={chunk_pages}")
    
        response = client.responses.create(
            model="gpt-5-mini",
            input=prompt,
            text={
                "format": {
                    "type": "json_schema",
                    "name": "fire_protection_locations",
                    "schema": schema,
                    "strict": True,
                }
            },
        )
    
        if self.debug:
            print("[OPENAI] response received")
    
        parsed = json.loads(response.output_text)
        items = parsed.get("items", [])
    
        clean_items = []
    
        for item in items:
            page = item.get("page")
    
            if page not in chunk_pages:
                continue
    
            point_name_raw = self._normalize_str(item.get("point_name_raw"))
            point_name_canonical = self._normalize_str(item.get("point_name_canonical"))
            work = self._normalize_str(item.get("work"))
            excerpt = self._normalize_excerpt(item.get("excerpt"))
    
            if not point_name_raw or not point_name_canonical or not work or not excerpt:
                continue
    
            clean_items.append({
                "point_name_raw": point_name_raw,
                "point_name_canonical": point_name_canonical,
                "work": work,
                "lat": None,
                "lon": None,
                "page": page,
                "excerpt": excerpt,
            })
    
        return clean_items

    def geolocatePoint(self, query: str):

        url = "https://maps.googleapis.com/maps/api/geocode/json"
    
        params = {
            "address": query,
            "key": self.google_api_key,
            "region": "gr",
        }
    
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
    
        data = r.json()
    
        if data["status"] != "OK":
            return None
    
        result = data["results"][0]
    
        location = result["geometry"]["location"]
    
        return {
            "lat": location["lat"],
            "lon": location["lng"],
            "formatted_address": result.get("formatted_address"),
            "place_id": result.get("place_id"),
        }

    def geolocateWork(self):

        if self.data is None:
            self.locateWork()
    
        new_data = []
    
        for row in self.data:
    
            point = row.get("point_name_canonical")
    
            if not point:
                row["lat"] = None
                row["lon"] = None
                new_data.append(row)
                continue
    
            query = f"{point}, Ελλάδα"
    
            geo = self.geolocatePoint(query)
    
            if geo:
                row["lat"] = geo["lat"]
                row["lon"] = geo["lon"]
                row["formatted_address"] = geo["formatted_address"]
                row["place_id"] = geo["place_id"]
            else:
                row["lat"] = None
                row["lon"] = None
    
            new_data.append(row)
    
        self.data = self._deduplicate_findings_by_coords(new_data)
    
        return self.data

    def _deduplicate_findings_pre_geocode(self, findings: list[dict]) -> list[dict]:
        results = []

        for item in findings:
            point_name_canonical = self._normalize_str(item.get("point_name_canonical"))
            work = self._normalize_str(item.get("work"))

            existing = None
            for row in results:
                same_canonical = (
                    point_name_canonical
                    and self._normalize_str(row.get("point_name_canonical")) == point_name_canonical
                )
                same_work = self._work_sets_overlap(row.get("work"), work)
                if same_canonical or same_work:
                    existing = row
                    break

            if existing is None:
                results.append({
                    "point_name_raw": item.get("point_name_raw"),
                    "point_name_canonical": point_name_canonical,
                    "work": work,
                    "lat": self._normalize_coord(item.get("lat")),
                    "lon": self._normalize_coord(item.get("lon")),
                    "page": item.get("page"),
                    "pages": [item.get("page")],
                    "excerpt": item.get("excerpt"),
                    "formatted_address": self._normalize_str(item.get("formatted_address")),
                    "place_id": self._normalize_str(item.get("place_id")),
                })
                continue

            self._merge_finding_rows(existing, item)

        return self._finalize_deduplicated_rows(results)

    def _deduplicate_findings_by_coords(self, findings: list[dict]) -> list[dict]:
        grouped = {}

        for idx, item in enumerate(findings):
            lat = self._normalize_coord(item.get("lat"))
            lon = self._normalize_coord(item.get("lon"))
            key = ("coords", round(lat, 6), round(lon, 6)) if lat is not None and lon is not None else ("row", idx)

            if key not in grouped:
                grouped[key] = {
                    "point_name_raw": item.get("point_name_raw"),
                    "point_name_canonical": self._normalize_str(item.get("point_name_canonical")),
                    "work": self._normalize_str(item.get("work")),
                    "lat": lat,
                    "lon": lon,
                    "page": item.get("page"),
                    "pages": [item.get("page")],
                    "excerpt": item.get("excerpt"),
                    "formatted_address": self._normalize_str(item.get("formatted_address")),
                    "place_id": self._normalize_str(item.get("place_id")),
                }
                continue

            self._merge_finding_rows(grouped[key], item)

        return self._finalize_deduplicated_rows(list(grouped.values()))

    def _merge_finding_rows(self, existing: dict, item: dict) -> None:
        point_name_canonical = self._normalize_str(item.get("point_name_canonical"))
        place_id = self._normalize_str(item.get("place_id"))

        if item.get("page") not in existing["pages"]:
            existing["pages"].append(item.get("page"))

        if len(item.get("point_name_raw", "")) > len(existing.get("point_name_raw", "")):
            existing["point_name_raw"] = item.get("point_name_raw")

        candidate_canonical = point_name_canonical or ""
        existing_canonical = existing.get("point_name_canonical", "") or ""
        if candidate_canonical and (
            not existing_canonical
            or len(candidate_canonical) < len(existing_canonical)
        ):
            existing["point_name_canonical"] = candidate_canonical

        if len(item.get("excerpt", "")) > len(existing.get("excerpt", "")):
            existing["excerpt"] = item.get("excerpt")

        if not existing.get("formatted_address") and item.get("formatted_address"):
            existing["formatted_address"] = self._normalize_str(item.get("formatted_address"))

        if not existing.get("place_id") and place_id:
            existing["place_id"] = place_id

        existing["work"] = ", ".join(self._merge_work_values(
            existing.get("work"),
            item.get("work"),
        ))

    def _finalize_deduplicated_rows(self, rows: list[dict]) -> list[dict]:
        for row in rows:
            row["pages"] = sorted(set(page for page in row["pages"] if page is not None))
            row["page"] = row["pages"][0] if row["pages"] else None
        return rows

    def _normalize_str(self, value):
        if value is None:
            return None
        value = str(value).strip()
        return value if value else None

    def _normalize_excerpt(self, value):
        if value is None:
            return ""
        return " ".join(str(value).split()).strip()

    def _normalize_coord(self, value):
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _merge_work_values(self, *values):
        merged = []
        seen = set()
        for value in values:
            if value is None:
                continue
            for part in [x.strip() for x in str(value).split(",")]:
                if not part:
                    continue
                normalized = part.casefold()
                if normalized in seen:
                    continue
                seen.add(normalized)
                merged.append(part)
        return merged

    def _work_sets_overlap(self, left, right) -> bool:
        left_parts = {x.casefold() for x in self._merge_work_values(left)}
        right_parts = {x.casefold() for x in self._merge_work_values(right)}
        if not left_parts or not right_parts:
            return False
        return bool(left_parts & right_parts)

    def _resolve_database_url(self) -> str:
        return resolve_database_url(self.db_path)
