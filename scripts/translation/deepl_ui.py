#!/usr/bin/env python3
"""Local, budget-guarded translation of explicitly allowlisted UI catalogues.

No database access, source-document extraction, automatic POST retries or
frontend credentials. Machine output stays local until editorial review.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import html
import json
import os
from pathlib import Path
import re
import sys
import uuid
import xml.etree.ElementTree as ET

import requests
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[2]
STATE_DIR = ROOT / 'state/translations'
GLOSSARY = ROOT / 'app/src/i18n/glossary.el-en.json'
PROJECT_LIMIT = 1_000_000
MAX_BATCH_CHARACTERS = 20_000
# Add reviewed UI-only sources here. Never accept arbitrary input paths or
# database fields. Contract/document titles must never enter text OR context.
CATALOGUES = {
    'analysisMethodology1': {
        'path': ROOT / 'app/src/i18n/catalogs/analysisMethodology1.el.json',
        'kind': 'ui',
        'context': 'Υπάρχον δημοσιογραφικό άρθρο και οδηγίες του FireWatch για μελέτη δημόσιων συμβάσεων πυροπροστασίας. Μόνο editorial αφήγηση, UI και σχόλια παραδειγμάτων κώδικα, όχι πραγματικοί τίτλοι συμβάσεων ή διοικητικών εγγράφων, source values, search roots, raw records ή PDF. Διατήρηση λογικών συνόλων και επιστημονικών επιφυλάξεων. Διάκριση contract values, payments, funding, commitments, VAT και double counting. Primary και Secondary CPVs παραμένουν οι αντίστοιχες κατηγορίες. Consistent inclusion/exclusion keywords, contracting authority και contractor. Code placeholders αποδίδονται ως YOUR_MAP_KEY. FireWatch, EFFIS, FIRMS, MODIS, VIIRS, EPSG και identifiers διατηρούνται.',
    },
    'analysisMethodology2': {
        'path': ROOT / 'app/src/i18n/catalogs/analysisMethodology2.el.json',
        'kind': 'ui',
        'context': 'Υπάρχον δημοσιογραφικό άρθρο και οδηγίες του FireWatch για μελέτη δημόσιων συμβάσεων πυροπροστασίας. Μόνο editorial αφήγηση, UI και σχόλια παραδειγμάτων κώδικα, όχι πραγματικοί τίτλοι συμβάσεων ή διοικητικών εγγράφων, source values, search roots, raw records ή PDF. Διατήρηση λογικών συνόλων και επιστημονικών επιφυλάξεων. Διάκριση contract values, payments, funding, commitments, VAT και double counting. Primary και Secondary CPVs παραμένουν οι αντίστοιχες κατηγορίες. Consistent inclusion/exclusion keywords, contracting authority και contractor. Code placeholders αποδίδονται ως YOUR_MAP_KEY. FireWatch, EFFIS, FIRMS, MODIS, VIIRS, EPSG και identifiers διατηρούνται.',
    },
    'metadata': {
        'path': ROOT / 'app/src/i18n/catalogs/metadata.el.json',
        'kind': 'ui',
        'context': 'Τίτλοι σελίδων και σύντομες περιγραφές για μηχανές αναζήτησης και κοινοποίηση της εφαρμογής FireWatch. Ελληνικές δασικές πυρκαγιές, δημόσιες συμβάσεις πυροπροστασίας, διαφάνεια και δεδομένα από ΚΗΜΔΗΣ/KIMDIS και Diavgeia. Διάκριση contract value από payments και funding, διατήρηση ΦΠΑ/ετών/αριθμών και ονομασίας Ministry of Environment and Energy. Μόνο editorial metadata/UI σελίδων, όχι τίτλοι ή περιγραφές συγκεκριμένων συμβάσεων ή διοικητικών εγγράφων, subjects, records ή PDFtext. Το FireWatch είναι brand και δεν μεταφράζεται.',
    },

    'analysisWestAtticaPlaces': {
        'path': ROOT / 'app/src/i18n/catalogs/analysisWestAtticaPlaces.el.json',
        'kind': 'ui',
        'context': 'Γεωγραφικές ετικέτες σημείων του χάρτη Δυτικής Αττικής και ελεγχόμενοι τύποι παρεμβάσεων AntiNERO: συντήρηση/εγκατάσταση δεξαμενών και διαχείριση καύσιμης ύλης. Μόνο point_name_canonical και work_type, όχι work/τίτλοι συμβάσεων ή αποφάσεων, subjects, PDFtext ή ονόματα αναδόχων. Τα ονόματα τόπων μεταγράφονται, Δήμος Municipality, Περιφερειακή Ενότητα Regional Unit, Δυτική Αττική West Attica.',
    },

    'analysisWestAtticaUi': {
        'path': ROOT / 'app/src/i18n/catalogs/analysisWestAtticaUi.el.json',
        'kind': 'ui',
        'context': 'Υπάρχον κείμενο διεπαφής και δημοσιογραφικής ανάλυσης διαδραστικού χάρτη πυρκαγιάς Δυτικής Αττικής–Βοιωτίας2026. Πιστή απόδοση επιστημονικών επιφυλάξεων, χωρίς συμπεράσματα αποτελεσματικότητας ή αιτιότητας πέρα από το πρωτότυπο. FRP Fire Radiative Power, burn severity, spotting, fuel management, AntiNERO. Διατήρηση ακριβών ημερομηνιών/ωρών/αριθμών, μονάδων stremmata, Mg/ha, km/h, MW και όλων των placeholders. Ministry of Environment and Energy. Οι στεγασμένες αντιπυρικές ζώνες είναι shaded fuel breaks. Δεν περιέχονται τίτλοι/περιγραφές συγκεκριμένων συμβάσεων ή αποφάσεων, subjects, API diagnostics ή PDFtext. Το editorial σώμα ανάλυσης περιλαμβάνεται, με HTMLmarkup εκτός corpus.',
    },
    'analysisWestAtticaText': {
        'path': ROOT / 'app/src/i18n/catalogs/analysisWestAtticaText.el.json',
        'kind': 'ui',
        'context': 'Υπάρχον κείμενο διεπαφής και δημοσιογραφικής ανάλυσης διαδραστικού χάρτη πυρκαγιάς Δυτικής Αττικής–Βοιωτίας2026. Πιστή απόδοση επιστημονικών επιφυλάξεων, χωρίς συμπεράσματα αποτελεσματικότητας ή αιτιότητας πέρα από το πρωτότυπο. FRP Fire Radiative Power, burn severity, spotting, fuel management, AntiNERO. Διατήρηση ακριβών ημερομηνιών/ωρών/αριθμών, μονάδων stremmata, Mg/ha, km/h, MW και όλων των placeholders. Ministry of Environment and Energy. Οι στεγασμένες αντιπυρικές ζώνες είναι shaded fuel breaks. Δεν περιέχονται τίτλοι/περιγραφές συγκεκριμένων συμβάσεων ή αποφάσεων, subjects, API diagnostics ή PDFtext. Το editorial σώμα ανάλυσης περιλαμβάνεται, με HTMLmarkup εκτός corpus.',
    },

    'analysisYearly1': {
        'path': ROOT / 'app/src/i18n/catalogs/analysisYearly1.el.json',
        'kind': 'ui',
        'context': 'Υπάρχον editorial κείμενο ετήσιας αναφοράς FireWatch για συμβάσεις και οριστικοποιήσεις πληρωμών πυροπροστασίας. Πιστή απόδοση της υπάρχουσας μεθοδολογίας χωρίς νέους ισχυρισμούς. Διατήρηση διακριτών contract value, payments, funding, ΦΠΑ/μισθοδοσίας και αποφυγής διπλής καταμέτρησης. Κάθε placeholder διατηρείται ακριβώς. Δεν περιλαμβάνονται τίτλοι ή περιγραφές συγκεκριμένων συμβάσεων/διοικητικών εγγράφων, PDF text ή raw δεδομένα. Ποσά, ελληνικά αναγνωριστικά και αριθμοί δεν αλλάζουν. Τα εκατ./χιλ. είναι million/thousand. Οι σύνδεσμοι δήμων είναι Associations of Municipalities. Ονομασία Ministry of Environment and Energy.',
    },
    'analysisYearly2': {
        'path': ROOT / 'app/src/i18n/catalogs/analysisYearly2.el.json',
        'kind': 'ui',
        'context': 'Υπάρχον editorial κείμενο ετήσιας αναφοράς FireWatch για συμβάσεις και οριστικοποιήσεις πληρωμών πυροπροστασίας. Πιστή απόδοση της υπάρχουσας μεθοδολογίας χωρίς νέους ισχυρισμούς. Διατήρηση διακριτών contract value, payments, funding, ΦΠΑ/μισθοδοσίας και αποφυγής διπλής καταμέτρησης. Κάθε placeholder διατηρείται ακριβώς. Δεν περιλαμβάνονται τίτλοι ή περιγραφές συγκεκριμένων συμβάσεων/διοικητικών εγγράφων, PDF text ή raw δεδομένα. Ποσά, ελληνικά αναγνωριστικά και αριθμοί δεν αλλάζουν. Τα εκατ./χιλ. είναι million/thousand. Οι σύνδεσμοι δήμων είναι Associations of Municipalities. Ονομασία Ministry of Environment and Energy.',
    },
    'analysisYearly3': {
        'path': ROOT / 'app/src/i18n/catalogs/analysisYearly3.el.json',
        'kind': 'ui',
        'context': 'Υπάρχον editorial κείμενο ετήσιας αναφοράς FireWatch για συμβάσεις και οριστικοποιήσεις πληρωμών πυροπροστασίας. Πιστή απόδοση της υπάρχουσας μεθοδολογίας χωρίς νέους ισχυρισμούς. Διατήρηση διακριτών contract value, payments, funding, ΦΠΑ/μισθοδοσίας και αποφυγής διπλής καταμέτρησης. Κάθε placeholder διατηρείται ακριβώς. Δεν περιλαμβάνονται τίτλοι ή περιγραφές συγκεκριμένων συμβάσεων/διοικητικών εγγράφων, PDF text ή raw δεδομένα. Ποσά, ελληνικά αναγνωριστικά και αριθμοί δεν αλλάζουν. Τα εκατ./χιλ. είναι million/thousand. Οι σύνδεσμοι δήμων είναι Associations of Municipalities. Ονομασία Ministry of Environment and Energy.',
    },

    'analysisAntinero': {
        'path': ROOT / 'app/src/i18n/catalogs/analysisAntinero.el.json',
        'kind': 'ui',
        'context': 'Ετικέτες και επεξηγήσεις διεπαφής δικτύου συμβάσεων AntiNERO στη Δυτική Αττική. Συνδέσεις αρχικών συμβάσεων, τροποποιήσεων, συμπληρώσεων, κοινών αποφάσεων και αναδόχων. Διάκριση έγκρισης από υπογραφή σύμβασης, τιμήματος από πληρωμές, διατήρηση ΦΠΑ/αριθμών/ετών και stremmata χωρίς μετατροπή. Περιλαμβάνονται μόνο UI/categories και ονόματα δασικών υπηρεσιών/περιοχών. Εξαιρούνται όλοι οι τίτλοι και περιγραφές συμβάσεων/αποφάσεων, subjects, PDF text και ονόματα ιδιωτών.',
    },
    'analysisStatistics': {
        'path': ROOT / 'app/src/i18n/catalogs/analysisStatistics.el.json',
        'kind': 'ui',
        'context': 'UI και επεξηγηματικά κείμενα στατιστικής ανάλυσης δημοσίων συμβάσεων πυροπροστασίας FireWatch: μηνιαία ποσά/πλήθος, απευθείας αναθέσεις, κατανομή αξιών χωρίς ΦΠΑ, τύποι/διαδικασίες, φορείς και CPV. Διατήρηση ακριβών αριθμών, αναφορών νόμων, λογαριθμικής κλίμακας και διάκρισης ποσού από πλήθος. Τα Ιαν/Φεβ/Μαρ/Απρ/Μαϊ/Ιουν/Ιουλ/Αυγ/Σεπ/Οκτ/Νοε/Δεκ είναι συντομογραφίες μηνών. Δεν περιέχονται τίτλοι συμβάσεων/διοικητικών εγγράφων, API diagnostics ή πραγματικές εγγραφές.',
    },
    'analysisCatalog': {
        'path': ROOT / 'app/src/i18n/catalogs/analysisCatalog.el.json',
        'kind': 'ui',
        'context': 'Τίτλοι και περιγραφές editorial αναλύσεων του FireWatch, κάρτες καταλόγου και αρχικό carousel/promo. Δασικές πυρκαγιές, συμβάσεις, δαπάνες, απευθείας αναθέσεις και έργα AntiNERO. Διατήρηση stremmata χωρίς μετατροπή μονάδων, ποσών/αριθμών και βάσης ΦΠΑ. Πρόκειται για editorial τίτλους αναλύσεων και UI, όχι τίτλους συγκεκριμένων συμβάσεων ή διοικητικών εγγράφων, οι οποίοι εξαιρούνται. Δεν περιλαμβάνεται περιεχόμενο πρωτοτύπων PDF.',
    },
    'taxonomy': {
        'path': ROOT / 'app/src/i18n/catalogs/taxonomy.el.json',
        'kind': 'ui',
        'context': 'Ετικέτες τύπων διοικητικών πράξεων Διαύγειας, διαδικασιών/κριτηρίων δημόσιας σύμβασης, θεματικών κατηγοριών και τύπων φορέων στο FireWatch. Διατήρηση αναφορών σε άρθρα/παραγράφους νόμων και διακριτών εννοιών αξίας, δέσμευσης, πληρωμής και έγκρισης. Δεν πρόκειται για τίτλους ή περιεχόμενο συγκεκριμένων αποφάσεων/συμβάσεων.',
    },
    'cpvSuppliesWorks': {
        'path': ROOT / 'app/src/i18n/catalogs/cpvSuppliesWorks.el.json',
        'kind': 'ui',
        'context': 'Ελληνικές περιγραφές κατηγοριών του Κοινού Λεξιλογίου Δημοσίων Συμβάσεων CPV 2008, αγαθά και έργα, για αγγλική διεπαφή FireWatch. Όχι τίτλοι συμβάσεων ή εγγράφων. Διατήρηση μονάδων, αριθμητικών αναφορών και της ακριβούς κατηγορίας. Οι αποδόσεις θα ελεγχθούν με το επίσημο αγγλικό λεξικό CPV.',
    },
    'cpvServices': {
        'path': ROOT / 'app/src/i18n/catalogs/cpvServices.el.json',
        'kind': 'ui',
        'context': 'Ελληνικές περιγραφές κατηγοριών του Κοινού Λεξιλογίου Δημοσίων Συμβάσεων CPV 2008, υπηρεσίες, για αγγλική διεπαφή FireWatch. Όχι τίτλοι συμβάσεων ή εγγράφων. Διατήρηση μονάδων, αριθμητικών αναφορών και της ακριβούς κατηγορίας. Οι αποδόσεις θα ελεγχθούν με το επίσημο αγγλικό λεξικό CPV.',
    },
    'mediaTitles': {
        'path': ROOT / 'app/src/i18n/catalogs/mediaTitles.el.json',
        'kind': 'ui',
        'context': 'Τίτλοι δημοσιεύσεων και ραδιοφωνικών/τηλεοπτικών εκπομπών στη σελίδα Media του FireWatch. Φυσική αγγλική απόδοση των τίτλων, με διατήρηση ποσοστών, συχνοτήτων και ταυτότητας προσώπων/μέσων. Πρόκειται για ρητά επιτρεπόμενους τίτλους media, όχι για τίτλους συμβάσεων ή διοικητικών εγγράφων, οι οποίοι εξαιρούνται.',
    },
    'publicInfo': {
        'path': ROOT / 'app/src/i18n/catalogs/publicInfo.el.json',
        'kind': 'ui',
        # This context describes publicInfo only. Media headlines are now
        # translated in mediaTitles. Keep this context to reuse its paid cache.
        'context': 'Υπάρχοντα κείμενα διεπαφής, όρων χρήσης, απορρήτου και επιλογών cookies του FireWatch. Πιστή μετάφραση του υπάρχοντος περιεχομένου, χωρίς προσθήκη ή αλλαγή νομικών όρων, ισχυρισμών, διάρκειας cookies ή συμπεριφοράς συγκατάθεσης. Διατηρούνται ονόματα παρόχων, επικοινωνία και placeholders. Οι τίτλοι δημοσιεύσεων, εκπομπών, συμβάσεων και εγγράφων αποκλείονται.',
    },
    'overview': {
        'path': ROOT / 'app/src/i18n/catalogs/overview.el.json',
        'kind': 'ui',
        'context': 'Κείμενα διεπαφής του FireWatch για δασικές πυρκαγιές, δορυφορικές παρατηρήσεις, καμένες εκτάσεις, κρατική χρηματοδότηση και δημόσιες συμβάσεις. Το στρέμμα παραμένει stremma και ο πληθυντικός stremmata, χωρίς μετατροπή μονάδων ή τιμών. Περιλαμβάνονται μόνο στατικές ετικέτες και επεξηγήσεις, όχι τίτλοι εγγράφων ή εγγραφές βάσης.',
    },
    'places': {
        'path': ROOT / 'app/src/i18n/catalogs/places.el.json',
        'kind': 'ui',
        'context': None,  # shared UI context below; reuses matching paid text
    },
    'ministry': {
        'path': ROOT / 'app/src/i18n/catalogs/ministry.el.json',
        'kind': 'ui',
        'context': None,  # same reviewed UI context; no source record fields
    },
    'procurement': {
        'path': ROOT / 'app/src/i18n/catalogs/procurement.el.json',
        'kind': 'ui',
        'context': 'Ετικέτες, φίλτρα και βοήθεια του FireWatch για δημόσιες συμβάσεις και διοικητικές αποφάσεις. Όχι τίτλοι εγγράφων. Τα ποσά παραμένουν ευρώ και η βάση ΦΠΑ διατηρείται.',
    },
}
# Full authority names are explicitly requested by the user. Only the reviewed
# name catalogues are allowed; contract/document titles remain excluded.
for _name in ('municipalityNames', 'regionNames', 'organizationNames1',
              'organizationNames2', 'organizationNames3', 'organizationNames4',
              'organizationNames5'):
    CATALOGUES[_name] = {
        'path': ROOT / f'app/src/i18n/catalogs/{_name}.el.json',
        'kind': 'ui',
        'context': (
            'Ονομασίες ελληνικών δήμων ως τοπωνύμια στη διεπαφή FireWatch. Απόδοση με αγγλική ονομαστική και καθιερωμένη λατινική γραφή, όχι ως γενική ή ονόματα κατοίκων. Διατήρηση ιστορικών/συνδυαστικών ονομασιών και slash. Πρόκειται αποκλειστικά για ονομασίες δήμων, όχι τίτλους συμβάσεων ή εγγράφων.'
            if _name == 'municipalityNames' else
            'Ονομασίες ελληνικών διοικητικών περιφερειών στη διεπαφή FireWatch, ως γεωγραφικά ονόματα στην αγγλική ονομαστική. Διατήρηση της ακριβούς περιφέρειας. Όχι τίτλοι συμβάσεων ή εγγράφων.'
            if _name == 'regionNames' else
            'Πλήρη ή συντομευμένα ονόματα ελληνικών δημόσιων φορέων στη διεπαφή FireWatch: υπουργεία, δήμοι, σύνδεσμοι, πανεπιστήμια, νοσοκομεία, δημόσιες και δημοτικές επιχειρήσεις. Αγγλική ονομασία με σωστά τοπωνύμια και νομική μορφή, χωρίς επέκταση ακρωνυμίων όταν δεν τεκμηριώνεται από το όνομα. Όχι τίτλοι συμβάσεων ή εγγράφων. Διατήρηση ιστορικής ονομασίας φορέα, όχι αντικατάσταση από σημερινό διάδοχο. Οι περιγραφικοί όροι μεταφράζονται, τα κύρια ονόματα μεταγράφονται και διατηρούνται αριθμοί/αναφορές νόμων.'
        ),
    }

CATALOGUES['places']['context'] = CATALOGUES['overview']['context']
CATALOGUES['ministry']['context'] = CATALOGUES['overview']['context']

OPTIONS = {
    'source_lang': 'EL', 'target_lang': 'EN-GB', 'tag_handling': 'xml',
    'ignore_tags': ['x'], 'preserve_formatting': True,
    'split_sentences': 'nonewlines', 'show_billed_characters': True,
}


class TranslationError(Exception):
    pass


def now():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def glossary_spec(glossary):
    # Editorial notes/status do not alter the translation request. Changing
    # those must not cause already paid text to be submitted again.
    return {**{key: glossary.get(key) for key in ('version', 'source_lang', 'target_lang', 'translation_target_lang', 'protected_tokens')},
            'entries': [{key: entry[key] for key in ('source', 'target')} for entry in glossary['entries']]}


def save_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    with temporary.open('w') as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2)
        handle.write('\n')
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


@contextmanager
def locked(directory):
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / '.lock').open('a') as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise TranslationError('Another translation process holds the local lock.') from None
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def integer(value):
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def validate_usage(data):
    if not isinstance(data, dict) or not all(integer(data.get(k)) for k in ('character_count', 'character_limit')):
        raise TranslationError('Missing or invalid account usage; translation stopped.')
    selected = {k: data[k] for k in ('character_count', 'character_limit')}
    if 'api_key_character_limit' in data or 'api_key_character_count' in data:
        if not all(integer(data.get(k)) for k in ('api_key_character_count', 'api_key_character_limit')):
            raise TranslationError('Incomplete API-key usage; translation stopped.')
        selected.update({k: data[k] for k in ('api_key_character_count', 'api_key_character_limit')})
    return selected


def charged(state):
    return sum(r['billed'] if r['status'] == 'complete' else r['reserved'] for r in state['requests'])


def capacity(usage, state):
    used = charged(state)
    # Deliberately retain all local reservations in addition to remote usage.
    # This can double-count already reflected characters, but never releases
    # headroom merely because time passed or a remote billing period reset.
    # No automatic reconciliation or limit increase is available in this CLI.
    remaining = [PROJECT_LIMIT - used, usage['character_limit'] - usage['character_count'] - used]
    if 'api_key_character_limit' in usage:
        remaining.append(usage['api_key_character_limit'] - usage['api_key_character_count'] - used)
    return max(0, min(remaining))


def protect(text, tokens):
    alternatives = [r'\{\{[^{}]+\}\}', r'https?://[^\s<>]+', r'</?\d+>', r'(?<!\w)\d+(?:[.,]\d+)*(?:%)?']
    alternatives.extend(re.escape(token) for token in sorted(tokens, key=len, reverse=True))
    matcher = re.compile('|'.join(alternatives))
    pieces, cursor, values = [], 0, []
    for match in matcher.finditer(text):
        pieces.append(html.escape(text[cursor:match.start()]))
        value = match.group()
        pieces.append(f'<x id="{len(values)}">{html.escape(value)}</x>')
        values.append(value)
        cursor = match.end()
    pieces.append(html.escape(text[cursor:]))
    return '<s>' + ''.join(pieces) + '</s>', values


def restore(text, expected):
    try:
        element = ET.fromstring(text)
    except ET.ParseError:
        raise TranslationError('Invalid translated markup; output retained for review, no retry.') from None
    if element.tag != 's' or any(node.tag != 'x' for node in list(element)):
        raise TranslationError('Unexpected translated tags; no retry.')
    actual = [(node.get('id'), node.text or '') for node in element]
    required = [(str(i), value) for i, value in enumerate(expected)]
    if Counter(actual) != Counter(required) or any(len(node) for node in element):
        raise TranslationError('Protected values changed or disappeared; no retry.')
    return ''.join(element.itertext())


def read_catalogue(name):
    definition = CATALOGUES.get(name)
    if not definition or definition['kind'] != 'ui':
        raise TranslationError('Only allowlisted UI catalogues are accepted. Source titles are excluded.')
    messages = json.loads(definition['path'].read_text())
    if not isinstance(messages, dict) or not messages or any(not isinstance(v, str) or not v.strip() for v in messages.values()):
        raise TranslationError('A catalogue must contain nonempty UI strings only.')
    return definition, messages


class Client:
    def __init__(self, key, directory=STATE_DIR, session=None):
        self.directory = directory
        self.path = directory / 'ledger.json'
        self.session = session or requests.Session()
        self.origin = 'https://api-free.deepl.com' if key.endswith(':fx') else 'https://api.deepl.com'
        self.headers = {'Authorization': 'DeepL-Auth-Key ' + key}
        binding = hashlib.sha256(key.encode()).hexdigest()
        if self.path.exists():
            self.state = json.loads(self.path.read_text())
            if self.state.get('schema') != 1 or self.state.get('key_fingerprint') != binding:
                raise TranslationError('Ledger/key mismatch; review existing usage before continuing.')
        else:
            self.state = {'schema': 1, 'key_fingerprint': binding, 'requests': [], 'cache': {}, 'glossaries': {}, 'usage_checks': []}
            self.save()

    def save(self):
        save_json(self.path, self.state)

    def api(self, method, path, payload=None, accepted=(200,)):
        # requests does not retry by default. Never print the key, request
        # headers, arbitrary response bodies or exceptions containing them.
        try:
            response = self.session.request(method, self.origin + path, headers=self.headers,
                                            json=payload, timeout=(10, 45), allow_redirects=False)
        except requests.RequestException:
            raise TranslationError('DeepL connection failed. Submitted requests are NOT automatically retried.') from None
        if response.status_code not in accepted:
            raise TranslationError(f'DeepL HTTP {response.status_code}; stopped without retry.')
        try:
            return response.json()
        except ValueError:
            raise TranslationError('Invalid DeepL response; stopped without retry.') from None

    def usage(self):
        value = validate_usage(self.api('GET', '/v2/usage'))
        self.state['usage_checks'].append({'at': now(), **value})
        self.save()
        return value

    def ensure_clear(self):
        if any(r['status'] != 'complete' for r in self.state['requests']):
            raise TranslationError('An unresolved request reserves budget. Reconcile it before sending more text.')

    def glossary(self, glossary):
        identity = fingerprint(glossary_spec(glossary))
        legacy = fingerprint(glossary)
        if identity not in self.state['glossaries'] and legacy in self.state['glossaries']:
            self.state['glossaries'][identity] = self.state['glossaries'][legacy]
            self.save()
        glossary_id = self.state['glossaries'].get(identity)
        if glossary_id:
            info = self.api('GET', '/v3/glossaries/' + glossary_id)
            if not any(d.get('source_lang', '').upper() == 'EL' and d.get('target_lang', '').upper() == 'EN'
                       and d.get('entry_count') == len(glossary['entries']) for d in info.get('dictionaries', [])):
                raise TranslationError('Remote glossary does not match the expected language pair/entry count.')
            return glossary_id
        # Deterministic name permits recovery after a lost glossary-create
        # response. Do not modify or delete anyone else's glossary.
        name = 'FireWatch EL-EN ' + identity[:16]
        existing = self.api('GET', '/v3/glossaries')
        matches = [g for g in existing.get('glossaries', []) if g.get('name') == name]
        if len(matches) > 1:
            raise TranslationError('Multiple matching glossaries; review before continuing.')
        if matches:
            result = matches[0]
        else:
            dictionary = {'source_lang': 'EL', 'target_lang': 'EN',
                          'entries': '\n'.join(e['source'] + '\t' + e['target'] for e in glossary['entries']),
                          'entries_format': 'tsv'}
            owned = [g for g in existing.get('glossaries', [])
                     if g.get('glossary_id') in self.state['glossaries'].values()
                     and g.get('name', '').startswith('FireWatch EL-EN ')]
            if owned:
                if len(owned) != 1 or len(owned[0].get('dictionaries', [])) != 1:
                    raise TranslationError('Ambiguous owned glossary; stopped before updating it.')
                old = owned[0]
                language_pair = old['dictionaries'][0]
                if (language_pair.get('source_lang', '').upper(), language_pair.get('target_lang', '').upper()) != ('EL', 'EN'):
                    raise TranslationError('Owned glossary has an unexpected language pair; stopped.')
                old_id = old['glossary_id']
                entries_path = '/v3/glossaries/' + old_id + '/entries?source_lang=EL&target_lang=EN'
                snapshot = self.api('GET', entries_path)
                update = {'at': now(), 'glossary_id': old_id, 'before': snapshot, 'status': 'prepared',
                          'old_fingerprints': [k for k, v in self.state['glossaries'].items() if v == old_id]}
                self.state.setdefault('glossary_updates', []).append(update)
                self.save()
                # Update only our own single dictionary. This does not consume
                # translation characters or require a second glossary slot.
                self.api('PUT', '/v3/glossaries/' + old_id + '/dictionaries', dictionary)
                verified = self.api('GET', entries_path)
                dictionaries = verified.get('dictionaries', [])
                expected = set(dictionary['entries'].splitlines())
                if len(dictionaries) != 1 or set(dictionaries[0].get('entries', '').splitlines()) != expected:
                    raise TranslationError('Updated glossary could not be verified; no translation sent.')
                update.update(status='complete', after=verified)
                self.state['glossaries'] = {k: v for k, v in self.state['glossaries'].items() if v != old_id}
                self.save()
                result = {'glossary_id': old_id}
            else:
                result = self.api('POST', '/v3/glossaries', {'name': name, 'dictionaries': [dictionary]}, accepted=(201,))
        glossary_id = result.get('glossary_id')
        if not isinstance(glossary_id, str) or not re.fullmatch(r'[a-zA-Z0-9-]+', glossary_id):
            raise TranslationError('Missing glossary ID; no translation sent.')
        self.state['glossaries'][identity] = glossary_id
        self.save()
        return glossary_id

    def items(self, messages, context, glossary):
        result = []
        for key, source in messages.items():
            wire, protected = protect(source, glossary['protected_tokens'])
            identity = fingerprint({'source': source, 'context': context, 'glossary': glossary_spec(glossary), 'options': OPTIONS})
            legacy = fingerprint({'source': source, 'context': context, 'glossary': glossary, 'options': OPTIONS})
            if identity not in self.state['cache'] and legacy in self.state['cache']:
                self.state['cache'][identity] = self.state['cache'][legacy]
            if identity in self.state['cache']:
                self.state['cache'][identity].update(context=context, options_fingerprint=fingerprint(OPTIONS))
            result.append({'key': key, 'source': source, 'wire': wire, 'protected': protected, 'identity': identity})
        old_glossary = fingerprint(glossary)
        if old_glossary in self.state['glossaries']:
            self.state['glossaries'][fingerprint(glossary_spec(glossary))] = self.state['glossaries'][old_glossary]
        self.save()
        return result

    def review(self, items, context, targets):
        if set(targets) != {item['key'] for item in items}:
            raise TranslationError('Reviewed keys do not match the source catalogue.')
        updates = {}
        for item in items:
            target = targets[item['key']]
            if not isinstance(target, str) or not target.strip():
                raise TranslationError('Empty reviewed UI message; approval stopped.')
            tokens = lambda value: Counter(re.findall(r'\{\{[^{}]+\}\}', value))
            if tokens(item['source']) != tokens(target):
                raise TranslationError('Reviewed placeholders differ from the source; approval stopped.')
            # Editorial approval may carry paid text across an intentional
            # glossary update. It cannot invent an initial machine translation
            # or reuse one from a different context/language/options.
            candidates = [entry for entry in self.state['cache'].values()
                          if entry.get('source') == item['source'] and entry.get('context') == context
                          and entry.get('options_fingerprint') == fingerprint(OPTIONS)]
            if not candidates:
                raise TranslationError('No matching paid machine translation; translate the new UI message first.')
            updates[item['identity']] = {**candidates[-1], 'reviewed': target, 'reviewed_at': now()}
        self.state['cache'].update(updates)
        self.save()

    def translate_batch(self, items, context, glossary_id):
        self.ensure_clear()
        before = self.usage()  # failure here guarantees no POST
        reservation = sum(len(i['wire']) for i in items)  # Unicode code points, conservatively includes XML
        if not items or len(items) > 50 or reservation > MAX_BATCH_CHARACTERS or reservation > capacity(before, self.state):
            raise TranslationError('Batch exceeds the safe remaining budget or request limits.')
        payload = {**OPTIONS, 'text': [i['wire'] for i in items], 'context': context, 'glossary_id': glossary_id}
        if len(json.dumps(payload).encode()) > 120_000:
            raise TranslationError('Encoded batch is too large; no translation sent.')
        record = {'id': str(uuid.uuid4()), 'at': now(), 'status': 'reserved', 'reserved': reservation,
                  'identities': [i['identity'] for i in items], 'usage_before': before}
        self.state['requests'].append(record)
        self.save()  # durable reservation BEFORE the billable request
        try:
            result = self.api('POST', '/v2/translate', payload)
            record['response'] = result  # retain even malformed/partial output; never discard paid work
            self.save()
            translations = result.get('translations', [])
            if len(translations) != len(items) or any(not integer(t.get('billed_characters')) for t in translations):
                raise TranslationError('Incomplete translation/billing response; reservation retained.')
            billed = sum(t['billed_characters'] for t in translations)
            if billed > reservation:
                record['reserved'] = billed
                raise TranslationError('Billing exceeded the conservative estimate; stopped for review.')
            restored = [restore(t['text'], i['protected']) for i, t in zip(items, translations)]
            for item, translation in zip(items, restored):
                self.state['cache'][item['identity']] = {'source': item['source'], 'machine': translation, 'at': now()}
            record.update(status='complete', billed=billed)
            self.save()
        except (TranslationError, KeyError, TypeError, ValueError):
            record['status'] = 'uncertain'
            self.save()
            try:
                record['usage_after'] = self.usage()
                self.save()
            except TranslationError:
                pass
            raise TranslationError('Translation stopped with an unresolved request. Output/reservation retained; no retry.') from None
        record['usage_after'] = self.usage()  # failure stops the loop; completed output already durable
        self.save()
        return billed


def run(command, catalogue, key, max_characters, directory=STATE_DIR):
    with locked(directory):
        client = Client(key, directory)
        usage = client.usage()
        report = {'usage': usage, 'project_characters_committed': charged(client.state),
                  'conservative_available': capacity(usage, client.state)}
        if command == 'usage':
            return report
        client.ensure_clear()
        glossary = json.loads(GLOSSARY.read_text())
        if command == 'glossary':
            glossary_id = client.glossary(glossary)
            return {**report, 'glossary_id': glossary_id, 'entry_count': len(glossary['entries']), 'usage_after': client.usage()}
        definition, messages = read_catalogue(catalogue)
        items = client.items(messages, definition['context'], glossary)
        if command == 'review':
            targets = json.loads(definition['path'].with_name(definition['path'].name.replace('.el.json', '.en.json')).read_text())
            client.review(items, definition['context'], targets)
            return {**report, 'reviewed_messages': len(items), 'usage_after': client.usage()}
        pending = list({i['identity']: i for i in items if i['identity'] not in client.state['cache']}.values())
        required = sum(len(i['wire']) for i in pending)
        report.update(catalogue=catalogue, messages=len(items), uncached_unique_messages=len(pending),
                      reserved_estimate=required, max_run_characters=max_characters)
        if command == 'plan':
            return report
        if required > max_characters or required > report['conservative_available']:
            raise TranslationError('Run exceeds its explicit character ceiling or remaining budget. Nothing translated.')
        if pending:
            glossary_id = client.glossary(glossary)
            client.usage()  # check again after glossary preparation
            for start in range(0, len(pending), 50):
                client.translate_batch(pending[start:start + 50], definition['context'], glossary_id)
        machine = {i['key']: client.state['cache'][i['identity']].get('reviewed', client.state['cache'][i['identity']]['machine']) for i in items}
        save_json(directory / (catalogue + '.machine.json'), machine)
        after = client.usage()
        report.update(usage_after=after, project_characters_committed=charged(client.state),
                      conservative_available=capacity(after, client.state),
                      machine_output=str(directory / (catalogue + '.machine.json')))
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('usage', 'plan', 'translate', 'review', 'glossary'))
    parser.add_argument('--catalogue', choices=tuple(CATALOGUES), default='procurement')
    parser.add_argument('--max-characters', type=int, default=MAX_BATCH_CHARACTERS)
    args = parser.parse_args()
    if not 0 < args.max_characters <= MAX_BATCH_CHARACTERS:
        parser.error('--max-characters must be between 1 and 20000; it never increases the project/account limit.')
    values = dotenv_values(ROOT / '.env')
    key = next((values[name] for name in ('DEEPL_API_KEY', 'deepl_api_key', 'DEEPL_AUTH_KEY') if values.get(name)), None)
    if not key:
        parser.error('DeepL key is missing from the local root .env.')
    try:
        result = run(args.command, args.catalogue, key, args.max_characters)
    except (TranslationError, OSError, ValueError) as error:
        # Never output arbitrary filesystem/environment/API exception content.
        print(str(error) if isinstance(error, TranslationError) else 'Invalid local translation state; stopped.', file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    sys.exit(main())
