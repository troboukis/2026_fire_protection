from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "delete_procurements_by_keywords.py"
SPEC = importlib.util.spec_from_file_location("delete_procurements_by_keywords", MODULE_PATH)
assert SPEC is not None
assert SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_prepare_keywords_merges_file_and_inline_values(tmp_path: Path):
    keywords_file = tmp_path / "keywords.txt"
    keywords_file.write_text("\nκαθαρισμός\n# ignore\nΑποψίλωση\nκαθαρισμός\n", encoding="utf-8")

    result = MODULE.prepare_keywords(["  Κλάδεμα  ", "αποψίλωση"], keywords_file)

    assert result == ["κλαδεμα", "αποψιλωση", "καθαρισμοσ"]


def test_prepare_columns_uses_defaults_and_removes_duplicates():
    assert MODULE.prepare_columns(None) == ["title", "short_descriptions"]
    assert MODULE.prepare_columns(["title", "title", "contract_type"]) == ["title", "contract_type"]


def test_build_keyword_filter_supports_all_match_mode():
    where_sql, params = MODULE.build_keyword_filter(
        ["title", "short_descriptions"],
        ["καθαρισμοσ", "πυροπροστασια"],
        "all",
        use_unaccent=False,
    )

    assert " AND " in where_sql
    assert "REGEXP_REPLACE(" in where_sql
    assert "TRANSLATE(" in where_sql
    assert where_sql.count("LIKE %s") == 4
    assert params == [
        "%καθαρισμοσ%",
        "%καθαρισμοσ%",
        "%πυροπροστασια%",
        "%πυροπροστασια%",
    ]


def test_normalize_keyword_removes_accents_and_special_characters():
    assert MODULE.normalize_keyword(" Καθαρισμός / αποψίλωση-δασών ") == "καθαρισμοσαποψιλωσηδασων"
    assert MODULE.normalize_keyword("ΠΡΟΛΗΨΗ_ΠΥΡΚΑΓΙΑΣ!!!") == "προληψηπυρκαγιασ"
