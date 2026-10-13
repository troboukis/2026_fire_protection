"""Shared root exclusions and matching for KIMDIS and Diavgeia ingestion."""

import re
import unicodedata

REQUESTED_EXCLUDE_KEYWORDS = (
    "σχολει", "σχολικ", "πανεπιστημ", "γηπεδ",
    "πιστοποιητικ πυρασφαλ", "κηποτεχνικ συντηρησ", "συντηρησ κηπ",
    "οακα", "παραγωγ φυτ", "φυτωριακ εργασ", "δασικ φυτωρι",
    "καυσιμ θερμανσ", "κολυμβητηρ", "server room",
    "προσχολικ", "διασχολικ", "γυμνασ", "λυκει",
)

# User-requested exclusions: ordered roots, up to two intervening words.
ROOT_EXCLUDE_KEYWORDS = (
    *REQUESTED_EXCLUDE_KEYWORDS,
    "αρσ επικινδυνοτ εγκαταλειμ κτιρι",
    "συμβασ παροχ καθαριοτ κτιριακ εγκαταστασ",
    "υπηρεσ καθαρισ χωρ κτιρι",
    "πλημμυρ φαινομ", "χλοοταπητ", "μουσει",
    "συστηματ πυρασφαλ", "αναβαθμισ συστηματ πυροσβεσ",
    "εγκαταστ συστηματ πυροσβεσ", "πυροσβεστηρ αυτοκινητ",
    "αδεσποτ ζω", "κλειστ γυμναστηρ",
    "εγκαταστ εξοπλισμ συστηματ ενεργητικ παθητικ πυροπροστασ",
    "χιονοπτ", "καθαρισ δημοτικ κοιμητηρ", "daniel", "ντανιελ",
    "προμηθ τοποθετ πυροσβεστικ φωλ",
    "πυροπροστασ κτιριακ εγκαταστασ",
)
ROOT_EXCLUDE_TOKEN_RULES = {tuple(term.split()) for term in ROOT_EXCLUDE_KEYWORDS}


def normalize_keyword_tokens(value: str) -> list[str]:
    if not isinstance(value, str):
        return []
    decomposed = unicodedata.normalize("NFD", value)
    normalized = "".join(ch for ch in decomposed if unicodedata.category(ch) != "Mn")
    normalized = normalized.casefold().replace("ς", "σ")
    return [token for token in re.split(r"\W+", normalized, flags=re.UNICODE) if token]


def text_matches_keyword_prefix(text: str, keyword: str) -> bool:
    """Match keyword tokens only at token starts, not as arbitrary substrings."""
    text_tokens = normalize_keyword_tokens(text)
    keyword_tokens = normalize_keyword_tokens(keyword)
    if not text_tokens or not keyword_tokens or len(keyword_tokens) > len(text_tokens):
        return False

    if tuple(keyword_tokens) in ROOT_EXCLUDE_TOKEN_RULES:
        for start, token in enumerate(text_tokens):
            if not token.startswith(keyword_tokens[0]):
                continue
            positions = {start}
            for root in keyword_tokens[1:]:
                positions = {
                    next_position
                    for position in positions
                    for next_position in range(position + 1, min(len(text_tokens), position + 4))
                    if text_tokens[next_position].startswith(root)
                }
                if not positions:
                    break
            if positions:
                return True
        return False

    last_start = len(text_tokens) - len(keyword_tokens)
    for start in range(last_start + 1):
        if all(
            text_tokens[start + offset].startswith(keyword_token)
            for offset, keyword_token in enumerate(keyword_tokens)
        ):
            return True
    return False


def text_has_requested_exclusion(value: str) -> bool:
    return any(text_matches_keyword_prefix(value, term) for term in REQUESTED_EXCLUDE_KEYWORDS)
