"""One keyword exclusion policy for KIMDIS and Diavgeia ingestion."""

import re
import unicodedata


MAX_INTERVENING_WORDS = 2


# Common exclusions for both sources, without equivalent case/accent spellings.
# Each value is the maximum intervening words between consecutive word prefixes.
# 0 requires adjacent words; 2 preserves the original root-matching allowance.
DEFAULT_EXCLUDE_KEYWORDS: dict[str, int] = {
    "σχολει": 2,
    "σχολικ": 2,
    "πανεπιστημ": 2,
    "γηπεδ": 2,
    "πιστοποιητικ πυρασφαλ": 2,
    "κηποτεχνικ συντηρησ": 2,
    "συντηρησ κηπ": 2,
    "οακα": 2,
    "παραγωγ φυτ": 2,
    "φυτωριακ εργασ": 2,
    "δασικ φυτωρι": 2,
    "καυσιμ θερμανσ": 2,
    "κολυμβητηρ": 2,
    "server room": 2,
    "προσχολικ": 2,
    "διασχολικ": 2,
    "γυμνασ": 2,
    "λυκει": 2,
    "αρσ επικινδυνοτ εγκαταλειμ κτιρι": 2,
    "συμβασ παροχ καθαριοτ κτιριακ εγκαταστασ": 2,
    "υπηρεσ καθαρισ χωρ κτιρι": 2,
    "πλημμυρ φαινομ": 2,
    "χλοοταπητ": 2,
    "μουσει": 2,
    "συστηματ πυρασφαλ": 2,
    "αναβαθμισ συστηματ πυροσβεσ": 2,
    "εγκαταστ συστηματ πυροσβεσ": 2,
    "πυροσβεστηρ αυτοκινητ": 2,
    "αδεσποτ ζω": 2,
    "κλειστ γυμναστηρ": 2,
    "εγκαταστ εξοπλισμ συστηματ ενεργητικ παθητικ πυροπροστασ": 2,
    "χιονοπτ": 2,
    "καθαρισ δημοτικ κοιμητηρ": 2,
    "daniel": 2,
    "ντανιελ": 2,
    "προμηθ τοποθετ πυροσβεστικ φωλ": 2,
    "πυροπροστασ κτιριακ εγκαταστασ": 2,
    # Former KIMDIS additions: no intervening words.
    "νοσοκομειο": 0,
    "σχολ": 0,
    "παντειο": 0,
    "επιθεωρηση εργασιασ": 0,
    "αναγομ": 0,
    "αντλητικ": 0,
    "επαλ": 0,
    "βρεφο": 0,
    "παιδικ": 0,
    "συντηρηση χωρων πρασινου": 0,
    "sports4all": 0,
    "κινηματογραφο": 0,
    "aqua montis": 0,
    "αναδειξη υφισταμενων διαδρομων": 0,
    "αναδειξη υφοσταμενων διαδρομων": 0,
}


def normalize_keyword_tokens(value: object) -> list[str]:
    """Tokenize text without case, accents, punctuation or final-sigma differences."""
    if not isinstance(value, str):
        return []
    decomposed = unicodedata.normalize("NFD", value)
    normalized = "".join(ch for ch in decomposed if unicodedata.category(ch) != "Mn")
    normalized = normalized.casefold().replace("ς", "σ")
    return [token for token in re.split(r"\W+", normalized, flags=re.UNICODE) if token]


def keyword_max_intervening_words(keyword: str) -> int:
    """Read the phrase's gap allowance from the single common policy."""
    normalized = " ".join(normalize_keyword_tokens(keyword))
    return DEFAULT_EXCLUDE_KEYWORDS.get(normalized, MAX_INTERVENING_WORDS)


def _tokens_match_keyword_prefix(
    text_tokens: list[str], keyword_tokens: list[str], max_intervening_words: int
) -> bool:
    if not text_tokens or not keyword_tokens or len(keyword_tokens) > len(text_tokens):
        return False
    for start, token in enumerate(text_tokens):
        if not token.startswith(keyword_tokens[0]):
            continue
        positions = {start}
        for root in keyword_tokens[1:]:
            positions = {
                next_position
                for position in positions
                for next_position in range(
                    position + 1,
                    min(len(text_tokens), position + max_intervening_words + 2),
                )
                if text_tokens[next_position].startswith(root)
            }
            if not positions:
                break
        if positions:
            return True
    return False


def text_matches_keyword_prefix(text: object, keyword: str) -> bool:
    """Match ordered token prefixes using the phrase's configured gap allowance."""
    return _tokens_match_keyword_prefix(
        normalize_keyword_tokens(text),
        normalize_keyword_tokens(keyword),
        keyword_max_intervening_words(keyword),
    )


def matching_exclusion_keywords(*values: object) -> tuple[str, ...]:
    """Return matching terms in list order; phrases never span separate fields."""
    tokenized_values = [normalize_keyword_tokens(value) for value in values]
    return tuple(
        keyword
        for keyword, max_intervening_words in DEFAULT_EXCLUDE_KEYWORDS.items()
        if any(
            _tokens_match_keyword_prefix(tokens, normalize_keyword_tokens(keyword), max_intervening_words)
            for tokens in tokenized_values
        )
    )


def text_has_exclusion(*values: object) -> bool:
    """Whether any supplied field matches the common keyword exclusion list."""
    return bool(matching_exclusion_keywords(*values))
