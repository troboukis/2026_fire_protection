"""Exact ADA links declared in KIMDIS contract columns.

Each column may contain several identifiers or other text. Extract individual
ADA tokens and match whole identifiers, never substrings of another ADA.
"""

CONTRACT_DIAVGEIA_LINKS_SELECT = """
    SELECT DISTINCT d.id AS diavgeia_id, p.id AS procurement_id
    FROM public.procurement p
    CROSS JOIN LATERAL regexp_matches(
        UPPER(CONCAT_WS(' | ',
            p.contract_related_ada,
            p.decision_related_ada,
            p.diavgeia_ada
        )),
        '[A-ZΑ-Ω0-9]+-[A-ZΑ-Ω0-9]+',
        'g'
    ) AS refs(ada_parts)
    JOIN public.diavgeia d ON d.ada = refs.ada_parts[1]
"""
