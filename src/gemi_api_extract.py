import argparse
import re
import time
from typing import Any

import requests


SEARCH_URL = "https://publicity.businessportal.gr/api/search"
DETAILS_URL = "https://publicity.businessportal.gr/api/company/details"

DEFAULT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,el;q=0.8",
    "Content-Type": "application/json",
    "Origin": "https://publicity.businessportal.gr",
    "Referer": "https://publicity.businessportal.gr/",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
}


def normalize_afm(afm: str) -> str:
    return "".join(re.findall(r"\d", str(afm or "")))


def build_search_payload(company_name: str, language: str = "el") -> dict[str, Any]:
    return {
        "dataToBeSent": {
            "inputField": company_name,
            "city": None,
            "postcode": None,
            "legalType": [],
            "status": [],
            "suspension": [],
            "category": [],
            "specialCharacteristics": [],
            "employeeNumber": [],
            "armodiaGEMI": [],
            "kad": [],
            "recommendationDateFrom": None,
            "recommendationDateTo": None,
            "closingDateFrom": None,
            "closingDateTo": None,
            "alterationDateFrom": None,
            "alterationDateTo": None,
            "person": [],
            "personrecommendationDateFrom": None,
            "personrecommendationDateTo": None,
            "radioValue": "all",
            "places": [],
        },
        "token": None,
        "language": language,
    }


def search_gemi(company_name: str, language: str = "el") -> dict[str, Any]:
    response = requests.post(
        SEARCH_URL,
        headers=DEFAULT_HEADERS,
        json=build_search_payload(company_name, language=language),
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def get_company_hits(company_name: str, language: str = "el") -> list[dict[str, Any]]:
    data = search_gemi(company_name, language=language)
    return data.get("company", {}).get("hits", [])


def get_company_details_by_gemi(
    gemi_number: str, language: str = "el"
) -> dict[str, Any]:
    headers = {
        **DEFAULT_HEADERS,
        "Referer": f"https://publicity.businessportal.gr/company/{gemi_number}",
    }
    payload = {"query": {"arGEMI": gemi_number}, "token": None, "language": language}
    response = requests.post(
        DETAILS_URL,
        headers=headers,
        json=payload,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def get_company_details_by_afm(
    afm: str, language: str = "el", delay_seconds: float = 2
) -> dict[str, Any]:
    normalized_afm = normalize_afm(afm)
    hits = get_company_hits(normalized_afm, language=language)
    if not hits:
        raise LookupError(f"No company found for AFM {normalized_afm}")

    exact_hit = next(
        (hit for hit in hits if normalize_afm(hit.get("afm")) == normalized_afm),
        None,
    )
    if exact_hit is None:
        raise LookupError(f"No exact AFM match found for {normalized_afm}")

    gemi_number = exact_hit.get("gemiNumber")
    if not gemi_number:
        raise LookupError(f"Search hit for AFM {normalized_afm} does not include gemiNumber")

    time.sleep(delay_seconds)

    return {
        "search_hit": exact_hit,
        "details": get_company_details_by_gemi(gemi_number, language=language),
    }


def get_company_url_by_afm(afm: str, language: str = "el") -> str:
    gemi_number = get_gemi_number_by_afm(afm, language=language)
    return f"https://publicity.businessportal.gr/company/{gemi_number}"


def get_gemi_number_by_afm(afm: str, language: str = "el") -> str:
    normalized_afm = normalize_afm(afm)
    hits = get_company_hits(normalized_afm, language=language)
    exact_hit = next(
        (hit for hit in hits if normalize_afm(hit.get("afm")) == normalized_afm),
        None,
    )
    if exact_hit is None:
        raise LookupError(f"No exact AFM match found for {normalized_afm}")

    gemi_number = exact_hit.get("gemiNumber")
    if not gemi_number:
        raise LookupError(f"Search hit for AFM {normalized_afm} does not include gemiNumber")

    return str(gemi_number).strip()


def extract_company_profile(result: dict[str, Any]) -> dict[str, Any]:
    search_hit = result.get("search_hit", {})
    payload = result["details"]["companyInfo"]["payload"]
    company = payload["company"]
    titles = payload.get("titles", [])
    management_persons = payload.get("managementPersons", [])
    representation = payload.get("representation", [])
    kad_data = payload.get("kadData", [])

    current_management = [
        {
            "first_name": person.get("firstName"),
            "last_name": person.get("lastName"),
            "afm": person.get("afm"),
            "capacity": person.get("capacityDescr"),
            "date_from": person.get("dateFrom"),
            "date_to": person.get("dateTo"),
            "active": person.get("active"),
            "shared": person.get("shared"),
            "non_shared": person.get("nonShared"),
        }
        for person in management_persons
        if person.get("active") == 1 and person.get("tableName") != "member"
    ]

    current_ownership = [
        {
            "first_name": person.get("firstName"),
            "last_name": person.get("lastName"),
            "afm": person.get("afm"),
            "capacity": person.get("capacityDescr"),
            "percentage": person.get("percentage"),
            "date_from": person.get("dateFrom"),
            "date_to": person.get("dateTo"),
            "active": person.get("active"),
        }
        for person in management_persons
        if person.get("active") == 1 and person.get("tableName") == "member"
    ]

    current_representation = [
        {
            "name": person.get("name"),
            "afm": person.get("afm"),
            "shared": person.get("shared"),
            "non_shared": person.get("nonShared"),
            "active": person.get("active"),
            "active_from": person.get("activeFrom"),
            "active_to": person.get("activeTo"),
        }
        for person in representation
        if person.get("active") == 1
    ]

    current_titles = [
        {
            "title": item.get("title"),
            "title_i18n": [x.get("title") for x in item.get("titleI18n", [])],
            "is_enable": item.get("isEnable"),
        }
        for item in titles
        if item.get("isEnable") == 1
    ]

    inactive_titles = [
        {
            "title": item.get("title"),
            "title_i18n": [x.get("title") for x in item.get("titleI18n", [])],
            "is_enable": item.get("isEnable"),
        }
        for item in titles
        if item.get("isEnable") != 1
    ]

    main_kads = [
        {"kad": item.get("kad"), "description": item.get("descr")}
        for item in kad_data
        if item.get("activities") == "Κύρια"
    ]

    secondary_kads = [
        {"kad": item.get("kad"), "description": item.get("descr")}
        for item in kad_data
        if item.get("activities") != "Κύρια"
    ]

    return {
        "core_company_identity": {
            "name": company.get("name"),
            "name_i18n": company.get("namei18n"),
            "afm": company.get("afm"),
            "gemi_number": company.get("id", "").lstrip("0") or company.get("id"),
            "legal_type": company.get("legalType", {}).get("desc"),
        },
        "name_variations": {
            "current_titles": current_titles,
            "historical_or_disabled_titles": inactive_titles,
        },
        "address": {
            "street": company.get("company_street"),
            "street_number": company.get("company_street_number"),
            "city": company.get("company_city"),
            "municipality": company.get("company_municipality"),
            "region": company.get("company_region"),
            "zip_code": company.get("company_zip_code"),
            "full_search_address": search_hit.get("addressCity"),
        },
        "dates": {
            "date_start": company.get("dateStart"),
            "date_gemi_registered": company.get("dateGemiRegistered"),
        },
        "management": {"current": current_management},
        "ownership": {"current_members": current_ownership},
        "representation": {"current": current_representation},
        "contact": {
            "telephone": payload.get("moreInfo", {}).get("telephone"),
            "email": payload.get("moreInfo", {}).get("email"),
            "website": company.get("companyWebsite"),
            "eshop": company.get("companyEshop"),
        },
        "status": {
            "current_status": company.get("companyStatus", {}).get("status"),
            "is_suspended": company.get("issuspended"),
            "company_status_history": [
                {"date": item.get("dt"), "status": item.get("descr")}
                for item in payload.get("companyStatusHistoryInfo", [])
            ],
            "corporate_status_history": [
                {"date": item.get("dt"), "legal_form": item.get("descr")}
                for item in payload.get("corporateStatusHistoryInfo", [])
            ],
        },
    }


def get_company_profile_by_afm(
    afm: str, language: str = "el", delay_seconds: float = 2
) -> dict[str, Any]:
    return extract_company_profile(
        get_company_details_by_afm(
            afm,
            language=language,
            delay_seconds=delay_seconds,
        )
    )


def get_company_profile_by_gemi(gemi_number: str, language: str = "el") -> dict[str, Any]:
    details = get_company_details_by_gemi(gemi_number, language=language)
    result = {"search_hit": {"gemiNumber": gemi_number, "title": []}, "details": details}
    return extract_company_profile(result)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Query the GEMI search endpoint extracted from the workshop notebook."
    )
    parser.add_argument(
        "query",
        nargs="?",
        help="Company name or AFM to search for",
    )
    parser.add_argument(
        "--language",
        default="el",
        choices=["el", "en"],
        help="Response language sent in the request payload",
    )
    parser.add_argument(
        "--details-by-gemi",
        metavar="GEMI_NUMBER",
        help="Fetch full company details using a GEMI number",
    )
    parser.add_argument(
        "--details-by-afm",
        metavar="AFM",
        help="Search by AFM and then fetch the full company details",
    )
    args = parser.parse_args()

    if args.details_by_gemi:
        print(get_company_details_by_gemi(args.details_by_gemi, language=args.language))
        return

    if args.details_by_afm:
        print(get_company_details_by_afm(args.details_by_afm, language=args.language))
        return

    if not args.query:
        parser.error("Provide a query, --details-by-gemi, or --details-by-afm.")

    hits = get_company_hits(args.query, language=args.language)
    for hit in hits[:10]:
        print(
            {
                "name": hit.get("name"),
                "gemiNumber": hit.get("gemiNumber"),
                "afm": hit.get("afm"),
                "status": hit.get("status"),
                "legalType": hit.get("legalType"),
            }
        )


if __name__ == "__main__":
    main()
