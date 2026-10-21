#!/usr/bin/env python3
"""Retired entrypoint for the removed building-specific exclusion rules."""


def main() -> None:
    raise SystemExit(
        "This script has been retired: building-specific exclusion rules were removed. "
        "No database connection was made and no database changes were performed."
    )


if __name__ == "__main__":
    main()
