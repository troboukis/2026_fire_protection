from __future__ import annotations

from src.map_copernicus_to_municipalities import resolve_database_url


def test_resolve_database_url_accepts_prefixed_env_value(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "DATABASE_URL=postgresql://user:pass@localhost:5432/postgres")
    assert resolve_database_url(None) == "postgresql://user:pass@localhost:5432/postgres"
