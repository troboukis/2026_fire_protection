from __future__ import annotations

from types import SimpleNamespace

from src.locate_work import Document


def make_response(status_code: int, *, content: bytes = b"", headers: dict | None = None, text: str = ""):
    def raise_for_status():
        if status_code >= 400:
            raise RuntimeError(f"{status_code} error")

    return SimpleNamespace(
        status_code=status_code,
        content=content,
        headers=headers or {},
        text=text,
        raise_for_status=raise_for_status,
    )


def test_get_document_retries_on_429_like_fetch_kimdis_procurements(monkeypatch):
    doc = Document.__new__(Document)
    doc.ref_number = "26SYMV000000000"
    doc.debug = False
    doc.doc = None

    responses = [
        make_response(429, headers={"Retry-After": "3"}),
        make_response(200, content=b"%PDF-1.4", headers={"Content-Type": "application/pdf"}),
    ]
    sleeps: list[int] = []

    def fake_get(url, timeout):
        assert url.endswith(doc.ref_number)
        assert timeout == 60
        return responses.pop(0)

    monkeypatch.setattr("src.locate_work.requests.get", fake_get)
    monkeypatch.setattr("src.locate_work.time.sleep", sleeps.append)

    result = doc.getDocument()

    assert result == b"%PDF-1.4"
    assert doc.doc == b"%PDF-1.4"
    assert sleeps == [3]
