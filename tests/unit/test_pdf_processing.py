from types import SimpleNamespace
import sys

from app.services.content_analysis import maybe_extract_pdf_text


def test_maybe_extract_pdf_text_uses_first_sufficient_parser(monkeypatch):
    monkeypatch.setattr(
        "app.services.content_analysis._extract_pdf_text_with_pymupdf",
        lambda _content: "Page 1:\nOpen House on October 7 at 6:00 PM\n" * 3,
    )
    monkeypatch.setattr(
        "app.services.content_analysis._extract_pdf_text_with_pdfplumber",
        lambda _content: (_ for _ in ()).throw(AssertionError("should not reach fallback parser")),
    )
    monkeypatch.setattr(
        "app.services.content_analysis._ocr_pdf_with_openai",
        lambda _content: (_ for _ in ()).throw(AssertionError("should not reach OCR")),
    )

    result = maybe_extract_pdf_text(b"%PDF-1.4 test")
    assert "Open House" in result


def test_maybe_extract_pdf_text_falls_back_to_ocr_when_parser_output_is_weak(monkeypatch):
    monkeypatch.setattr("app.services.content_analysis._extract_pdf_text_with_pymupdf", lambda _content: "Hi")
    monkeypatch.setattr("app.services.content_analysis._extract_pdf_text_with_pdfplumber", lambda _content: "")
    monkeypatch.setattr("app.services.content_analysis._extract_pdf_text_with_pypdf", lambda _content: "Page 1")
    monkeypatch.setattr("app.services.content_analysis._extract_pdf_text_with_pypdf2", lambda _content: "")
    monkeypatch.setattr("app.services.content_analysis._extract_pdf_text_with_pdftotext", lambda _content: "")
    monkeypatch.setattr(
        "app.services.content_analysis._ocr_pdf_with_openai",
        lambda _content: "Page 1:\nOctober 7 Open House\nPage 2:\nOctober 10 PA Day",
    )

    result = maybe_extract_pdf_text(b"%PDF-1.4 test")
    assert "October 10 PA Day" in result


def test_ocr_pdf_with_openai_uses_gpt5_compatible_payload(monkeypatch):
    import app.config as config_module
    import app.services.content_analysis as content_analysis_module

    captured = {}

    class _FakePixmap:
        def tobytes(self, fmt: str) -> bytes:
            assert fmt == "png"
            return b"fake-png"

    class _FakePage:
        def get_pixmap(self, matrix=None):
            return _FakePixmap()

    class _FakeDocument:
        page_count = 1

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def load_page(self, page_index: int):
            assert page_index == 0
            return _FakePage()

    class _FakeFitzModule:
        @staticmethod
        def Matrix(x: float, y: float):
            return (x, y)

        @staticmethod
        def open(stream=None, filetype=None):
            assert stream == b"%PDF-1.4 test"
            assert filetype == "pdf"
            return _FakeDocument()

    class _MockResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"choices": [{"message": {"content": "October 7 Open House"}}]}

    class _CaptureClient:
        def __init__(self, timeout: int):
            self.timeout = timeout

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def post(self, url, headers=None, json=None):  # noqa: A002
            captured["url"] = url
            captured["headers"] = headers
            captured["payload"] = json
            return _MockResponse()

    monkeypatch.setitem(sys.modules, "fitz", _FakeFitzModule())
    monkeypatch.setattr(content_analysis_module.httpx, "Client", _CaptureClient)
    monkeypatch.setattr(
        config_module,
        "settings",
        SimpleNamespace(
            openai_api_key="test-key",
            openai_model="gpt-5-mini-2025-08-07",
            openai_timeout_sec=20,
            openai_base_url="https://api.openai.com/v1",
        ),
    )

    result = content_analysis_module._ocr_pdf_with_openai(b"%PDF-1.4 test")

    assert result == "Page 1:\nOctober 7 Open House"
    assert captured["url"].endswith("/chat/completions")
    assert captured["headers"]["Authorization"] == "Bearer test-key"
    assert "temperature" not in captured["payload"]
    assert "<output_contract>" in captured["payload"]["messages"][0]["content"][0]["text"]
    assert (
        captured["payload"]["messages"][0]["content"][1]["image_url"]["detail"]
        == "high"
    )
