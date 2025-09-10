from __future__ import annotations

import io
from typing import Iterable, BinaryIO

try:
    from PyPDF2 import PdfReader, PdfWriter
except Exception:  # pragma: no cover - defensive import
    PdfReader = None  # type: ignore
    PdfWriter = None  # type: ignore


def build_pdf_from_pages(pdf_bytes: bytes, page_numbers: Iterable[int]) -> bytes:
    """
    Return new PDF bytes containing only the given 1-based page numbers
    from the source `pdf_bytes`. Ignores out-of-range indices.
    """
    if PdfReader is None or PdfWriter is None:
        raise RuntimeError("PyPDF2 is required for PDF operations")

    reader = PdfReader(io.BytesIO(pdf_bytes))
    writer = PdfWriter()

    pages = list(page_numbers)
    total = len(reader.pages)
    for pno in pages:
        # Convert to 0-based and guard bounds
        idx = int(pno) - 1
        if 0 <= idx < total:
            writer.add_page(reader.pages[idx])

    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


def get_page_count_from_bytes(pdf_bytes: bytes) -> int:
    """Return page count for a PDF given as bytes."""
    if PdfReader is None:
        raise RuntimeError("PyPDF2 is required for PDF operations")
    reader = PdfReader(io.BytesIO(pdf_bytes))
    return len(reader.pages)


def get_page_count_from_stream(stream: BinaryIO) -> int:
    """
    Return page count for a file-like object. Stream position is not guaranteed
    to be preserved.
    """
    if PdfReader is None:
        raise RuntimeError("PyPDF2 is required for PDF operations")
    pos = None
    try:
        pos = stream.tell()
    except Exception:
        pos = None
    try:
        reader = PdfReader(stream)
        return len(reader.pages)
    finally:
        try:
            if pos is not None:
                stream.seek(pos)
        except Exception:
            pass
