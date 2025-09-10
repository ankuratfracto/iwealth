#!/usr/bin/env python3
"""
Lightweight smoke test for iwe_core.pdf_ops.

Generates a tiny 3-page PDF in memory (via reportlab), then verifies:
 - get_page_count_from_bytes → 3
 - build_pdf_from_pages([2])  → 1 page
 - build_pdf_from_pages([1,3]) → 2 pages

Run: python scripts/smoke_pdf_ops.py
"""
from __future__ import annotations

import io
from iwe_core.pdf_ops import build_pdf_from_pages, get_page_count_from_bytes

from reportlab.pdfgen import canvas


def _make_sample_pdf(n_pages: int = 3) -> bytes:
    buf = io.BytesIO()
    c = canvas.Canvas(buf)
    for i in range(1, n_pages + 1):
        c.drawString(72, 720, f"Sample Page {i}")
        c.showPage()
    c.save()
    buf.seek(0)
    return buf.getvalue()


def main():
    pdf = _make_sample_pdf(3)
    assert get_page_count_from_bytes(pdf) == 3, "Expected 3 pages"

    only2 = build_pdf_from_pages(pdf, [2])
    assert get_page_count_from_bytes(only2) == 1, "Expected 1 page for [2]"

    one_three = build_pdf_from_pages(pdf, [1, 3])
    assert get_page_count_from_bytes(one_three) == 2, "Expected 2 pages for [1,3]"

    print("OK: pdf_ops basic functions working as expected.")


if __name__ == "__main__":
    main()

