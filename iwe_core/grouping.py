from __future__ import annotations

import io
import re
import logging

from PyPDF2 import PdfReader

from .config import CFG


logger = logging.getLogger(__name__)


def _canon_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


_DOC_NORMALISATIONS: list[tuple[str, str]] = [
    (r"^consolidated.*balance.*", "Consolidated Balance Sheet"),
    (r"^standalone.*balance.*", "Standalone Balance Sheet"),
    (r"\bbalance\s*sheet\b", "Standalone Balance Sheet"),
    (r"statement of assets and liabilities", "Standalone Balance Sheet"),
    (r"^consolidated.*(profit).*(loss)", "Consolidated Profit and Loss Statement"),
    (r"^standalone.*(profit).*(loss)", "Standalone Profit and Loss Statement"),
    (r"(statement of profit).*(loss)", "Standalone Profit and Loss Statement"),
    (r"\bprofit\s*and\s*loss\b", "Standalone Profit and Loss Statement"),
    (r"^consolidated.*cash.*flow", "Consolidated Cashflow"),
    (r"^standalone.*cash.*flow", "Standalone Cashflow"),
    (r"cash\s*flow", "Standalone Cashflow"),
]


def normalize_doc_type(label: str | None) -> str:
    s = _canon_text(label or "")
    if not s:
        return "Others"
    for pat, out in _DOC_NORMALISATIONS:
        if re.search(pat, s):
            return out
    return (label or "Others").strip().title()


def extract_page_texts_from_pdf_bytes(pdf_bytes: bytes) -> list[str]:
    texts: list[str] = []
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        for p in reader.pages:
            try:
                t = p.extract_text() or ""
            except Exception:
                t = ""
            texts.append(t)
    except Exception:
        pass
    return texts


def infer_doc_type_from_text(text: str) -> str | None:
    s = _canon_text(text)
    if not s:
        return None
    is_cons = "consolidated" in s
    is_stand = "standalone" in s and not is_cons
    base: str | None = None
    if ("statement of assets and liabilities" in s) or ("balance sheet" in s):
        base = "Balance Sheet"
    elif ("statement of profit and loss" in s) or ("profit before" in s) or ("revenue from operations" in s) or ("earnings per share" in s):
        base = "Profit and Loss Statement"
    elif (
        "cash flow statement" in s
        or "statement of cash flows" in s
        or ("cash flow from" in s)
        or (("operating activities" in s or "investing activities" in s or "financing activities" in s) and "cash flow" in s)
    ):
        base = "Cashflow"
    if not base:
        return None
    prefix = "Consolidated " if is_cons and not is_stand else ("Standalone " if is_stand else "")
    return f"{prefix}{base}".strip()


def build_groups(
    selected_pages: list[int],
    classification: list[dict],
    original_pdf_bytes: bytes,
    first_pass_results: list[dict] | None = None,
) -> dict[str, list[int]]:
    doc_by_page: dict[int, str] = {}
    for item in classification or []:
        sel_no = item.get("page_number")
        is_cont = str(item.get("is_continuation", "")).lower() == "true"
        dt_raw = (item.get("continuation_of") if is_cont else None) or item.get("doc_type")
        dt = normalize_doc_type(dt_raw)
        if isinstance(sel_no, int) and 1 <= sel_no <= len(selected_pages):
            orig = selected_pages[sel_no - 1]
            doc_by_page[orig] = dt

    inherit_on_cont = bool(CFG.get("grouping", {}).get("inherit_scope_on_continuation", True))
    cont_orig_pages: set[int] = set()
    if inherit_on_cont:
        for item in classification or []:
            sel_no = item.get("page_number")
            is_cont = str(item.get("is_continuation", "")).lower() == "true"
            if not is_cont or not isinstance(sel_no, int):
                continue
            if 1 <= sel_no <= len(selected_pages):
                cont_orig_pages.add(selected_pages[sel_no - 1])

    def _is_true(x):
        return str(x).strip().lower() in ("true", "1", "yes", "y", "on")

    has_two_orig_pages: set[int] = set()
    for item in classification or []:
        sel_no = item.get("page_number")
        if not isinstance(sel_no, int):
            continue
        has_two_flag = _is_true(item.get("has_two") or item.get("Has_multiple_sections") or "")
        if not has_two_flag:
            continue
        if 1 <= sel_no <= len(selected_pages):
            has_two_orig_pages.add(selected_pages[sel_no - 1])

    prevent_override_others = bool(CFG.get("grouping", {}).get("prevent_override_when_others", True))
    page_texts = extract_page_texts_from_pdf_bytes(original_pdf_bytes)
    disable_header_override = bool(CFG.get("grouping", {}).get("disable_header_override", False))
    for orig in selected_pages:
        inferred = infer_doc_type_from_text(page_texts[orig - 1] if 0 <= orig - 1 < len(page_texts) else "")
        if inferred:
            inferred = normalize_doc_type(inferred)
        if orig not in doc_by_page:
            doc_by_page[orig] = inferred or "Others"
        else:
            current_label = doc_by_page[orig]
            if (inherit_on_cont and orig in cont_orig_pages) or (orig in has_two_orig_pages) or (prevent_override_others and current_label == "Others"):
                pass
            else:
                if not disable_header_override:
                    current = _canon_text(current_label)
                    if inferred and _canon_text(inferred) not in (current,):
                        kinds = lambda s: ("cash" if "cash" in s else "pl" if "loss" in s or "profit" in s else "bs" if "balance" in s or "assets" in s else "other")
                        if kinds(current) != kinds(_canon_text(inferred)):
                            try:
                                logger.info("Header override @p%d: %s → %s", orig, current_label, inferred)
                            except Exception:
                                pass
                            doc_by_page[orig] = inferred

    pages_sorted = sorted(doc_by_page)
    for p in pages_sorted:
        if doc_by_page[p] == "Others":
            prev_dt = None
            for q in range(p - 1, 0, -1):
                if q in doc_by_page and doc_by_page[q] != "Others":
                    prev_dt = doc_by_page[q]
                    break
            next_dt = None
            for q in range(p + 1, len(page_texts) + 1):
                if q in doc_by_page and doc_by_page[q] != "Others":
                    next_dt = doc_by_page[q]
                    break
            if prev_dt and next_dt and prev_dt == next_dt:
                doc_by_page[p] = prev_dt

    groups: dict[str, list[int]] = {}
    for p in sorted(doc_by_page):
        dt = doc_by_page[p]
        if dt == "Others":
            continue
        groups.setdefault(dt, []).append(p)

    for item in classification or []:
        sel_no = item.get("page_number")
        if not isinstance(sel_no, int):
            continue
        if 1 <= sel_no <= len(selected_pages):
            orig = selected_pages[sel_no - 1]
        else:
            continue

        second_dt_raw = (
            item.get("second_doc_type")
            or item.get("Second_doc_type")
            or item.get("second_type")
            or item.get("secondDocType")
            or ""
        )
        second_dt = normalize_doc_type(second_dt_raw)
        if second_dt and second_dt != "Others":
            lst = groups.setdefault(second_dt, [])
            if orig not in lst:
                lst.append(orig)

    if first_pass_results:
        try:
            for orig in sorted(set(selected_pages)):
                if not (1 <= orig <= len(first_pass_results)):
                    continue
                fp = first_pass_results[orig - 1] or {}
                pdict = (fp.get("data", {}) or {}).get("parsedData", {}) or {}
                has_multi = str(pdict.get("Has_multiple_sections") or pdict.get("has_multiple_sections") or "").lower() == "true"
                secs = pdict.get("Sections") or pdict.get("sections") or []
                if has_multi and isinstance(secs, list):
                    for sec in secs:
                        sec_dt_raw = (sec or {}).get("sec_doc_type") or (sec or {}).get("doc_type") or ""
                        sec_dt = normalize_doc_type(sec_dt_raw)
                        if sec_dt and sec_dt != "Others":
                            lst = groups.setdefault(sec_dt, [])
                            if orig not in lst:
                                lst.append(orig)
        except Exception:
            pass

    for dt, lst in list(groups.items()):
        groups[dt] = sorted({p for p in lst})

    try:
        logger.info("Third-pass groups (original pages) → %s", {k: v for k, v in groups.items()})
        logger.info("Third-pass grouping → %s", {k: len(v) for k, v in groups.items()})
    except Exception:
        pass

    return groups


__all__ = [
    "build_groups",
    "normalize_doc_type",
    "extract_page_texts_from_pdf_bytes",
    "infer_doc_type_from_text",
]
