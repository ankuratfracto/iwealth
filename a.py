"""Legacy OCR/Excel script (superseded by `iwe_core` pipeline).

Contains CLI helpers and PDF utilities kept for backward compatibility and
ad‑hoc workflows. New code should prefer the orchestrated entry points in
`iwe_core.pipeline` and related modules.
"""

from __future__ import annotations
import re
from pathlib import Path
import yaml, os
from typing import Any
import iwe_core.excel_ops as excel_ops
from iwe_core import json_ops
from iwe_core.config import CFG
from iwe_core.ocr_client import (
    call_fracto as call_fracto,
    call_fracto_parallel as call_fracto_parallel,
    resolve_api_key as _resolve_api_key,
)
from iwe_core.selection import (
    _is_truthy_val,
    _json_get_first,
    _json_any_truthy,
    _schema_paths,
    _second_pass_container,
    _second_pass_field,
)
from iwe_core.grouping import (
    _canon_text,
    extract_page_texts_from_pdf_bytes,
)
from iwe_core.utils import (
    company_type_from_token,
    is_true_flag,
    format_ranges,
)
from iwe_core.json_ops import doc_type_from_payload as _doc_type_from_payload
from iwe_core.debug_utils import (
    dprint,
    debug_enabled,
)

# Global cache used by Excel header renaming and diagnostics
PERIOD_LABELS_BY_DOC: dict[str, dict] = {}

# Process one page per chunk; use config defaults
CHUNK_SIZE_PAGES = int(CFG.get("concurrency", {}).get("chunk_size_pages", 1))
MAX_PARALLEL     = int(CFG.get("concurrency", {}).get("max_parallel", 9))
MIN_TAIL_COMBINE = int(CFG.get("concurrency", {}).get("min_tail_combine", 1))

# Splitting logic is provided by iwe_core.ocr_client; no local copy needed

 
#!/usr/bin/env python
"""
fracto_page_ocr.py
──────────────────
Split a PDF page-by-page and pipe each page through Fracto Smart-OCR.
"""

import io
# import os  # already imported above
# use top-level sys import
import json
import time
from typing import Dict
import logging
# from pathlib import Path  # already imported above
 

# from openpyxl.styles import Font, Alignment, PatternFill  # no longer used here

from reportlab.pdfgen import canvas

 

# DataFrame reordering and sanitization now handled in excel_ops normalization


"""Runtime patch helpers removed. Use excel_ops normalization for DF sanitization."""


def stamp_job_number(src_bytes: bytes, job_no: str, margin: int = 20) -> bytes:
    """
    Return new PDF bytes with an extra *margin* (pt) added to the top
    of every page, then stamps 'Job Number: <job_no>' inside that space.

    This ensures the stamp never covers the original page content.
    """
    if not job_no:
        return src_bytes

    from PyPDF2 import PdfReader, PdfWriter, Transformation, PageObject

    base_reader = PdfReader(io.BytesIO(src_bytes))
    writer      = PdfWriter()

    for orig_page in base_reader.pages:
        w = float(orig_page.mediabox.width)
        h = float(orig_page.mediabox.height)

        # 1️⃣  Create a new blank page taller by *margin*
        new_page = PageObject.create_blank_page(None, w, h + margin)

        # 2️⃣  Shift original page content down by `margin`
        orig_page.add_transformation(Transformation().translate(tx=0, ty=-margin))
        new_page.merge_page(orig_page)

        # 3️⃣  Create text overlay the same enlarged size
        overlay_buf = io.BytesIO()
        c = canvas.Canvas(overlay_buf, pagesize=(w, h + margin))
        c.setFont("Helvetica-Bold", 10)
        c.drawString(40, h + margin - 15, f"Job Number: {job_no}")
        c.save()
        overlay_buf.seek(0)

        overlay_reader = PdfReader(overlay_buf)
        new_page.merge_page(overlay_reader.pages[0])

        # 4️⃣  Add to writer
        writer.add_page(new_page)

    out_buf = io.BytesIO()
    writer.write(out_buf)
    return out_buf.getvalue()

# ─── CONFIG (from config.yaml, env can override api_key only) ───────────
FRACTO_ENDPOINT = CFG.get("api", {}).get("endpoint", "https://prod-ml.fracto.tech/upload-file-smart-ocr")
API_KEY_ENV     = CFG.get("api", {}).get("api_key_env", "FRACTO_API_KEY")
API_KEY         = os.getenv(API_KEY_ENV, "")
QR_RANGE_ENABLE = bool((CFG.get("api", {}).get("qr_range", {}) or {}).get("enable", False))
QR_RANGE_VALUE  = str((CFG.get("api", {}).get("qr_range", {}) or {}).get("value", "")).strip()
VALIDATION_SUM_ENABLE = bool(((CFG.get("validation", {}) or {}).get("checks", {}) or {}).get("balance_sheet", {}).get("sum_subitems", {}).get("enable", True))
VALIDATION_SUM_TOL_PCT = float(((CFG.get("validation", {}) or {}).get("checks", {}) or {}).get("balance_sheet", {}).get("sum_subitems", {}).get("tolerance_pct", 0.001))
VALIDATION_SUM_ABS_MIN = float(((CFG.get("validation", {}) or {}).get("checks", {}) or {}).get("balance_sheet", {}).get("sum_subitems", {}).get("abs_min", 1.0))

# Fine-grained validation toggles (with sensible defaults)
_VAL_CFG_BS = (((CFG.get("validation", {}) or {}).get("checks", {}) or {}).get("balance_sheet", {}) or {})
VAL_DECLARED_COMPONENTS = bool(_VAL_CFG_BS.get("declared_components", True))
VAL_CHILDREN_WITHOUT_COMPONENTS = bool(_VAL_CFG_BS.get("children_without_components", True))
VAL_SECTION_CHECKS = bool(_VAL_CFG_BS.get("section_checks", True))
VAL_SECTION_EQUALITY = bool(_VAL_CFG_BS.get("section_equality", True))
VAL_BLOCK_FALLBACK = bool(_VAL_CFG_BS.get("contiguous_block_fallback", True))
VAL_COMPOSED_AND_GRAND = bool(_VAL_CFG_BS.get("composed_and_grand", True))
INCLUDE_VALIDATION_SHEET = bool(((CFG.get("export", {}) or {}).get("statements_workbook", {}) or {}).get("include_validation_sheet", True))
VALIDATION_SHEET_NAME = str(((CFG.get("export", {}) or {}).get("statements_workbook", {}) or {}).get("validation_sheet_name", "Validation"))
VALIDATION_PROFILES = (CFG.get("validation", {}) or {}).get("profiles", {}) or {}

# Pass defaults
PARSER_APP_ID        = CFG.get("passes", {}).get("first",  {}).get("parser_app", "")
MODEL_ID             = CFG.get("passes", {}).get("first",  {}).get("model", "tv7")
EXTRA_ACCURACY_FIRST = str(CFG.get("passes", {}).get("first",  {}).get("extra_accuracy", False)).lower()

SECOND_PARSER_APP_ID = CFG.get("passes", {}).get("second", {}).get("parser_app", "")
SECOND_MODEL_ID      = CFG.get("passes", {}).get("second", {}).get("model", MODEL_ID)
EXTRA_ACCURACY_SECOND= str(CFG.get("passes", {}).get("second", {}).get("extra_accuracy", False)).lower()


THIRD_PARSER_APP_ID  = CFG.get("passes", {}).get("third",  {}).get("defaults", {}).get("parser_app", "")
THIRD_MODEL_ID       = CFG.get("passes", {}).get("third",  {}).get("defaults", {}).get("model", MODEL_ID)
EXTRA_ACCURACY_THIRD = str(CFG.get("passes", {}).get("third",  {}).get("defaults", {}).get("extra_accuracy", True)).lower()
SECOND_COMBINE_PAGES = bool(CFG.get("passes", {}).get("second", {}).get("combine_pages", True))
THIRD_COMBINE_PAGES  = bool(CFG.get("passes", {}).get("third",  {}).get("combine_pages", True))

# API key & timeout (env first, then config)
API_KEY_CFG     = CFG.get("api", {}).get("api_key", "")
API_TIMEOUT_SEC = int(CFG.get("api", {}).get("timeout_seconds", 600))

# Use iwe_core.ocr_client.resolve_api_key

# Selection (first pass → second pass)
SELECTION_USE_HAS_TABLE     = bool(CFG.get("passes", {}).get("first", {}).get("selection", {}).get("use_has_table", True))
HAS_TABLE_FIELD             = CFG.get("passes", {}).get("first", {}).get("selection", {}).get("has_table_field", "has_table")
SELECTION_EXPAND_NEIGHBORS  = int(CFG.get("passes", {}).get("first", {}).get("selection", {}).get("neighbor_radius", 0))

# Second-pass artifacts
SAVE_SELECTED_JSON          = bool(CFG.get("passes", {}).get("second", {}).get("save_selected_json", True))
SELECTED_JSON_NAME_TMPL     = CFG.get("passes", {}).get("second", {}).get("selected_json_name", "{stem}_selected_ocr.json")
SELECTED_PDF_NAME_TMPL      = CFG.get("passes", {}).get("second", {}).get("selected_pdf_name", "{stem}_selected.pdf")

# Export knobs
EXPORT_INCLUDE_ROUTING_SUMMARY = bool(CFG.get("export", {}).get("statements_workbook", {}).get("include_routing_summary", True))

# Routing via config.yaml (supports company_type later; default to corporate mapping)
_ROUTING_CFG = CFG.get("routing", {}) or {}
_ROUTING_COMPANY_DEFAULT = str(CFG.get("company_type_prior", {}).get("default", "corporate")).lower()
_ROUTING_FALLBACK_ORDER = _ROUTING_CFG.get("fallback_order", ["company_type_and_doc_type","corporate_and_doc_type","third_defaults"])
_ROUTING_ALLOWED_PARSERS = set((_ROUTING_CFG.get("allowed_parsers") or []) or [])
_ROUTING_BLOCKED_PARSERS = set((_ROUTING_CFG.get("blocked_parsers") or []) or [])
_ROUTING_SKIP_ON_DISABLED = bool(_ROUTING_CFG.get("skip_on_disabled", False))

# ─── Timing helpers ───────────────────────────────────────────────────────
def _fmt_ts(ts: float) -> str:
    """Format epoch seconds into a human-readable timestamp honoring logging.utc."""
    try:
        use_utc = bool((CFG.get("logging") or {}).get("utc", False))
    except Exception:
        use_utc = False
    t = time.gmtime(ts) if use_utc else time.localtime(ts)
    return time.strftime("%Y-%m-%d %H:%M:%S", t)

# ─── Generic JSON access & criteria helpers (config‑driven) ─────────────
TRUTHY_SET = {str(x).strip().lower() for x in (CFG.get("truthy_values") or ["true","1","yes","y","on"])}

def _select_by_criteria(res: dict) -> bool:
    """Evaluate passes.first.selection.criteria over a single first-pass result."""
    sel_cfg = (CFG.get("passes", {}).get("first", {}).get("selection", {}) or {})
    crit = sel_cfg.get("criteria") or {}
    rules = crit.get("rules") or []
    if not rules:
        return False
    mode = str(crit.get("combine", "any")).lower()
    outcomes = []
    # Precompute common contexts
    ctx_root = res or {}
    ctx_data = (ctx_root.get("data", {}) or {})
    ctx_pd   = (ctx_data.get("parsedData", {}) or {})
    for rule in rules:
        # Resolve candidate paths
        paths = []
        if "alias" in rule:
            paths = _schema_paths(str(rule["alias"]))
        if not paths and "paths" in rule:
            paths = [str(p) for p in (rule.get("paths") or [])]
        if not paths and "path" in rule:
            paths = [str(rule.get("path"))]
        op = str(rule.get("op", "truthy")).lower()
        # Extract values for all paths from multiple contexts
        vals = []
        for p in paths:
            tried = set()
            # try as-is on parsedData
            key = ("pd", p)
            if key not in tried:
                tried.add(key)
                v = _json_get_first(ctx_pd, p)
                if isinstance(v, list):
                    vals.extend(v)
                elif v is not None:
                    vals.append(v)
            # try as-is on data
            key = ("data", p)
            if key not in tried:
                tried.add(key)
                v = _json_get_first(ctx_data, p)
                if isinstance(v, list):
                    vals.extend(v)
                elif v is not None:
                    vals.append(v)
            # try as-is on root
            key = ("root", p)
            if key not in tried:
                tried.add(key)
                v = _json_get_first(ctx_root, p)
                if isinstance(v, list):
                    vals.extend(v)
                elif v is not None:
                    vals.append(v)
            # try prefixed variants
            for pref in ("data.", "data.parsedData."):
                pp = pref + p
                key = ("root", pp)
                if key in tried:
                    continue
                tried.add(key)
                v = _json_get_first(ctx_root, pp)
                if isinstance(v, list):
                    vals.extend(v)
                elif v is not None:
                    vals.append(v)
        ok = False
        if op == "truthy":
            ok = any(_is_truthy_val(v) for v in vals)
        elif op in ("eq", "equals"):
            ok = any(str(v) == str(rule.get("value")) for v in vals)
        elif op in ("neq", "not_equals"):
            ok = any(str(v) != str(rule.get("value")) for v in vals)
        elif op == "contains":
            needle = str(rule.get("value", "")).lower()
            ok = any(needle in str(v).lower() for v in vals)
        elif op == "in":
            choices = set(map(str, rule.get("values") or []))
            ok = any(str(v) in choices for v in vals)
        elif op == "regex":
            pat = re.compile(str(rule.get("value", "")), re.I)
            ok = any(bool(pat.search(str(v))) for v in vals)
        else:
            ok = any(_is_truthy_val(v) for v in vals)
        outcomes.append(ok)
    return any(outcomes) if mode == "any" else all(outcomes)

def _first_pass_has_table(res: dict) -> bool:
    """Prefer schema‑configured paths; fallback to legacy has_table_field."""
    paths = (CFG.get("schema", {}).get("first_pass", {}) or {}).get("has_table") or []
    pdict = (res.get("data", {}) or {}).get("parsedData", {})
    if paths:
        if isinstance(pdict, list):
            return any(_json_any_truthy(item, paths) for item in pdict if isinstance(item, dict))
        return _json_any_truthy(pdict, paths)
    # Legacy fallback (single key)
    field = HAS_TABLE_FIELD
    if isinstance(pdict, list):
        for item in pdict:
            if isinstance(item, dict) and field in item and _is_truthy_val(item.get(field)):
                return True
        return False
    return _is_truthy_val(pdict.get(field))

"""
Importing _second_pass_container and _second_pass_field from iwe_core.selection.
"""

def _second_pass_org_type(pd_payload: dict | list):
    """Extract organisation_type.type from second-pass payload.
    Priority:
      1) Configured schema paths under CFG['schema']['second_pass']['organisation_type']
      2) Sensible fallbacks on common shapes: payload['organisation_type']['type'] etc.
    """
    if isinstance(pd_payload, list):
        return None
    # 1) Config-driven lookup (if provided)
    paths = (CFG.get("schema", {}).get("second_pass", {}) or {}).get("organisation_type") or []
    for p in paths:
        v = _json_get_first(pd_payload, p)
        if v:
            try:
                logger.debug("[routing] organisation_type found via schema path '%s': %r", p, v)
            except Exception:
                pass
            return v
    # 2) Fallbacks for common keys
    try:
        direct = (((pd_payload or {}).get("organisation_type") or {}).get("type"))
        if direct:
            logger.debug("[routing] organisation_type found via default path 'organisation_type.type': %r", direct)
            return direct
    except Exception:
        pass
    try:
        alt = (((pd_payload or {}).get("organization_type") or {}).get("type"))
        if alt:
            logger.debug("[routing] organisation_type found via fallback path 'organization_type.type': %r", alt)
            return alt
    except Exception:
        pass
    logger.debug("[routing] organisation_type not found; will fall back to default company_type")
    return None

def _resolve_routing(doc_type: str, company_type: str | None = None) -> tuple[str, str, str]:
    """
    Resolve (parser_app, model_id, extra_accuracy) using config routing first,
    then fall back to 'third' defaults.
    """
    dt = (doc_type or "").strip().lower()
    ct = (company_type or _ROUTING_COMPANY_DEFAULT or "corporate").strip().lower()

    # Show available keys for visibility
    try:
        _keys_ct = sorted(list((_ROUTING_CFG.get(ct) or {}).keys())) if isinstance(_ROUTING_CFG.get(ct), dict) else []
        _keys_corporate = sorted(list((_ROUTING_CFG.get("corporate") or {}).keys())) if isinstance(_ROUTING_CFG.get("corporate"), dict) else []
        logger.info("[routing] available doc_type keys → ct=%s: %s | corporate: %s", ct, _keys_ct, _keys_corporate)
    except Exception:
        pass

    def _lookup(ct_key: str, dt_key: str):
        ct_map = _ROUTING_CFG.get(ct_key, {})
        if isinstance(ct_map, dict):
            hit = ct_map.get(dt_key)
            if isinstance(hit, dict):
                # Per-entry toggle (default enabled)
                if str(hit.get("enable", True)).strip().lower() in {"false","0","no","off"}:
                    # Respect skip-on-disabled semantics if enabled
                    if _ROUTING_SKIP_ON_DISABLED:
                        try:
                            logger.info("[routing] %s/%s is disabled and skip_on_disabled=true → skipping", ct_key, dt_key)
                        except Exception:
                            pass
                        return (None, None, None)
                    return None
                parser = hit.get("parser") or THIRD_PARSER_APP_ID
                model  = hit.get("model")  or THIRD_MODEL_ID
                extra  = str(hit.get("extra", EXTRA_ACCURACY_THIRD)).lower()
                # Global allow/block lists
                if _ROUTING_ALLOWED_PARSERS and parser not in _ROUTING_ALLOWED_PARSERS:
                    try:
                        logger.info("[routing] parser %s not in allowed_parsers; falling back", parser)
                    except Exception:
                        pass
                    return None
                if parser in _ROUTING_BLOCKED_PARSERS:
                    try:
                        logger.info("[routing] parser %s is blocked; falling back", parser)
                    except Exception:
                        pass
                    return None
                try:
                    logger.info("[routing] matched %s/%s → parser=%s, model=%s, extra=%s", ct_key, dt_key, parser, model, extra)
                except Exception:
                    pass
                return (parser, model, extra)
        return None

    for mode in _ROUTING_FALLBACK_ORDER:
        try:
            logger.info("[routing] attempt=%s ct=%s dt=%s", mode, ct, dt)
        except Exception:
            pass
        if mode == "company_type_and_doc_type":
            r = _lookup(ct, dt)
            if r == (None, None, None):
                # Explicit skip requested
                return r
            if r: return r
        elif mode == "corporate_and_doc_type":
            r = _lookup("corporate", dt)
            if r: return r
        elif mode == "third_defaults":
            try:
                logger.info("[routing] no route; using third_defaults → parser=%s, model=%s, extra=%s", THIRD_PARSER_APP_ID, THIRD_MODEL_ID, EXTRA_ACCURACY_THIRD)
            except Exception:
                pass
            return (THIRD_PARSER_APP_ID, THIRD_MODEL_ID, EXTRA_ACCURACY_THIRD)

    try:
        logger.info("[routing] exhausted fallbacks; using third_defaults → parser=%s, model=%s, extra=%s", THIRD_PARSER_APP_ID, THIRD_MODEL_ID, EXTRA_ACCURACY_THIRD)
    except Exception:
        pass
    return (THIRD_PARSER_APP_ID, THIRD_MODEL_ID, EXTRA_ACCURACY_THIRD)


# ─── Doc-type normalisation & page-text heuristics ────────────────────────

"""
Using grouping._canon_text for canonical text normalisation.
"""

_DOC_NORMALISATIONS: list[tuple[str, str]] = [
    # Balance Sheet
    (r"^consolidated.*balance.*", "Consolidated Balance Sheet"),
    (r"^standalone.*balance.*", "Standalone Balance Sheet"),
    (r"\bbalance\s*sheet\b", "Standalone Balance Sheet"),
    (r"statement of assets and liabilities", "Standalone Balance Sheet"),
    # Profit & Loss
    (r"^consolidated.*(profit).*(loss)", "Consolidated Profit and Loss Statement"),
    (r"^standalone.*(profit).*(loss)", "Standalone Profit and Loss Statement"),
    (r"(statement of profit).*(loss)", "Standalone Profit and Loss Statement"),
    (r"\bprofit\s*and\s*loss\b", "Standalone Profit and Loss Statement"),
    # Cashflow
    (r"^consolidated.*cash.*flow", "Consolidated Cashflow"),
    (r"^standalone.*cash.*flow", "Standalone Cashflow"),
    (r"cash\s*flow", "Standalone Cashflow"),
]


def normalize_company_type(ct_raw: str | None) -> str:
    """
    Map 'organisation_type.type' (e.g., 'Bank', 'Non Banking Financial Company', 'Non Financial Company')
    to routing keys in config.yaml: 'bank', 'nbfc', 'insurance', 'corporate'.
    Token-aware: respects the left-to-right order in strings like "Bank/NBFC/Non Financial Company".
    """
    s = _canon_text(ct_raw or "")
    if not s:
        return _ROUTING_COMPANY_DEFAULT or "corporate"

    # Split by common separators and evaluate tokens left→right
    tokens = [t.strip() for t in re.split(r"[|/,;]+", s) if t.strip()]
    for tok in tokens:
        cls = company_type_from_token(tok)
        if cls:
            return cls

    # Broad fallbacks on the full string if token pass didn't match
    if "bank" in s and "non banking" not in s and "non-banking" not in s:
        return "bank"
    if "nbfc" in s or "non banking financial" in s or "non-banking financial" in s:
        return "nbfc"
    if "insur" in s:
        return "insurance"
    if "non financial" in s or "corporate" in s or "company" in s:
        return "corporate"
    return _ROUTING_COMPANY_DEFAULT or "corporate"

def normalize_doc_type(label: str | None) -> str:
    """Map a variety of labels/synonyms to a canonical sheet name."""
    s = _canon_text(label or "")
    if not s:
        return "Others"
    for pat, out in _DOC_NORMALISATIONS:
        if re.search(pat, s):
            return out
    return (label or "Others").strip().title()

"""
Using grouping.extract_page_texts_from_pdf_bytes for PDF text extraction.
"""

def infer_doc_type_from_text(text: str) -> str | None:
    """
    Heuristically infer the statement type from visible page text.
    Returns a canonical doc_type or None.
    """
    s = _canon_text(text)
    if not s:
        return None
    is_cons  = "consolidated" in s
    is_stand = "standalone" in s and not is_cons
    base: str | None = None
    # Prefer Balance Sheet, then P&L; treat Cashflow more strictly to avoid false positives
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

def expand_selected_pages(selected_pages: list[int], total_pages: int, radius: int = 1) -> list[int]:
    """
    Be forgiving: include ±radius neighbour pages so we don't miss 'continued' pages
    that were misclassified as 'Others' in the first pass.
    """
    if not selected_pages:
        return list(range(1, total_pages + 1))  # fallback: include all pages
    include = set(selected_pages)
    for p in selected_pages:
        for d in range(1, radius + 1):
            if p - d >= 1:
                include.add(p - d)
            if p + d <= total_pages:
                include.add(p + d)
    return sorted(include)

def build_groups(
    selected_pages: list[int],
    classification: list[dict],
    original_pdf_bytes: bytes,
    first_pass_results: list[dict] | None = None,
) -> dict[str, list[int]]:
    """
    Build {doc_type -> [original_page_numbers]} using:
      • second-pass classification (page_wise_classification)
      • page-text heuristics to override obviously wrong labels
      • smoothing to pull 'Others' pages that sit between same-type pages
    Any leftover 'Others' pages are dropped.
    """
    # 1) Start with whatever the classifier returned, honoring continuations.
    doc_by_page: dict[int, str] = {}
    for item in classification or []:
        sel_no = item.get("page_number")
        # If this row is a continuation, prefer the parent's label.
        is_cont = is_true_flag(item.get("is_continuation"))
        dt_raw = (item.get("continuation_of") if is_cont else None) or item.get("doc_type")
        dt = normalize_doc_type(dt_raw)
        if isinstance(sel_no, int) and 1 <= sel_no <= len(selected_pages):
            orig = selected_pages[sel_no - 1]
            doc_by_page[orig] = dt
        else:
            try:
                logger.warning("Skipping out-of-range selected index in classification: %r (max=%d)", sel_no, len(selected_pages))
            except Exception:
                pass

    # Track which original pages are explicit continuations (for override guard)
    inherit_on_cont = bool(CFG.get("grouping", {}).get("inherit_scope_on_continuation", True))
    cont_orig_pages: set[int] = set()
    if inherit_on_cont:
        for item in classification or []:
            sel_no = item.get("page_number")
            is_cont = is_true_flag(item.get("is_continuation"))
            if not is_cont or not isinstance(sel_no, int):
                continue
            if 1 <= sel_no <= len(selected_pages):
                cont_orig_pages.add(selected_pages[sel_no - 1])
            else:
                # Out-of-range selected index; ignore
                continue

    # Track which original pages were flagged as 'has_two' (mixed page)
    has_two_orig_pages: set[int] = set()
    for item in classification or []:
        sel_no = item.get("page_number")
        if not isinstance(sel_no, int):
            continue
        has_two_flag = is_true_flag(item.get("has_two") or item.get("Has_multiple_sections") or "")
        if not has_two_flag:
            continue
        if 1 <= sel_no <= len(selected_pages):
            has_two_orig_pages.add(selected_pages[sel_no - 1])
        else:
            # Out-of-range selected index; ignore
            continue

    # 2) Ensure every selected page is present; use header heuristics if needed.
    # Guard: prevent header-based override of "Others" if config says so
    prevent_override_others = bool(CFG.get("grouping", {}).get("prevent_override_when_others", True))
    page_texts = extract_page_texts_from_pdf_bytes(original_pdf_bytes)
    disable_header_override = bool(CFG.get("grouping", {}).get("disable_header_override", False))
    for orig in selected_pages:
        inferred = infer_doc_type_from_text(page_texts[orig - 1] if 0 <= orig - 1 < len(page_texts) else "")
        if inferred:
            inferred = normalize_doc_type(inferred)
        if orig not in doc_by_page:
            # If classifier didn't label this page, fall back to header inference
            doc_by_page[orig] = inferred or "Others"
        else:
            current_label = doc_by_page[orig]
            # Do not override continuation or mixed (has_two) pages, or pages labeled 'Others' by second pass
            if (inherit_on_cont and orig in cont_orig_pages) or (orig in has_two_orig_pages) or (prevent_override_others and current_label == "Others"):
                pass  # keep classifier/primary label
            else:
                # Optionally disable header-based overrides to honor classification exactly
                if not disable_header_override:
                    # If classifier says Balance Sheet but header screams Cashflow (or vice-versa), trust header.
                    current = _canon_text(current_label)
                    if inferred and _canon_text(inferred) not in (current,):
                        kinds = lambda s: ("cash" if "cash" in s else "pl" if "loss" in s or "profit" in s else "bs" if "balance" in s or "assets" in s else "other")
                        if kinds(current) != kinds(_canon_text(inferred)):
                            try:
                                logger.info("Header override @p%d: %s → %s", orig, current_label, inferred)
                            except Exception:
                                pass
                            doc_by_page[orig] = inferred

    # 3) Absorb 'Others' *only when sandwiched* between same-type pages
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
            # Note: do NOT absorb at heads/tails; leave as 'Others'

    # 4) Groups (primary)
    groups: dict[str, list[int]] = {}
    for p in sorted(doc_by_page):
        dt = doc_by_page[p]
        if dt == "Others":
            continue
        groups.setdefault(dt, []).append(p)

    # 5) Include secondary classifications when the second parser flags two sections on one page.
    #    Accept common key variants for resilience.
    for item in classification or []:
        sel_no = item.get("page_number")
        if not isinstance(sel_no, int):
            continue
        # Map selected.pdf index back to original page number
        if 1 <= sel_no <= len(selected_pages):
            orig = selected_pages[sel_no - 1]
        else:
            # Out-of-range selected index; ignore this secondary label
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

    # 5b) Also use first‑pass "Sections" to route single pages to multiple parsers (no cropping).
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
            # never fail grouping because of a malformed first-pass JSON
            pass

    # De‑duplicate & sort page lists
    for dt, lst in list(groups.items()):
        groups[dt] = sorted({p for p in lst})

    # Log info about group pages & sizes before returning
    try:
        logger.info("Third-pass groups (original pages) → %s", {k: v for k, v in groups.items()})
        logger.info("Third-pass grouping → %s", {k: len(v) for k, v in groups.items()})
    except Exception:
        pass

    return groups



# (central debug helpers imported above)

"""Row extraction moved to iwe_core.json_ops.extract_rows."""

def sanitize_statement_df(doc_type: str, df: Any) -> Any:
    """
    Light-weight cleanups to match human expectations:
      • Merge '(Not annualised)' style notes into the 'particulars' text instead of a separate column.
      • Clear duplicate numbers copied onto the row just above a 'Total ...' line.
    """
    # no pandas import needed here
    if df is None or df.empty:
        return df

    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]

    # 1) Merge notes into particulars when they contain 'annualis...'
    part_col = next((c for c in out.columns if str(c).strip().lower() in {"particulars", "particular", "description", "line item", "line_item"}), None)
    note_col = next((c for c in out.columns if str(c).strip().lower() in {"note", "notes", "remark", "remarks"}), None)
    if part_col and note_col:
        mask = out[note_col].astype(str).str.contains("annualis", case=False, na=False)
        if mask.any():
            out.loc[mask, part_col] = (
                out.loc[mask, part_col].fillna("").astype(str).str.rstrip()
                + " (" + out.loc[mask, note_col].astype(str).str.strip() + ")"
            ).str.replace(r"\s+", " ", regex=True)
            # Clear the merged notes
            out.loc[mask, note_col] = ""
        # Drop an entirely empty notes column, if any
        if out[note_col].astype(str).str.strip().eq("").all():
            out = out.drop(columns=[note_col])

    # 2) Clear duplicate values on the row just above a 'Total ...' line
    if part_col:
        is_total = out[part_col].astype(str).str.contains(r"\btotal\b", case=False, na=False)
        # treat as numeric columns if at least one value parses via our coercer
        def _any_parses(col):
            try:
                return any(_coerce_number_like(v) is not None for v in out[col])
            except Exception:
                return False
        num_cols = [c for c in out.columns if c != part_col and _any_parses(c)]
        for idx in out.index[is_total]:
            pos = out.index.get_loc(idx)
            if pos == 0:
                continue
            prev_idx = out.index[pos - 1]
            # If every numeric cell parses and equals the total row below, blank the previous row's numbers
            duplicate_all = True
            for c in num_cols:
                a = _coerce_number_like(out.at[prev_idx, c])
                b = _coerce_number_like(out.at[idx, c])
                # require both to be numbers and equal
                if a is None or b is None or a != b:
                    duplicate_all = False
                    break
            if duplicate_all:
                for c in num_cols:
                    out.at[prev_idx, c] = None

    # 3) Reorder columns: sr_no, Particulars, then c1..cN, then the rest
    try:
        import re
        cols = list(out.columns)
        # locate sr_no
        sr = next((c for c in cols if str(c).strip().lower() == "sr_no"), None)
        # locate/normalize particulars
        part_aliases = {"particulars","particular","description","line item","line_item","account head","head"}
        pc = next((c for c in cols if str(c).strip().lower() in part_aliases), None)
        if pc and pc != "Particulars" and "Particulars" not in out.columns:
            out.rename(columns={pc: "Particulars"}, inplace=True)
            pc = "Particulars"
        elif "Particulars" in out.columns:
            pc = "Particulars"
        # collect c1..cN (keep actual casing)
        c_cols = sorted(
            [c for c in out.columns if re.fullmatch(r"[cC]\d+", str(c))],
            key=lambda x: int(re.findall(r"\d+", str(x))[0])
        )
        ordered = []
        if sr and sr in out.columns:
            ordered.append(sr)
        if pc and pc in out.columns and pc not in ordered:
            ordered.append(pc)
        for c in c_cols:
            if c not in ordered:
                ordered.append(c)
        for c in out.columns:
            if c not in ordered:
                ordered.append(c)
        out = out.loc[:, ordered]
    except Exception:
        # never fail on ordering
        pass
    # Preserve LLM order by default; caller can enable reorder via env
    # Reordering handled by downstream excel normalization when needed
    return out

def _extract_period_maps_from_payload(pd_payload: dict | list) -> tuple[dict[str, dict], dict[str, str]]:
    """
    Extract period details from a Fracto parsedData payload.
    Returns a tuple:
      • by_id: {"c1": {label, start_date, end_date, role, is_cumulative, is_audited}, ...}
      • labels: {"c1": "<label>", ...}
    """
    try:
        meta = (pd_payload or {}).get("meta") or {}
    except AttributeError:
        return {}, {}
    periods = meta.get("periods") or []
    try:
        print(f"[Periods] payload meta periods count={len(periods) if isinstance(periods, list) else 0}")
    except Exception:
        pass
    by_id: dict[str, dict] = {}
    labels: dict[str, str] = {}
    for p in periods:
        if not isinstance(p, dict):
            continue
        pid = str(p.get("id") or "").strip().lower()
        if not pid:
            continue
        info = {
            "label": p.get("label") or "",
            "start_date": p.get("start_date"),
            "end_date": p.get("end_date"),
            "role": p.get("role"),
            "is_cumulative": _is_truthy_val(p.get("is_cumulative")),
            "is_audited": _is_truthy_val(p.get("is_audited")),
        }
        by_id[pid] = info
        labels[pid] = info["label"] or pid.upper()
    return by_id, labels
# ──────────────────────────────────────────────────────────────────────────

# --- Helper: build periods map from in-memory third-pass raw ---
def _build_periods_map_from_third(third_pass_raw: dict[str, list[dict]] | None) -> dict[str, dict]:
    """
    Build {doc_type: {'c1': {...}, 'c2': {...}, ...}} from in-memory third_pass_raw.
    """
    out: dict[str, dict] = {}
    if not third_pass_raw:
        return out
    for _dt_key, _res_list in (third_pass_raw or {}).items():
        dt_norm = normalize_doc_type(_dt_key)
        candidates = _res_list if isinstance(_res_list, list) else [_res_list]
        for _res in candidates:
            if not isinstance(_res, dict):
                continue
            pd_payload = ((_res.get("data") or {}).get("parsedData") or {})
            by_id, _labels = _extract_period_maps_from_payload(pd_payload)
            if by_id:
                out[dt_norm] = by_id
                break
    return out

def _labels_only(periods_by_doc: dict[str, dict] | None) -> dict[str, dict]:
    """Reduce {doc: {c1:{label,...}}} → {doc: {c1:'label',...}} for compact debug dumps."""
    out: dict[str, dict] = {}
    for dt, cmap in (periods_by_doc or {}).items():
        out[dt] = {}
        for cid, meta in (cmap or {}).items():
            try:
                out[dt][str(cid).lower()] = (meta or {}).get("label", "")
            except Exception:
                out[dt][str(cid).lower()] = ""
    return out

# --- Insert: doc type and period discovery helpers ---
"""
Using iwe_core.json_ops.doc_type_from_payload via local alias _doc_type_from_payload.
"""

def _scan_group_jsons_for_periods(pdf_path: str, stem: str) -> tuple[dict[str, dict], dict[str, dict]]:
    """
    Fallback discovery: scan per‑group *_ocr.json files next to the PDF to
    recover period metadata when third‑pass raw payloads aren't available here.

    Returns:
      (periods_by_doc, labels_by_doc)
      where:
        periods_by_doc[doc_type] = {"c1": {...}, ...}
        labels_by_doc[doc_type]  = {"c1": "As on 31.03.2025 (Audited)", ...}
    """
    base_dir = Path(pdf_path).expanduser().resolve().parent
    periods_by_doc: dict[str, dict] = {}
    labels_by_doc: dict[str, dict] = {}
    print(f"[Periods] scan: dir={base_dir} pattern='{stem}_*_ocr.json'")

    # Pattern like: <stem>_<slug>_ocr.json, but ignore the first‑pass "<stem>_ocr.json"
    for p in sorted(base_dir.glob(f"{stem}_*_ocr.json")):
        name = p.name
        if name == f"{stem}_ocr.json":
            continue  # first‑pass; no meta.periods there
        try:
            with open(p, "r", encoding="utf-8") as fh:
                obj = json.load(fh)
        except Exception:
            continue

        # Locate parsedData in a tolerant way
        pd_payload = None
        if isinstance(obj, dict):
            pd_payload = ((obj.get("data") or {}).get("parsedData")) or obj.get("parsedData") or None
        if not isinstance(pd_payload, dict):
            continue

        print(f"[Periods] inspecting file: {p.name}")
        if not isinstance(pd_payload, dict):
            print(f"[Periods]   -> parsedData missing/not dict; skipping {p.name}")
            continue

        by_id, labels = _extract_period_maps_from_payload(pd_payload)
        print(f"[Periods]   -> found {len(labels) if labels else 0} period labels in {p.name}")
        if not labels:
            continue

        dt = _doc_type_from_payload(pd_payload)
        if not dt:
            # Fallback: derive a doc label from the filename slug
            slug = p.stem.replace(f"{stem}_", "").replace("_ocr", "")
            dt = normalize_doc_type(slug.replace("_", " "))

        if dt:
            periods_by_doc[dt] = by_id
            labels_by_doc[dt] = {k.lower(): v for k, v in labels.items()}

    return periods_by_doc, labels_by_doc

# ──────────────────────────────────────────────────────────────────────────

# --- Helper: pick period labels for a sheet from local/global ---
def _pick_period_labels_for_sheet(sheet_name: str,
                                  local_labels_by_doc: dict[str, dict] | None,
                                  global_labels_by_doc: dict[str, dict] | None) -> dict[str, str]:
    """
    Return a labels map for the given sheet by:
      1) exact key match in local (disk-scanned) labels
      2) normalized-key match in local labels
      3) exact key match in global cache
      4) normalized-key match in global cache
    Values are expected like {"c1": "Label", ...} and keys should be lowercase.
    """
    def _norm(s: str) -> str:
        return _canon_text(s or "")
    local = local_labels_by_doc or {}
    globl = global_labels_by_doc or {}

    # 1) Exact in local
    if sheet_name in local and isinstance(local[sheet_name], dict):
        return {k.lower(): v for k, v in local[sheet_name].items()}

    # 2) Normalized in local
    s_norm = _norm(sheet_name)
    for k, v in local.items():
        if _norm(k) == s_norm and isinstance(v, dict):
            return {kk.lower(): vv for kk, vv in v.items()}

    # 3) Exact in global
    if sheet_name in globl and isinstance(globl[sheet_name], dict):
        return {k.lower(): v for k, v in globl[sheet_name].items()}

    # 4) Normalized in global
    for k, v in globl.items():
        if _norm(k) == s_norm and isinstance(v, dict):
            return {kk.lower(): vv for kk, vv in v.items()}

    return {}


def _coerce_number_like(x):
    """
    Convert number-like strings to floats/ints:
    - "1,234.56" -> 1234.56
    - "(1,000)"  -> -1000
    - "—", "-", "" -> None
    Returns None if not numeric.
    """
    if x is None:
        return None
    # Treat Pandas/NumPy NaNs as None
    try:
        import math as _math
        # plain float NaN
        if isinstance(x, float) and _math.isnan(x):
            return None
    except Exception:
        pass
    try:
        import numpy as _np
        if isinstance(x, _np.floating) and _np.isnan(x):
            return None
    except Exception:
        pass
    try:
        import pandas as _pd  # type: ignore
        if x is getattr(_pd, 'NA', object()):
            return None
    except Exception:
        pass
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if s == "" or s in {"—", "–", "-", "N/A", "na", "NA", "None", "none", "null", "NULL"}:
        return None
    if s.lower() in {"nan", "+nan", "-nan", "inf", "+inf", "-inf"}:
        return None
    # Normalise unicode spaces and odd glyphs
    try:
        import re as _re
        s = s.replace("\u00A0", " ")  # NBSP → space
        s = _re.sub(r"[\u2000-\u200B]", "", s)  # thin/zero-width spaces
    except Exception:
        pass
    neg = s.startswith("(") and s.endswith(")")
    if neg:
        s = s[1:-1].strip()
    s = s.replace(",", "").replace("₹", "").replace("$", "").replace("€", "").replace("£", "")
    # Drop any trailing footnote marks or non-numeric suffix/prefix like '*', '†'
    try:
        import re as _re
        s = _re.sub(r"[^0-9.\-]", "", s)
    except Exception:
        pass
    try:
        v = float(s)
        return -v if neg else v
    except Exception:
        return None





from iwe_core.config import configure_logging as _configure_logging

# Configure logging from config.yaml (console + optional rotating file)
_logfile = _configure_logging()
logger = logging.getLogger("FractoPageOCR")

import sys
print(f"[BOOT] Running script: {__file__}", flush=True)
if _logfile:
    try:
        print(f"[BOOT] Log file: {_logfile}", flush=True)
    except Exception:
        pass
try:
    print(f"[BOOT] Excel writer defined at line {excel_ops._write_statements_workbook.__code__.co_firstlineno}", flush=True)
except Exception as e:
    print(f"[BOOT] Excel writer introspection failed: {e}", flush=True)

# Global: do NOT reset if already populated earlier in this process
try:
    PERIOD_LABELS_BY_DOC  # type: ignore[name-defined]
except NameError:
    PERIOD_LABELS_BY_DOC: dict[str, dict] = {}


def _load_formats():
    """
    Parse mapping.yaml and return a dict[str, dict] keyed by human‑friendly
    format name → {'mappings':…, 'template_path':…, 'sheet_name':…}
    and gracefully support three YAML layouts:
      ① legacy `excel_export` (single format)
      ② multiple `excel_export_*` siblings
      ③ modern `formats: { … }`
    """
    script_dir   = Path(__file__).parent
    mapping_rel  = CFG.get("paths", {}).get("mapping_yaml", "mapping.yaml")
    mapping_file = (script_dir / mapping_rel).expanduser().resolve()
    formats: dict[str, dict] = {}

    if not mapping_file.exists():
        return formats

    with open(mapping_file, "r", encoding="utf-8") as f:
        try:
            data = yaml.safe_load(f) or {}
        except yaml.YAMLError as exc:
            logger.error("Failed to parse %s: %s", mapping_file, exc)
            return formats

    # ③ modern block
    if isinstance(data, dict) and "formats" in data:
        for name, cfg in data["formats"].items():
            if isinstance(cfg, dict):
                formats[str(name)] = cfg

    # ① legacy – keep as "Customs Invoice"
    if isinstance(data, dict) and "excel_export" in data:
        formats["Customs Invoice"] = data["excel_export"]

    # ② multiple excel_export_* blocks
    for key, val in (data.items() if isinstance(data, dict) else []):
        if key.startswith("excel_export_") and isinstance(val, dict):
            pretty = key.replace("excel_export_", "").replace("_", " ").title()
            formats[pretty] = val

    # Fallback: raw mapping dict alone
    if not formats and isinstance(data, dict):
        formats["Customs Invoice"] = {"mappings": data}

    # Normalise paths & ensure each entry has mappings dict
    for cfg in formats.values():
        if "mappings" not in cfg:
            cfg["mappings"] = {}
        if tp := cfg.get("template_path"):
            cfg["template_path"] = (script_dir / tp).expanduser().resolve()

    return formats

FORMATS = _load_formats()
DEFAULT_FORMAT = next(iter(FORMATS)) if FORMATS else "Customs Invoice"

# Keep legacy single‑format globals for existing callers
_default_cfg       = FORMATS.get(DEFAULT_FORMAT, {})
MAPPINGS           = _default_cfg.get("mappings", {})
TEMPLATE_PATH      = _default_cfg.get("template_path")
SHEET_NAME         = _default_cfg.get("sheet_name")
HEADERS = list(MAPPINGS.keys())


"""Use iwe_core.ocr_client.call_fracto directly."""




# ─── Helper to persist results ───────────────────────────────────────────
"""Use iwe_core.json_ops.save_results for persisting."""


# ─── Simple helper for CLI workflow ──────────────────────────────────────
def process_pdf(pdf_path: str) -> list[dict]:
    """
    Read *pdf_path* from disk and OCR it via `call_fracto_parallel`, honouring
    the current CHUNK_SIZE_PAGES and MAX_PARALLEL settings.

    Returns the list of Fracto API responses for every page‑chunk.
    """
    pdf_path = Path(pdf_path).expanduser().resolve()
    with open(pdf_path, "rb") as fh:
        pdf_bytes = fh.read()
    return call_fracto_parallel(pdf_bytes, pdf_path.name, extra_accuracy=EXTRA_ACCURACY_FIRST)

# ─── CLI ─────────────────────────────────────────────────────────────────
def _cli():
    """
    Usage:
        python a.py <pdf-path> [output.json] [output.xlsx] [KEY=VALUE ...]

    Convenience:
        • If you pass only two arguments and the second one ends with .xlsx / .xlsm / .xls,
          it is treated as the Excel output, and the JSON will default to
          "<pdf‑stem>_ocr.json" next to the PDF.
        • Any KEY=VALUE pairs will be written or overwritten in every row of the Excel output.
    """
    if len(sys.argv) < 2:
        print("Usage: python a.py <pdf-path> [output.json] [output.xlsx] [KEY=VALUE ...]")
        sys.exit(1)

    args = sys.argv[1:]

    pdf_path     = args[0]
    json_out     = None
    excel_out    = None

    
    # Collect KEY=VALUE overrides (e.g. --set Client=Acme or Client=Acme)
    overrides = {}
    remaining = []
    for arg in args[1:]:
        if "=" in arg:
            k, v = arg.split("=", 1)
            overrides[k.strip()] = v
        else:
            remaining.append(arg)
    # Re‑interpret remaining (non‑override) args for json/excel outputs
    if remaining:
        if remaining[0].lower().endswith((".xlsx", ".xlsm", ".xls")):
            excel_out = remaining[0]
        else:
            json_out = remaining[0]
    if len(remaining) >= 2:
        excel_out = remaining[1]

    # If user provided an output.json on CLI, treat it as the combined statements JSON path
    client_json_out = json_out if (json_out and json_out.lower().endswith(".json")) else None

    # Allow CLI KEY=VALUE to toggle quick filter without editing YAML/env
    # e.g. `FILTER=0` or `QUICK_FILTER=off` will disable; `FILTER=1` enables
    try:
        _raw_cli_filter = None
        for _k in ("FILTER", "QUICK_FILTER", "FILTER_ENABLE", "QUICK_FILTER_ENABLE"):
            if _k in overrides:
                _raw_cli_filter = overrides[_k]
                break
        if _raw_cli_filter is not None:
            _val = str(_raw_cli_filter).strip().lower()
            _on = _val in ("1","true","yes","y","on")
            os.environ["FRACTO_FILTER_ENABLE"] = "1" if _on else "0"
            logger.info("Quick filter toggle via CLI: %s", "ON" if _on else "OFF")
        # Optional: min pages override `FILTER_MIN=8`
        if "FILTER_MIN" in overrides:
            os.environ["FRACTO_FILTER_MIN_PAGES"] = str(overrides["FILTER_MIN"]).strip()
    except Exception as _e:
        logger.warning("Ignoring CLI quick-filter toggle due to error: %s", _e)

    if not os.path.isfile(pdf_path):
        logger.error("File not found: %s", pdf_path)
        sys.exit(2)
    # Derive stem (used by writers and disk-scan fallbacks)
    try:
        pdf_p = Path(pdf_path).expanduser().resolve()
        stem = pdf_p.stem
        logger.info("Output stem derived: %s", stem)
    except Exception:
        pdf_p = Path(pdf_path)
        stem = pdf_p.stem

    # Preflight: ensure API key is present
    if not _resolve_api_key():
        logger.error("No API key found. Set %s or add api.api_key in config.yaml", API_KEY_ENV)
        sys.exit(3)

    # Timing variables
    timings: Dict[str, Dict[str, float]] = {}
    overall_start = time.time(); timings["overall"] = {"start": overall_start}
    first_pass_time = 0.0
    second_pass_time = 0.0
    third_pass_time = 0.0

    # 1️⃣ First‑pass OCR (page‑level classification)
    first_pass_start = time.time(); _t0 = first_pass_start
    results = process_pdf(pdf_path)
    first_pass_end = time.time(); first_pass_time = first_pass_end - _t0
    timings["first_pass"] = {"start": first_pass_start, "end": first_pass_end, "dur": first_pass_time}
    # If every chunk failed (e.g., 403), abort with guidance


    # ... (snip: rest of main logic) ...

    # After third-pass processing, before writing combined workbook and JSON:
    # Only proceed if combined artifacts exist; otherwise let downstream pipeline handle exports.
    have_combined = all(name in locals() for name in ("combined_sheets","combined_rows","groups","routing_used","company_type"))
    try:
        print("[Main] combined artifacts ready?", have_combined)
    except Exception:
        pass

    # Build periods map from in-memory third pass (preferred), fallback to disk scan
    periods_by_doc: dict[str, dict] = {}
    try:
        _tpr = locals().get('third_pass_raw')
        if _tpr:
            periods_by_doc = _build_periods_map_from_third(_tpr)
            print("[Main] built periods_by_doc from memory")
    except Exception as e:
        print("[Main] build periods_by_doc from memory failed:", e)
        periods_by_doc = {}
    if not periods_by_doc:
        try:
            _pmap, _plabels = _scan_group_jsons_for_periods(pdf_path, stem)
            periods_by_doc = _pmap or {}
            print("[Main] built periods_by_doc from disk scan")
        except Exception as e:
            print("[Main] disk scan for periods failed:", e)
            periods_by_doc = {}
    try:
        print("[Main] periods_by_doc keys:", list(periods_by_doc.keys()))
    except Exception:
        pass

    if have_combined:
        try:
            print("[Main] workbook finished, starting combined JSON writer …")
            periods_hint = _build_periods_map_from_third(locals().get('third_pass_raw'))  # may be None
            print("[Excel] periods_hint from third-pass raw:", {k: list(v.keys()) for k,v in (periods_hint or {}).items()}, flush=True)
            # xlsx_path = excel_ops._write_statements_workbook(
            #     pdf_path,
            #     stem,
            #     combined_sheets,
            #     routing_used=routing_used,
            #     periods_by_doc=periods_hint
            # )
            try:
                json_path = json_ops.write_statements_json(
                    str(pdf_p), stem,
                    locals().get('combined_rows'),
                    locals().get('groups'),
                    locals().get('routing_used'),
                    locals().get('company_type'),
                    out_path_override=client_json_out,
                    first_pass_results=results,
                    second_pass_result=locals().get('second_res'),
                    third_pass_raw=locals().get('third_pass_raw'),
                    periods_by_doc=periods_by_doc,
                )
            except TypeError:
                # periods_by_doc may not be supported; fallback to old signature
                print("[Main] workbook finished, starting combined JSON writer (legacy signature) …")
                json_path = json_ops.write_statements_json(
                    str(pdf_p), stem,
                    locals().get('combined_rows'),
                    locals().get('groups'),
                    locals().get('routing_used'),
                    locals().get('company_type'),
                    out_path_override=client_json_out,
                    first_pass_results=results,
                    second_pass_result=locals().get('second_res'),
                    third_pass_raw=locals().get('third_pass_raw'),
                )
        except Exception as e:
            print("[Main] finalize exports failed:", e)
    else:
        print("[Main] Deferring exports — combined artifacts not ready yet; pipeline will export later.")

    ok_count = sum(1 for r in results if (r or {}).get("status") == "ok")
    if ok_count == 0:
        logger.error("First pass failed for all pages. Likely authentication issue (403). Check API key and endpoint: %s", FRACTO_ENDPOINT)
        return

    # 2️⃣ Persist first‑pass JSON immediately
    # Always persist first-pass JSON to default name (<stem>_ocr.json); reserve CLI json_out for combined statements JSON
    json_ops.save_results(results, str(pdf_p), None)

    # 3️⃣ Identify pages to reprocess using configurable criteria (if any), else legacy has_table
    sel_cfg = CFG.get("passes", {}).get("first", {}).get("selection", {}) or {}
    use_criteria = bool((sel_cfg.get("criteria") or {}).get("rules"))
    if use_criteria:
        selected_pages = [idx + 1 for idx, res in enumerate(results) if _select_by_criteria(res)]
    else:
        selected_pages = [idx + 1 for idx, res in enumerate(results) if _first_pass_has_table(res)]
    # Optional neighbour expansion via env var (default radius = 0 to stick to 'has_table' pages)
    try:
        _radius = int(os.getenv("FRACTO_EXPAND_NEIGHBORS", str(SELECTION_EXPAND_NEIGHBORS)))
    except Exception:
        _radius = SELECTION_EXPAND_NEIGHBORS
    selected_pages = expand_selected_pages(selected_pages, len(results), radius=_radius)
    if not selected_pages:
        # Fallback: previous heuristic (Document_type/Has_multiple_sections)
        selected_pages = [
            idx + 1
            for idx, res in enumerate(results)
            if (
                (res.get("data", {}).get("parsedData", {}).get("Document_type", "Others") or "").lower() != "others"
                or _is_truthy_val((res.get("data", {}).get("parsedData", {}) or {}).get("Has_multiple_sections"))
            )
        ]
        selected_pages = expand_selected_pages(selected_pages, len(results), radius=_radius)
    if not selected_pages:
        # Ultimate fallback: include all pages to let second-pass classify carefully
        selected_pages = list(range(1, len(results) + 1))

    if selected_pages:
        # ── Optional quick nano filter to prune irrelevant pages ─────────────────
        flt = (CFG.get("passes", {}).get("filter", {}) or {})
        try:
            flt_enable = str(os.getenv("FRACTO_FILTER_ENABLE", str(flt.get("enable", False)))).strip().lower() in ("1","true","yes","y","on")
            min_pages_to_run = int(os.getenv("FRACTO_FILTER_MIN_PAGES", str(flt.get("min_pages_to_run", 0))))
            if flt_enable and len(selected_pages) >= min_pages_to_run:
                # Build a temporary selected.pdf for the quick filter
                from iwe_core.pdf_ops import build_pdf_from_pages
                with open(pdf_path, "rb") as _fh0:
                    _orig0 = _fh0.read()
                _sel0 = build_pdf_from_pages(_orig0, selected_pages)

                nano_parser = str(flt.get("parser_app") or SECOND_PARSER_APP_ID)
                nano_model  = str(flt.get("model") or SECOND_MODEL_ID)
                nano_extra  = str(flt.get("extra_accuracy", False)).lower()
                _nano_name  = SELECTED_PDF_NAME_TMPL.format(stem=Path(pdf_path).stem).replace("selected.pdf", "selected_quick.pdf")

                nano_res = call_fracto(
                    _sel0,
                    _nano_name,
                    parser_app=nano_parser,
                    model=nano_model,
                    extra_accuracy=nano_extra,
                )

                # Parse quick classification using schema helpers
                _pdpay = (nano_res.get("data", {}) or {}).get("parsedData", {})
                _rows  = _second_pass_container(_pdpay)
                _keep_types = set(flt.get("keep_doc_types") or (CFG.get("labels", {}).get("canonical", []) or []))
                _keep_types.discard("Others")
                _keep_if_has_two = bool(flt.get("keep_if_has_two", True))
                _drop_others     = bool(flt.get("drop_others", True))

                _keep_idx = []  # indices within selected.pdf (1-based)
                for i, item in enumerate(_rows, start=1):
                    if not isinstance(item, dict):
                        continue
                    _main  = normalize_doc_type(_second_pass_field(item, "doc_type") or _second_pass_field(item, "continuation_of"))
                    _has2  = _is_truthy_val(_second_pass_field(item, "has_two"))
                    # Decide keep/drop
                    keep = False
                    if _keep_if_has_two and _has2:
                        keep = True
                    elif _main and _main != "Others" and _main in _keep_types:
                        keep = True
                    elif not _drop_others and _main == "Others":
                        keep = True
                    if keep:
                        _keep_idx.append(i)

                if _keep_idx and len(_keep_idx) < len(selected_pages):
                    _before = selected_pages[:]
                    selected_pages = [ _before[i-1] for i in _keep_idx if 1 <= i <= len(_before) ]
                    logger.info("Quick filter kept %d/%d pages → %s", len(selected_pages), len(_before), selected_pages)
                else:
                    logger.info("Quick filter made no change (kept %d/%d)", len(selected_pages), len(selected_pages))
            elif not flt_enable:
                logger.info("Quick filter disabled via config/env/CLI → skipping.")
            else:
                logger.info("Quick filter skipped: selected_pages=%d < min_pages_to_run=%d",
                            len(selected_pages), min_pages_to_run)
        except Exception as _exc:
            logger.warning("Quick filter skipped due to error: %s", _exc)

        logger.info("Second pass: re‑processing %d selected pages %s",
                    len(selected_pages), selected_pages)

        # 4️⃣ Assemble those pages into a single in‑memory PDF (and keep original bytes for grouping)
        with open(pdf_path, "rb") as fh:
            orig_bytes = fh.read()
        from iwe_core.pdf_ops import build_pdf_from_pages

        stem = Path(pdf_path).stem
        sel_pdf_name = SELECTED_PDF_NAME_TMPL.format(stem=stem)

        second_pass_start = time.time(); _t1 = second_pass_start
        if SECOND_COMBINE_PAGES:
            # Combine selected pages into one selected.pdf
            selected_bytes = build_pdf_from_pages(orig_bytes, selected_pages)

            second_res = call_fracto(
                selected_bytes,
                sel_pdf_name,
                parser_app=SECOND_PARSER_APP_ID,
                model=SECOND_MODEL_ID,
                extra_accuracy=EXTRA_ACCURACY_SECOND,
            )
        
        
        else:
            # Per-page classification in parallel → synthesize a classification list
            per_results = [None] * len(selected_pages)
            from concurrent.futures import ThreadPoolExecutor, as_completed
            with ThreadPoolExecutor(max_workers=min(MAX_PARALLEL, len(selected_pages))) as pool:
                futs = {}
                for i, pno in enumerate(selected_pages, start=1):
                    b = io.BytesIO()
                    b.write(build_pdf_from_pages(orig_bytes, [pno]))
                    b.seek(0)
                    futs[pool.submit(
                        call_fracto,
                        b.getvalue(),
                        sel_pdf_name.replace("selected.pdf", f"selected_p{i}.pdf"),
                        parser_app=SECOND_PARSER_APP_ID,
                        model=SECOND_MODEL_ID,
                        extra_accuracy=EXTRA_ACCURACY_SECOND,
                    )] = i
                for fut in as_completed(futs):
                    i = futs[fut]
                    try:
                        per_results[i - 1] = fut.result()
                    except Exception as exc:
                        logger.error("Second-pass per-page classification failed @%d: %s", i, exc)
                        per_results[i - 1] = {"file": f"selected_p{i}.pdf", "status": "error", "error": str(exc)}

            classification = []
            org_type_raw = None
            for i, res in enumerate(per_results, start=1):
                pdp = (res.get("data", {}) or {}).get("parsedData", {}) if isinstance(res, dict) else {}
                if not org_type_raw:
                    try:
                        t = (((pdp or {}).get("organisation_type") or {}) or {}).get("type")
                        if t: org_type_raw = t
                    except Exception:
                        pass

                container = _second_pass_container(pdp)
                if isinstance(container, list) and container:
                    it = container[0] if isinstance(container[0], dict) else {}
                    main_dt   = _second_pass_field(it, "doc_type") or _second_pass_field(it, "continuation_of")
                    has_two   = _is_truthy_val(_second_pass_field(it, "has_two"))
                    second_dt = _second_pass_field(it, "second_doc_type")
                else:
                    main_dt   = pdp.get("Document_type") or ""
                    has_two   = _is_truthy_val(pdp.get("Has_multiple_sections"))
                    second_dt = None
                    secs = pdp.get("Sections") or pdp.get("sections") or []
                    for s in (secs if isinstance(secs, list) else []):
                        role = str((s or {}).get("sec_role") or (s or {}).get("role") or "").strip().lower()
                        sdt  = (s or {}).get("sec_doc_type") or (s or {}).get("doc_type")
                        if role == "bottom" and sdt:
                            second_dt = sdt; break
                        if not second_dt and sdt and sdt != main_dt:
                            second_dt = sdt

                classification.append({
                    "page_number": i,
                    "doc_type": main_dt,
                    "has_two": "true" if has_two else "",
                    "second_doc_type": second_dt,
                    "is_continuation": "",
                    "continuation_of": None,
                })

            pd_payload = {"classification": classification}
            if org_type_raw:
                pd_payload["organisation_type"] = {"type": org_type_raw}
            second_res = {"file": sel_pdf_name, "status": "ok", "data": {"parsedData": pd_payload}}

            # Persist synthetic per-page classification JSON when enabled
            if SAVE_SELECTED_JSON:
                selected_json_path = Path(str(pdf_p)).with_name(SELECTED_JSON_NAME_TMPL.format(stem=stem))
                with open(selected_json_path, "w", encoding="utf-8") as fh:
                    json.dump(second_res, fh, indent=2)
                logger.info("Second-pass (per-page) results written to %s", selected_json_path)        
        

        second_pass_end = time.time(); second_pass_time = second_pass_end - _t1
        timings["second_pass"] = {"start": second_pass_start, "end": second_pass_end, "dur": second_pass_time}
        # 6️⃣ Save second JSON as configured
        if SAVE_SELECTED_JSON:
            selected_json_path = Path(str(pdf_p)).with_name(SELECTED_JSON_NAME_TMPL.format(stem=stem))
            with open(selected_json_path, "w", encoding="utf-8") as fh:
                json.dump(second_res, fh, indent=2)
            logger.info("Second-pass results written to %s", selected_json_path)

        # 7️⃣  Third pass – group pages by doc_type and process each group separately
        third_pass_start = time.time(); timings["third_pass"] = {"start": third_pass_start}
        # Robustly handle dict/list shaped parsedData from second pass
        pd_payload = (second_res.get("data", {}) or {}).get("parsedData", {})
        org_type_raw = _second_pass_org_type(pd_payload)
        company_type = normalize_company_type(org_type_raw)
        logger.info("Routing company_type: %s (raw=%r)", company_type, org_type_raw)
        if not org_type_raw:
            try:
                logger.warning(
                    "[routing] organisation_type not found in second-pass payload; defaulting to %s. "
                    "Configure CFG.schema.second_pass.organisation_type (e.g., ['organisation_type.type']) to map it.",
                    company_type,
                )
            except Exception:
                pass
        classification = []
        raw_class = _second_pass_container(pd_payload)
        # Log second-pass classification inputs for routing visibility
        try:
            _dbg_cls = [
                {
                    "page_number": int(_second_pass_field(item, "page_number", i)),
                    "doc_type": _second_pass_field(item, "doc_type"),
                    "second_doc_type": _second_pass_field(item, "second_doc_type"),
                    "has_two": _is_truthy_val(_second_pass_field(item, "has_two")),
                }
                for i, item in enumerate(raw_class, start=1) if isinstance(item, dict)
            ]
            logger.info("[routing] second-pass classification (raw) → %s", _dbg_cls)
        except Exception:
            pass
        for i, item in enumerate(raw_class, start=1):
            if not isinstance(item, dict):
                continue
            main_dt   = _second_pass_field(item, "doc_type")
            has_two   = _is_truthy_val(_second_pass_field(item, "has_two"))
            second_dt = _second_pass_field(item, "second_doc_type")
            classification.append({
                "page_number": int(_second_pass_field(item, "page_number", i)),  # fallback to sequential index if missing
                "doc_type": main_dt,
                "has_two": "true" if has_two else "",
                "second_doc_type": second_dt,
                    "is_continuation": "true" if is_true_flag(_second_pass_field(item, "is_continuation")) else "",
                "continuation_of": _second_pass_field(item, "continuation_of"),
            })
        # Fallback: derive classification from first pass (use selected.pdf indexing)
        if not classification:
            tmp = []
            for sel_idx, orig_pno in enumerate(selected_pages, start=1):
                res = results[orig_pno - 1] or {}
                pdict = (res.get("data", {}) or {}).get("parsedData", {}) or {}
                dt = pdict.get("Document_type")
                if dt and str(dt).strip().lower() != "others":
                    tmp.append({"page_number": sel_idx, "doc_type": dt})
            classification = tmp
        if classification:
            try:
                _unique_types = sorted({
                    (it.get("doc_type") or it.get("continuation_of") or "").strip()
                    for it in classification
                    if (it.get("doc_type") or it.get("continuation_of"))
                })
                logger.info("Third pass: %d unique doc types detected (pre-smoothing) → %s", len(_unique_types), _unique_types)
            except Exception:
                pass

            # Debug: show mapping selected→original with labels (and secondary)
            try:
                _map = []
                for it in classification:
                    sel_no = int(it.get("page_number") or 0)
                    if sel_no <= 0:
                        continue
                    if 1 <= sel_no <= len(selected_pages):
                        orig_p = selected_pages[sel_no - 1]
                    else:
                        # Ignore spurious out-of-range entries from the classifier
                        continue
                    main = normalize_doc_type(it.get("doc_type") or it.get("continuation_of"))
                    sec  = normalize_doc_type(it.get("second_doc_type") or "")
                    flags = []
                    if is_true_flag(it.get("is_continuation")):
                        flags.append("cont")
                    if is_true_flag(it.get("has_two")) or is_true_flag(it.get("Has_multiple_sections")):
                        flags.append("has_two")
                    lab = main or "Others"
                    if sec and sec != "Others" and sec != lab:
                        lab = f"{lab} + {sec}"
                    if flags:
                        lab = f"{lab} ({','.join(flags)})"
                    _map.append(f"{sel_no}→{orig_p}:{lab}")
                if _map:
                    logger.info("Second-pass mapping (sel→orig:label) → %s", "; ".join(_map))
            except Exception:
                pass

            # Robust grouping: use our mixed-page + continuation aware helper (with first-pass fallback)
            groups = build_groups(selected_pages, classification, orig_bytes, first_pass_results=results)

            if groups:
                # Summary line: list all groups with page ranges
                try:
                    _summary = "; ".join(
                        f"{dt}: {format_ranges(sorted(pages))}" for dt, pages in sorted(groups.items())
                    )
                    logger.info("[groups] Third pass groups → %s", _summary)
                except Exception:
                    pass
                # Start timing of group processing (already included in third_pass start)
                _t2 = time.time()
                logger.info("Third pass: processing %d doc_type groups → %s", len(groups), sorted(groups.keys()))

                # Collector for combined sheets
                combined_sheets: dict[str, "pd.DataFrame"] = {}
                # Track which parser/model/accuracy was used per doc_type (for summary sheet)
                routing_used: dict[str, dict] = {}

                combined_rows: dict[str, list[dict]] = {}   # rows per doc_type for JSON
                third_pass_raw: dict[str, list[dict]] = {}  # raw parsedData per doc_type


                # Concurrent upload of each doc_type group (limit = MAX_PARALLEL)
                from iwe_core.pdf_ops import build_pdf_from_pages
                futures = {}

                from concurrent.futures import ThreadPoolExecutor, as_completed
                with ThreadPoolExecutor(max_workers=min(MAX_PARALLEL, len(groups))) as pool:
                    for doc_type, page_list in groups.items():
                        # Preserve original order
                        page_list = sorted(page_list)
                        # Build a slug for filenames
                        slug = (
                            doc_type.lower()
                            .replace(" ", "_")
                            .replace("&", "and")
                            .replace("/", "_")
                        )

                        # Show the final routing keys we are about to use
                        try:
                            logger.info(
                                "[routing] inputs: ct_raw=%r ct=%s doc_raw=%r doc_norm=%s key=%s",
                                org_type_raw,
                                company_type,
                                doc_type,
                                normalize_doc_type(doc_type),
                                normalize_doc_type(doc_type).strip().lower(),
                            )
                        except Exception:
                            pass
                        parser_app, model_id, extra_acc = _resolve_routing(doc_type, company_type=company_type)
                        if parser_app is None:
                            routing_used[doc_type] = {"parser_app": None, "model": None, "extra": None, "company_type": company_type, "skipped": True, "reason": "disabled"}
                            logger.info("↷ Skipping %s via company_type=%s (disabled; no fallback)", doc_type, company_type)
                            continue
                        routing_used[doc_type] = {"parser_app": parser_app, "model": model_id, "extra": extra_acc, "company_type": company_type}
                        logger.info("→ Routing %s via company_type=%s → parser=%s, model=%s, extra=%s, pages=%s",
                                    doc_type, company_type, parser_app, model_id, extra_acc, page_list)

                        if THIRD_COMBINE_PAGES:
                            group_bytes = build_pdf_from_pages(orig_bytes, page_list)

                            fut = pool.submit(
                                call_fracto,
                                group_bytes,
                                CFG.get("export", {}).get("filenames", {}).get("group_pdf", "{stem}_{slug}.pdf").format(stem=stem, slug=slug),
                                parser_app=parser_app,
                                model=model_id,
                                extra_accuracy=extra_acc,
                            )
                            futures[fut] = (doc_type, Path(str(pdf_p)).with_name(CFG.get("export", {}).get("filenames", {}).get("group_json", "{stem}_{slug}_ocr.json").format(stem=stem, slug=slug)), None)
                        else:
                            for pno in page_list:
                                page_bytes = build_pdf_from_pages(orig_bytes, [pno])
                                fut = pool.submit(
                                    call_fracto,
                                    page_bytes,
                                    CFG.get("export", {}).get("filenames", {}).get("group_pdf", "{stem}_{slug}.pdf").format(stem=f"{stem}", slug=f"{slug}_p{pno}"),
                                    parser_app=parser_app,
                                    model=model_id,
                                    extra_accuracy=extra_acc,
                                )
                                futures[fut] = (doc_type, Path(str(pdf_p)).with_name(CFG.get("export", {}).get("filenames", {}).get("group_json", "{stem}_{slug}_ocr.json").format(stem=f"{stem}", slug=f"{slug}_p{pno}")), pno)

                    for fut in as_completed(futures):
                        doc_type, group_json_path, pno = futures[fut]
                        try:
                            group_res = fut.result()
                            with open(group_json_path, "w", encoding="utf-8") as fh:
                                json.dump(group_res, fh, indent=2)
                            parsed = group_res.get("data", {}).get("parsedData", [])

                            # Keep raw parsedData per doc_type
                            try:
                                third_pass_raw.setdefault(doc_type, []).append(parsed)
                            except Exception:
                                pass

                            rows_list = json_ops.extract_rows(parsed)
                            if debug_enabled():
                                try:
                                    oa = [r for r in (rows_list or []) if str(r.get("Particulars","" )).strip().lower() == "other assets"]
                                    dprint(f"rows extracted for {doc_type}: {len(rows_list)} | other_assets={oa[:1]}")
                                except Exception:
                                    pass

                            # Append rows for combined JSON
                            try:
                                combined_rows.setdefault(doc_type, []).extend(rows_list or [])
                            except Exception:
                                pass

                            if rows_list:
                                import pandas as pd
                                all_keys = []
                                for row in rows_list:
                                    for k in row.keys():
                                        if k not in all_keys:
                                            all_keys.append(k)
                                rows = [{k: r.get(k, "") for k in all_keys} for r in rows_list]
                                df = pd.DataFrame(rows, columns=all_keys)
                                df = excel_ops._normalize_df_for_excel(doc_type, df)
                                if debug_enabled():
                                    try:
                                        _sample = df[df["Particulars"].astype(str).str.strip().str.lower()=="other assets"].head(1)
                                        if not _sample.empty:
                                            dprint(f"df after sanitize [{doc_type}] other_assets=", _sample.to_dict("records")[:1])
                                    except Exception:
                                        pass
                                if doc_type in combined_sheets and combined_sheets[doc_type] is not None and not combined_sheets[doc_type].empty:
                                    combined_sheets[doc_type] = pd.concat([combined_sheets[doc_type], df], ignore_index=True)
                                else:
                                    combined_sheets[doc_type] = df
                        except Exception as exc:
                            logger.error("Third-pass (%s) failed: %s", doc_type, exc)
                        except Exception as exc:
                            logger.error("Excel generation for %s failed: %s", doc_type, exc)

                # After all futures, log routing summary
                try:
                    _rlog = {dt: routing_used[dt].get("parser_app") for dt in sorted(routing_used)}
                    logger.info("Third pass routing summary (doc_type → parser_app): %s", _rlog)
                except Exception:
                    pass
                
                # Write a single workbook via the shared writer (includes Validation sheet)
                try:
                    xlsx_path = excel_ops._write_statements_workbook(
                        pdf_path=pdf_path,
                        stem=stem,
                        combined_sheets=combined_sheets,
                        routing_used=routing_used,
                        periods_by_doc=None
                    )
                    logger.info("Combined Excel workbook written to %s", xlsx_path)

                    # Build combined_rows from combined_sheets for JSON emission
                    combined_rows = {
                        k: ([] if (v is None or getattr(v, "empty", False)) else v.to_dict(orient="records"))
                        for k, v in (combined_sheets or {}).items()
                    }
                    try:
                        json_written_path = json_ops.write_statements_json(
                            pdf_path=pdf_path,
                            stem=stem,
                            combined_rows=combined_rows,
                            groups=groups,
                            routing_used=routing_used,
                            company_type=company_type,
                            out_path_override=client_json_out,   # honors CLI [output.json] if provided
                            first_pass_results=results,
                            second_pass_result=second_res,
                            third_pass_raw=third_pass_raw,
                        )
                        logger.info("Combined statements JSON written to %s", json_written_path)
                        # --- Post-JSON: ensure workbook uses period labels (rewrite with labels) ---
                        try:
                            # Re-scan disk for period metadata now that *_ocr.json and combined JSON exist
                            _pdoc, _plabels = _scan_group_jsons_for_periods(pdf_path, stem)
                            print(f"[Main] Rewriting workbook with period labels; docs={list((_plabels or {}).keys())}", flush=True)

                            # Overwrite the workbook with proper headers (this also prints per-sheet rename maps)
                            xlsx_out2 = excel_ops._write_statements_workbook(
                                pdf_path,
                                stem,
                                combined_sheets,
                                routing_used=routing_used,
                                periods_by_doc=_pdoc
                            )
                            logger.info("Rewrote Excel workbook with period labels → %s", xlsx_out2)
                        except Exception as e:
                            logger.error("Post-JSON Excel rewrite failed: %s", e)
                            print(f"[Main] ERROR: post-JSON Excel rewrite failed: {e}", flush=True)
                        # --------------------------------------------------------------------------
                    except Exception as _jexc:
                        logger.error("Failed to write combined statements JSON: %s", _jexc)

                except Exception as exc:
                    logger.error("Failed to write combined Excel workbook: %s", exc)
                finally:
                    # Keep local sub-phase metric but overall third_pass_time computed later
                    pass
    

            else:
                # No groups found — still delegate to shared writer to keep behavior consistent
                try:
                    empty_sheets: dict[str, Any] = {}
                    xlsx_path = excel_ops._write_statements_workbook(
                        pdf_path=pdf_path,
                        stem=stem,
                        combined_sheets=empty_sheets,
                        routing_used={},
                        periods_by_doc={}
                    )
                    logger.info("No groups classified; created empty workbook via writer at %s", xlsx_path)

                    # Also emit an (empty) combined JSON to keep interface consistent
                    try:
                        sheet_order = CFG.get("export", {}).get("statements_workbook", {}).get("sheet_order") \
                            or CFG.get("labels", {}).get("canonical", []) \
                            or []
                        _empty_rows = {sn: [] for sn in sheet_order}
                        _ = json_ops.write_statements_json(
                            pdf_path=pdf_path,
                            stem=stem,
                            combined_rows=_empty_rows,
                            groups={},
                            routing_used={},
                            company_type=company_type,
                            out_path_override=client_json_out,
                            first_pass_results=results,
                            second_pass_result=second_res,
                            third_pass_raw={}
                        )
                        logger.info("Empty combined JSON written alongside empty workbook.")
                    except Exception as _jexc:
                        logger.error("Failed to write empty combined JSON: %s", _jexc)

                except Exception as exc:
                    logger.error("Failed to write empty workbook via writer: %s", exc)

            # save Excel if requested
            if excel_out:
                excel_ops.write_excel_from_ocr(results, excel_out, overrides)

            # Timing summary (per pass and overall, with start/end timestamps)
            overall_end = time.time(); timings["overall"]["end"] = overall_end; timings["overall"]["dur"] = overall_end - overall_start
            # Compute third pass duration from its start to now (covers all branches)
            if "third_pass" in timings and "start" in timings["third_pass"]:
                third_pass_time = overall_end - timings["third_pass"]["start"]
                timings["third_pass"]["end"] = overall_end
                timings["third_pass"]["dur"] = third_pass_time
            else:
                third_pass_time = 0.0

            # Emit detailed timing lines
            try:
                logger.info(
                    "First pass: %.2fs (%s → %s)",
                    first_pass_time,
                    _fmt_ts(timings["first_pass"]["start"]),
                    _fmt_ts(timings["first_pass"]["end"]),
                )
                logger.info(
                    "Second pass: %.2fs (%s → %s)",
                    second_pass_time,
                    _fmt_ts(timings["second_pass"]["start"]),
                    _fmt_ts(timings["second_pass"]["end"]),
                )
                logger.info(
                    "Third pass: %.2fs (%s → %s)",
                    third_pass_time,
                    _fmt_ts(timings["third_pass"].get("start", overall_start)),
                    _fmt_ts(timings["third_pass"].get("end", overall_end)),
                )
                logger.info(
                    "Total: %.2fs (%s → %s)",
                    timings["overall"]["dur"],
                    _fmt_ts(timings["overall"]["start"]),
                    _fmt_ts(timings["overall"]["end"]),
                )
            except Exception:
                pass

            # One-line compact summary for print logs
            try:
                print(
                    "[Timing] First: %.2fs | Second: %.2fs | Third: %.2fs | Total: %.2fs" % (
                        first_pass_time, second_pass_time, third_pass_time, timings["overall"]["dur"]
                    ),
                    flush=True,
                )
                print(
                    "[Timing] Overall window: %s → %s" % (
                        _fmt_ts(timings["overall"]["start"]), _fmt_ts(timings["overall"]["end"])
                    ),
                    flush=True,
                )
            except Exception:
                pass



def _renumber_serials(results: list[dict],
                      json_field: str = "Serial_Number",
                      excel_header: str = "Item No.") -> None:
    """
    Mutates *results* in-place so that every row has a globally increasing
    serial number (1, 2, 3 …) across all Fracto chunks.

    The column name in the JSON is *json_field*; if it differs between your two
    formats, you can look it up via mappings in the caller instead.
    """
    counter = 1
    for res in results:
        rows = json_ops.extract_rows(res.get("data", []))
        for row in rows:
            row[json_field] = counter
            counter += 1


# (Compatibility re-exports removed to avoid pyflakes redefinition warnings.)

# ─── Main Entry Point ────────────────────────────────────────────────────
if __name__ == "__main__":
    # Support a fast path: build workbook directly from third-pass group JSONs
    # Usage:
    #   python a.py from-json <out.xlsx> <group1_ocr.json> [<group2_ocr.json> ...] [--pdf /path/to/original.pdf]
    try:
        argv = sys.argv[1:]
        if argv and argv[0] in {"from-json", "--from-json"}:
            from iwe_core.cli import run_from_json_argv
            sys.exit(run_from_json_argv(argv[1:]))
        if argv and argv[0] in {"analyze", "--analyze"}:
            from iwe_core.cli import run_analyze_argv
            sys.exit(run_analyze_argv(argv[1:]))
    except Exception as _e:
        print("[from-json] Failed:", _e)
    # Default CLI → delegate to pipeline
    try:
        from iwe_core.pipeline import run_cli as _run_cli
        sys.exit(_run_cli(sys.argv[1:]))
    except Exception as _pex:
        print("[pipeline] Failed:", _pex)
