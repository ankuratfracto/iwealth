# a.py

from __future__ import annotations
# from concurrent.futures import ThreadPoolExecutor, as_completed  # imported locally where needed
import re
from pathlib import Path
import yaml, os
import iwe_core.excel_ops as excel_ops
from iwe_core.config import CFG

# Global cache used by Excel header renaming and diagnostics
PERIOD_LABELS_BY_DOC: dict[str, dict] = {}

# Process one page per chunk; use config defaults
CHUNK_SIZE_PAGES = int(CFG.get("concurrency", {}).get("chunk_size_pages", 1))
MAX_PARALLEL     = int(CFG.get("concurrency", {}).get("max_parallel", 9))
MIN_TAIL_COMBINE = int(CFG.get("concurrency", {}).get("min_tail_combine", 1))

def _split_pdf_bytes(pdf_bytes: bytes,
                     chunk_size: int = CHUNK_SIZE_PAGES,
                     min_tail: int = MIN_TAIL_COMBINE) -> list[bytes]:
    """
    Return a list of PDF byte-chunks. Keeps fixed-size blocks, except that a final
    fragment < min_tail pages is merged into the previous chunk to retain context.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than 0")
    if min_tail < 0:
        raise ValueError("min_tail must be non-negative")

    from iwe_core.pdf_ops import build_pdf_from_pages, get_page_count_from_bytes

    total = get_page_count_from_bytes(pdf_bytes)
    if total <= chunk_size:
        return [pdf_bytes]

    # Compute (start,end) 1-based inclusive ranges with tail-merge
    ranges: list[tuple[int, int]] = []
    start = 1
    while start <= total:
        end = min(start + chunk_size - 1, total)
        tail = total - end
        if tail < min_tail and ranges:
            ps, pe = ranges[-1]
            ranges[-1] = (ps, total)
            break
        ranges.append((start, end))
        start = end + 1

    chunks: list[bytes] = []
    for s, e in ranges:
        chunks.append(build_pdf_from_pages(pdf_bytes, range(s, e + 1)))
    return chunks

def call_fracto_parallel(pdf_bytes, file_name, *, extra_accuracy: str = "true") -> list[dict]:
    from iwe_core.ocr_client import call_fracto_parallel as _core_parallel
    return _core_parallel(pdf_bytes, file_name, extra_accuracy=extra_accuracy)
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
import logging
# from pathlib import Path  # already imported above
# typing imported later where needed

from openpyxl.styles import Font, Alignment, PatternFill

from PyPDF2 import PdfReader
from reportlab.pdfgen import canvas

# ─── PDF Stamping Helper ──────────────────────────────────────────────

import re as _re

def reorder_dataframe_sections_first(df):
    """
    Ensure each section's header row appears before its break-up lines, with totals last.

    A row is considered a *header* if EITHER:
      • sr_no looks like a top-level number (e.g., 1, 1.0, 1.00, 12.30), OR
      • it’s a label-only line (non-empty name column, NO numeric amounts), and not a Total/Subtotal.

    Totals/Subtotals are always pushed to the end of their section.
    """
    # Proceed without importing pandas; operate on duck-typed DataFrame
    if df is None or getattr(df, "empty", True):
        return df

    cols = list(df.columns)

    # 1) Identify the "name" / particulars column (case-insensitive)
    name_col = None
    for c in cols:
        if str(c).strip().lower() in {
            "particulars", "description", "item", "line_item", "account", "head", "details"
        }:
            name_col = c
            break
    if name_col is None:
        return df  # no safe way to reorder

    # 2) Numeric columns (c1..cN or columns containing 'amount'/'value')
    num_cols = [c for c in cols if _re.fullmatch(r'(?i)c\d+', str(c))
                or ("amount" in str(c).lower())
                or ("value" in str(c).lower())]
    if not num_cols:
        meta = {name_col, "sr_no", "srno", "serial", "note"}
        num_cols = [c for c in cols if str(c).lower() not in {m.lower() for m in meta}]

    def _is_numlike(v):
        if v is None:
            return False
        if isinstance(v, str) and v.strip() in {"", "-", "–", "—", "na", "n/a", "nil"}:
            return False
        try:
            float(str(v).replace(",", ""))
            return True
        except Exception:
            return False

    n = len(df)
    is_header = [False]*n
    is_total  = [False]*n

    # Accept many variants of the serial column (regex) and provide a fallback
    def _norm(s: str) -> str:
        return str(s or "").strip().lower()

    sr_cols = []
    for c in cols:
        nm = _norm(c)
        if _re.search(r'^(sr\.?\s*no\.?|s\.?\s*no\.?|serial(?:\s*no)?)$', nm):
            sr_cols.append(c)
        elif nm in {"sr_no","srno","serial","serial no","serial_no","s no","s. no.","sno","s.no","sr. no.","sr no"}:
            sr_cols.append(c)
    sr_col = sr_cols[0] if sr_cols else None

    # Fallback: if no explicit sr_no column, try using the left-most column if many values look like 1, 1.0, 1.00, 12.30
    if sr_col is None and len(cols) > 0:
        first_col = cols[0]
        try:
            series = df[first_col].dropna().astype(str).str.strip()
            frac = (series.str.fullmatch(r'\d+(?:\.\d+)?').sum()) / max(len(series), 1)
            if frac >= 0.25:  # at least 25% look like serials → treat as sr_no
                sr_col = first_col
        except Exception:
            pass

    def cell(i, c):
        try:
            return df.iloc[i][c]
        except Exception:
            return None

    for i in range(n):
        name = str(cell(i, name_col) or "").strip()
        tot = bool(_re.match(r'^\s*(total|subtotal|grand\s+total)\b', name.lower()))
        is_total[i] = tot

        # Any numeric value in amount columns?
        num_present = any(_is_numlike(cell(i, c)) for c in num_cols)

        # Header detection:
        #  (a) numeric-style sr_no (1, 1.0, 1.00, 12.30) → header (even if numbers present)
        #  (b) non-empty name with NO numbers and not a total → header
        sr_val = (str(cell(i, sr_col)).strip() if sr_col else "")
        header_by_sr = bool(_re.fullmatch(r'\d+(?:\.\d+)?', sr_val))
        header_by_name_no_num = (name != "") and (not num_present) and (not tot)

        is_header[i] = header_by_sr or header_by_name_no_num

    out_idx, used = [], [False]*n

    def append_details(start, end):
        if start > end:
            return
        block = [j for j in range(start, end+1) if not is_header[j] and not used[j]]
        non_tot = [j for j in block if not is_total[j]]
        tots    = [j for j in block if is_total[j]]
        for j in non_tot + tots:
            out_idx.append(j); used[j] = True

    i = 0
    while i < n:
        if used[i]:
            i += 1; continue
        if is_header[i]:
            out_idx.append(i); used[i] = True
            j = i + 1
            while j < n and not is_header[j]:
                j += 1
            append_details(i+1, j-1)
            i = j
        else:
            # break-up rows before a header → bring the next header forward, then its details
            k = i
            while k < n and not is_header[k]:
                k += 1
            if k < n:
                out_idx.append(k); used[k] = True
                append_details(i, k-1)
                j = k + 1
                while j < n and not is_header[j]:
                    j += 1
                append_details(k+1, j-1)
                i = j
            else:
                append_details(i, n-1)
                i = n

    # Optional debug: set FRACTO_DEBUG_ORDER=1 to see header/total counts and a small preview
    import os as _os
    if _os.getenv("FRACTO_DEBUG_ORDER") == "1":
        try:
            preview_cols = [name_col]
            if sr_col and sr_col != name_col:
                preview_cols = [sr_col] + preview_cols
            print("[ORDERDBG] headers=", sum(is_header), "totals=", sum(is_total), flush=True)
            print("[ORDERDBG] preview (pre):", df[preview_cols].head(12).to_dict("records"), flush=True)
        except Exception:
            pass

    try:
        df_out = df.iloc[out_idx].reset_index(drop=True)
        import os as _os
        if _os.getenv("FRACTO_DEBUG_ORDER") == "1":
            try:
                preview_cols = [name_col]
                if sr_col and sr_col != name_col:
                    preview_cols = [sr_col] + preview_cols
                print("[ORDERDBG] preview (post):", df_out[preview_cols].head(12).to_dict("records"), flush=True)
            except Exception:
                pass
        return df_out
    except Exception:
        return df


# --- Patch: ensure reorder is applied before any DataFrame leaves sanitization

# Try to patch sanitize_statement_df to reorder before every return
import inspect

def _patch_sanitize_statement_df():
    # no local-only imports needed here
    global_vars = globals()
    # Find the function
    fn = global_vars.get("sanitize_statement_df")
    if fn is None:
        # Try to find in __main__ or other imports
        try:
            import __main__
            fn = getattr(__main__, "sanitize_statement_df", None)
        except Exception:
            fn = None
    if fn is None:
        return  # nothing to patch
    src = inspect.getsource(fn)
    lines = src.splitlines()
    new_lines = []
    for line in lines:
        if line.strip().startswith("return df"):
            indent = line[:line.find('return')]
            new_lines.append(f"{indent}df = reorder_dataframe_sections_first(df)")
            new_lines.append(line)
        else:
            new_lines.append(line)
    # Compile new function
    src_new = "\n".join(new_lines)
    # Prepare globals for exec
    g = fn.__globals__.copy()
    g["reorder_dataframe_sections_first"] = reorder_dataframe_sections_first
    # Note: exec in the right context
    exec(src_new, g)
    global_vars["sanitize_statement_df"] = g[fn.__name__]

import os as _os
if _os.getenv("IWEALTH_ENABLE_RUNTIME_PATCHES") == "1":
    try:
        _patch_sanitize_statement_df()
    except Exception:
        pass

# --- Patch: Excel-writing functions (sanitize_statement_df → reorder_dataframe_sections_first)
def _patch_excel_writers():
    import inspect
    global_vars = globals()
    # Find all functions that look like Excel writers
    for name, fn in list(global_vars.items()):
        if not callable(fn) or not inspect.isfunction(fn):
            continue
        src = None
        try:
            src = inspect.getsource(fn)
        except Exception:
            continue
        if "to_excel" not in src and "save_workbook" not in src and "write" not in name:
            continue
        # Look for calls to sanitize_statement_df
        pat = re.compile(r"(df_[a-zA-Z0-9_]*\s*=\s*)?sanitize_statement_df\(([^)]+)\)")
        matches = list(pat.finditer(src))
        if not matches:
            continue
        # Patch after each call
        lines = src.splitlines()
        new_lines = []
        for idx, line in enumerate(lines):
            new_lines.append(line)
            m = pat.search(line)
            if m:
                # Figure out the variable assigned, if any
                assign = m.group(1)
                varname = None
                if assign:
                    varname = assign.split("=")[0].strip()
                else:
                    # try to extract from inside call, e.g. return sanitize_statement_df(...) → can't patch
                    continue
                indent = line[:line.find(m.group(0))]
                # Insert reorder after sanitize_statement_df
                new_lines.append(f"{indent}{varname} = reorder_dataframe_sections_first({varname})")
        # Compile new function
        src_new = "\n".join(new_lines)
        g = fn.__globals__.copy()
        g["reorder_dataframe_sections_first"] = reorder_dataframe_sections_first
        try:
            exec(src_new, g)
            global_vars[name] = g[fn.__name__]
        except Exception:
            pass

if _os.getenv("IWEALTH_ENABLE_RUNTIME_PATCHES") == "1":
    try:
        _patch_excel_writers()
    except Exception:
        pass


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

def _resolve_api_key() -> str:
    """
    Resolve API key in this order:
    1) Env var indicated by api.api_key_env
    2) config.yaml's api.api_key (useful for local dev)
    """
    key = os.getenv(API_KEY_ENV, "")
    if key:
        return key
    if API_KEY_CFG:
        return str(API_KEY_CFG)
    return ""

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

# ─── Generic JSON access & criteria helpers (config‑driven) ─────────────
TRUTHY_SET = {str(x).strip().lower() for x in (CFG.get("truthy_values") or ["true","1","yes","y","on"])}

def _is_truthy_val(v) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    if s in TRUTHY_SET:
        return True
    try:
        return float(s) != 0.0
    except Exception:
        return False

def _json_get_first(obj, path: str):
    """
    Resolve a dotted path like 'parsedData.has_table'. If a list is encountered
    and the next step isn't a numeric index, scan elements and return the first
    successful lookup. Returns None if not found.
    """
    cur = obj
    for step in (path or "").split("."):
        if isinstance(cur, dict):
            if step in cur:
                cur = cur[step]
            else:
                return None
        elif isinstance(cur, list):
            if step.isdigit():
                idx = int(step)
                if 0 <= idx < len(cur):
                    cur = cur[idx]
                else:
                    return None
            else:
                # scan list elements
                found = None
                for el in cur:
                    if isinstance(el, dict) and step in el:
                        found = el[step]
                        break
                if found is None:
                    return None
                cur = found
        else:
            return None
    return cur

def _json_any_truthy(obj, paths: list[str]) -> bool:
    for p in (paths or []):
        val = _json_get_first(obj, p)
        if isinstance(val, list):
            if any(_is_truthy_val(x) for x in val):
                return True
        else:
            if _is_truthy_val(val):
                return True
    return False

def _schema_paths(alias: str) -> list[str]:
    """Lookup a schema alias like 'first_pass.has_table' into a list of paths."""
    node = CFG.get("schema", {})
    for key in (alias or "").split("."):
        if not isinstance(node, dict) or key not in node:
            return []
        node = node[key]
    return list(node) if isinstance(node, list) else ([] if node is None else [str(node)])

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

def _second_pass_container(pd_payload: dict | list) -> list:
    """Return the list of classification rows based on config container paths."""
    if isinstance(pd_payload, list):
        return pd_payload
    paths = (CFG.get("schema", {}).get("second_pass", {}) or {}).get("classification_container") or []
    for p in paths:
        lst = _json_get_first(pd_payload, p)
        if isinstance(lst, list):
            return lst
    return []

def _second_pass_field(item: dict, field_name: str, default=None):
    paths = (CFG.get("schema", {}).get("second_pass", {}).get("fields", {}) or {}).get(field_name) or []
    for p in paths:
        v = _json_get_first(item, p) if "." in p else item.get(p)
        if v is not None and v != "":
            return v
    return default

def _second_pass_org_type(pd_payload: dict | list):
    if isinstance(pd_payload, list):
        return None
    paths = (CFG.get("schema", {}).get("second_pass", {}) or {}).get("organisation_type") or []
    for p in paths:
        v = _json_get_first(pd_payload, p)
        if v:
            return v
    return None

def _resolve_routing(doc_type: str, company_type: str | None = None) -> tuple[str, str, str]:
    """
    Resolve (parser_app, model_id, extra_accuracy) using config routing first,
    then fall back to 'third' defaults.
    """
    dt = (doc_type or "").strip().lower()
    ct = (company_type or _ROUTING_COMPANY_DEFAULT or "corporate").strip().lower()

    def _lookup(ct_key: str, dt_key: str):
        ct_map = _ROUTING_CFG.get(ct_key, {})
        if isinstance(ct_map, dict):
            hit = ct_map.get(dt_key)
            if isinstance(hit, dict):
                # Per-entry toggle (default enabled)
                if str(hit.get("enable", True)).strip().lower() in {"false","0","no","off"}:
                    # Respect skip-on-disabled semantics if enabled
                    if _ROUTING_SKIP_ON_DISABLED:
                        return (None, None, None)
                    return None
                parser = hit.get("parser") or THIRD_PARSER_APP_ID
                model  = hit.get("model")  or THIRD_MODEL_ID
                extra  = str(hit.get("extra", EXTRA_ACCURACY_THIRD)).lower()
                # Global allow/block lists
                if _ROUTING_ALLOWED_PARSERS and parser not in _ROUTING_ALLOWED_PARSERS:
                    return None
                if parser in _ROUTING_BLOCKED_PARSERS:
                    return None
                return (parser, model, extra)
        return None

    for mode in _ROUTING_FALLBACK_ORDER:
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
            return (THIRD_PARSER_APP_ID, THIRD_MODEL_ID, EXTRA_ACCURACY_THIRD)

    return (THIRD_PARSER_APP_ID, THIRD_MODEL_ID, EXTRA_ACCURACY_THIRD)


# ─── Doc-type normalisation & page-text heuristics ────────────────────────

def _canon_text(s: str) -> str:
    """Lowercase + collapse whitespace for robust matching."""
    return re.sub(r"\s+", " ", (s or "").strip().lower())

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

    def _classify(token: str) -> str | None:
        # Avoid misreading "non banking ..." as "bank"
        if "nbfc" in token or "non banking financial" in token or "non-banking financial" in token:
            return "nbfc"
        if "insur" in token:
            return "insurance"
        if "bank" in token and "non banking" not in token and "non-banking" not in token:
            return "bank"
        if "non financial" in token or "corporate" in token or "company" in token:
            return "corporate"
        return None

    for tok in tokens:
        cls = _classify(tok)
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

def extract_page_texts_from_pdf_bytes(pdf_bytes: bytes) -> list[str]:
    """Return plain text per page using PyPDF2's extract_text (best-effort)."""
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
        is_cont = str(item.get("is_continuation", "")).lower() == "true"
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
            is_cont = str(item.get("is_continuation", "")).lower() == "true"
            if not is_cont or not isinstance(sel_no, int):
                continue
            if 1 <= sel_no <= len(selected_pages):
                cont_orig_pages.add(selected_pages[sel_no - 1])
            else:
                # Out-of-range selected index; ignore
                continue

    # Track which original pages were flagged as 'has_two' (mixed page)
    def _is_true(x): 
        return str(x).strip().lower() in ("true","1","yes","y","on")
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

from typing import Any, List, Dict

def _debug_flag_from_cfg(env_name: str, cfg_key: str, default: bool = False) -> bool:
    """Env overrides config. Config flag under CFG['debug'][cfg_key]."""
    try:
        env = os.getenv(env_name)
        if env is not None:
            s = str(env).strip().lower()
            return s in {"1","true","yes","y","on"}
        dbg = (CFG.get("debug", {}) or {}).get(cfg_key)
        # Use existing truthy parser
        return _is_truthy_val(dbg) if dbg is not None else bool(default)
    except Exception:
        return bool(default)

def _debug_enabled() -> bool:
    return _debug_flag_from_cfg("IWEALTH_DEBUG_JSON", "json_extraction", False)

def _dprint(*args, **kwargs):
    if _debug_enabled():
        try:
            print("[JSONDBG]", *args, **kwargs, flush=True)
        except Exception:
            pass

def _valdbg_enabled() -> bool:
    return _debug_flag_from_cfg("IWEALTH_DEBUG_VALIDATION", "validation", False)

def _vprint(*args, **kwargs):
    if _valdbg_enabled():
        try:
            print("[VALDBG]", *args, **kwargs, flush=True)
        except Exception:
            pass

def _extract_rows(parsed: Any, doc_type: str | None = None) -> List[Dict[str, Any]]:
    """
    Flatten Fracto parsedData into a simple list of row dicts.

    Supports shapes like:
      • dict-of-dicts-of-lists (e.g., Balance Sheet sections with c1..c4)
      • dict with 'breakup' sub-rows (e.g., Cashflow)
      • list of rows (already flat)
    Skips meta/report scaffolding keys.
    """
    rows: List[Dict[str, Any]] = []
    current_section: str = ""

    def _add_row(d: Dict[str, Any]) -> None:
        if not isinstance(d, dict):
            return
        kset = {str(k).strip().lower() for k in d.keys()}
        has_any_c = any(re.fullmatch(r"c\d+", k) for k in kset)
        is_data_row = ("particulars" in kset) or (("sr_no" in kset) and has_any_c)
        if not is_data_row:
            return

        row: Dict[str, Any] = {}
        # Preserve structural hints if present
        for meta_key in ("id", "row_type", "parent_id", "components"):
            if meta_key in d:
                row[meta_key] = d.get(meta_key)
        # Prefer explicit section_id if provided; else use derived current_section
        try:
            _sec = d.get("section_id") or d.get("sectionId")
        except Exception:
            _sec = None
        if _sec:
            row["section"] = _sec
        elif current_section:
            row["section"] = current_section
        # Keep sr_no if present
        for cand in ("sr_no", "Sr_no", "srNo", "SrNo"):
            if cand in d:
                row["sr_no"] = d.get(cand)
                break

        # Unify particulars -> "Particulars"
        part = (
            d.get("particulars")
            or d.get("Particulars")
            or d.get("description")
            or d.get("Description")
            or d.get("head")
            or d.get("Head")
            or ""
        )
        row["Particulars"] = part

        # Copy numeric columns c1..cN dynamically (case-insensitive)
        for ck in sorted(
            [k for k in d.keys() if re.match(r"^[cC]\d+$", str(k))],
            key=lambda x: int(re.findall(r"\d+", str(x))[0])
        ):
            row[str(ck).lower()] = d.get(ck)

        rows.append(row)
        # Debug spotlight for common problem lines
        try:
            p = str(row.get("Particulars", "")).strip().lower()
            if _debug_enabled() and any(key in p for key in ["other assets","total current assets","equity attributable"]):
                _dprint("extract_rows: row", {k: row.get(k) for k in ("Particulars","c1","c2","c3","c4")})
        except Exception:
            pass

    def _walk(x: Any, ancestors: list[str] | None = None) -> None:
        nonlocal current_section
        if ancestors is None:
            ancestors = []
        if isinstance(x, dict):
            # Header-first: add this node as a potential row *before* visiting children.
            _add_row(x)
            for k, v in x.items():
                if k in {"meta", "statement_type", "framework", "scope", "report"}:
                    continue
                if isinstance(v, (dict, list)):
                    prev = current_section
                    # if descending into a list field under ASSETS or EQUITY_AND_LIABILITIES, set section hint
                    if isinstance(v, list):
                        top = ancestors[-1] if ancestors else None
                        if top in {"ASSETS", "EQUITY_AND_LIABILITIES"}:
                            current_section = f"{top}:{k}"
                        elif not top and k in {"ASSETS", "EQUITY_AND_LIABILITIES"}:
                            current_section = k
                    _walk(v, ancestors + [k])
                    current_section = prev
        elif isinstance(x, list):
            for it in x:
                _walk(it, ancestors)
        else:
            return

    _walk(parsed, [])
    return rows

def sanitize_statement_df(doc_type: str, df: "pd.DataFrame") -> "pd.DataFrame":
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
    try:
        import os as _os
        if str(_os.getenv("IWEALTH_ENABLE_REORDER", "0")).strip() in {"1", "true", "yes"}:
            out = reorder_dataframe_sections_first(out)
    except Exception:
        pass
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
def _doc_type_from_payload(pd_payload: dict | list) -> str | None:
    """
    Best‑effort canonical doc‑type from a parsedData payload using either
    general_metadata or meta blocks. Falls back to None.
    """
    if not isinstance(pd_payload, dict):
        return None
    gm = (pd_payload.get("general_metadata") or {}) if isinstance(pd_payload.get("general_metadata"), dict) else {}
    mm = (pd_payload.get("meta") or {}) if isinstance(pd_payload.get("meta"), dict) else {}
    scope = (gm.get("scope") or mm.get("scope") or "").strip()
    stype = (gm.get("statement_type") or mm.get("statement_type") or "").strip()
    label = f"{scope} {stype}".strip() if stype else stype
    return normalize_doc_type(label) if label else None

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



def _write_statements_json(
    pdf_path: str,
    stem: str,
    combined_rows: dict[str, list[dict]],
    groups: dict[str, list[int]] | None,
    routing_used: dict[str, dict] | None,
    company_type: str | None,
    out_path_override: str | None = None,
    first_pass_results: list[dict] | None = None,
    second_pass_result: dict | None = None,
    third_pass_raw: dict[str, list[dict]] | None = None,
) -> str:
    """
    Write a single combined JSON containing only:
      - Consolidated/Standalone Balance Sheet
      - Consolidated/Standalone Profit and Loss Statement
      - Consolidated/Standalone Cashflow
    Structure:
    {
      "file": "<original.pdf>",
      "status": "ok",
      "company_type": "bank|nbfc|insurance|corporate",
      "documents": {
         "<Canonical Doc Type>": {
            "rows": [ {...}, ... ],
            "pages": [10, 11],
            "parser_app": "...",
            "model": "tv7",
            "extra_accuracy": true
         },
         ...
      }
    }
    """

    # Collect period maps per document type from third-pass raw payloads (if available)
    periods_by_doctype: dict[str, dict] = {}
    _labels_for_excel: dict[str, dict] = {}
    try:
        if third_pass_raw:
            # Expected shape: {doc_type -> [ {"data": {"parsedData": {...}}}, ... ]}
            for _dt_key, _res_list in (third_pass_raw or {}).items():
                dt_norm = normalize_doc_type(_dt_key)
                candidates = _res_list if isinstance(_res_list, list) else [_res_list]
                for _res in candidates:
                    if not isinstance(_res, dict):
                        continue
                    pd_payload = ((_res.get("data") or {}).get("parsedData") or {})
                    by_id, labels = _extract_period_maps_from_payload(pd_payload)
                    if by_id:
                        periods_by_doctype[dt_norm] = by_id
                        _labels_for_excel[dt_norm] = {k.lower(): v for k, v in labels.items()}
                        break
        # Expose labels for Excel header renaming
        if _labels_for_excel:
            #global PERIOD_LABELS_BY_DOC
            for _k, _v in _labels_for_excel.items():
                PERIOD_LABELS_BY_DOC[_k] = _v
    except Exception:
        # Never fail JSON export due to period extraction issues
        pass

    # Fallback: also scan any saved group JSONs on disk to enrich periods & labels
    try:
        _by_doc, _labels = _scan_group_jsons_for_periods(pdf_path, stem)
        if _by_doc:
            for _k, _v in _by_doc.items():
                periods_by_doctype.setdefault(_k, {}).update(_v)
        if _labels:
            # global PERIOD_LABELS_BY_DOC
            for _k, _v in _labels.items():
                PERIOD_LABELS_BY_DOC.setdefault(_k, {}).update(_v)
    except Exception:
        # Don't fail if group JSONs are missing or unreadable
        pass


    allowed = [lbl for lbl in (CFG.get("labels", {}).get("canonical", []) or []) if lbl != "Others"]

    def _coerce_row_numbers(row: dict) -> dict:
        """Coerce any cN/pN values in a shallow row dict to numbers when possible.
        Preserves empty strings as empty, and non-parsable values as-is.
        """
        out = dict(row)
        try:
            import re as _re
            for k in list(out.keys()):
                if _re.fullmatch(r"(?i)[cp]\d+", str(k)):
                    v = out[k]
                    nv = _coerce_number_like(v)
                    # Keep original if completely empty; otherwise use numeric or None
                    if v in ("", None):
                        continue
                    out[k] = nv if nv is not None else out[k]
        except Exception:
            pass
        return out
    docs: dict[str, dict] = {}
    for doc_type in allowed:
        rows = combined_rows.get(doc_type) or []
        if _debug_enabled():
            try:
                _dprint(f"pre-coerce rows for {doc_type}: {len(rows)}")
                _oa0 = [r for r in rows if str(r.get("Particulars","" )).strip().lower()=="other assets"]
                if _oa0:
                    _dprint("pre-coerce 'Other assets':", _oa0[:1])
            except Exception:
                pass
        # Coerce numeric-like strings to numbers so JSON is consistent and Excel sums are robust downstream
        try:
            rows = [_coerce_row_numbers(r) for r in rows]
        except Exception:
            pass
        if _debug_enabled():
            try:
                _oa1 = [r for r in rows if str(r.get("Particulars","" )).strip().lower()=="other assets"]
                if _oa1:
                    _dprint("post-coerce 'Other assets':", _oa1[:1])
            except Exception:
                pass
        if not rows:
            continue
        meta = (routing_used or {}).get(doc_type, {}) if routing_used else {}
        page_list = (groups or {}).get(doc_type, []) if groups else []
        entry = {
            "pages": page_list,
            "parser_app": meta.get("parser_app", ""),
            "model": meta.get("model", ""),
            "extra_accuracy": meta.get("extra", ""),
            "periods": periods_by_doctype.get(doc_type, {}),
        }
        include_rows = bool((CFG.get("export", {}).get("combined_json", {}) or {}).get("include_rows", True))
        if include_rows:
            if _debug_enabled():
                try:
                    oa = [r for r in rows if str(r.get("Particulars","" )).strip().lower()=="other assets"]
                    tca = [r for r in rows if str(r.get("Particulars","" )).strip().lower()=="total current assets"]
                    _dprint(f"statements.json rows for {doc_type}: {len(rows)} | other_assets={oa[:1]} | total_current_assets={tca[:1]}")
                except Exception:
                    pass
            entry["rows"] = rows
        docs[doc_type] = entry

    out = {
        "file": Path(pdf_path).name,
        "status": "ok",
        "company_type": company_type or "",
        "documents": docs,
    }
    try:
        _dbg_docs = {k: sorted((v or {}).get("periods", {}).keys()) for k, v in (docs or {}).items()}
        logger.info("Combined JSON: periods per doc → %s", _dbg_docs)
    except Exception:
        pass

    # Optionally include additional sections based on config flags
    combined_json_cfg = (CFG.get("export", {}).get("combined_json", {}) or {})
    if combined_json_cfg.get("include_first_pass") and first_pass_results is not None:
        out["first_pass"] = first_pass_results
    if combined_json_cfg.get("include_second_pass") and second_pass_result is not None:
        out["second_pass"] = second_pass_result
    if combined_json_cfg.get("include_third_pass_raw") and third_pass_raw:
        out["third_pass"] = third_pass_raw

    # Prefer export.combined_json.filename, fallback to export.filenames.statements_json, then default
    combined_json_cfg = (CFG.get("export", {}).get("combined_json", {}) or {})
    json_name_tmpl = combined_json_cfg.get("filename") \
        or CFG.get("export", {}).get("filenames", {}).get("statements_json") \
        or "{stem}_statements.json"

    if out_path_override:
        out_path = Path(out_path_override).expanduser().resolve()
    else:
        out_path = Path(pdf_path).with_name(json_name_tmpl.format(stem=stem))
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=2)
    logger.info("Combined JSON written to %s", out_path)
    return str(out_path)

logger = logging.getLogger("FractoPageOCR")
_lvl = str(CFG.get("logging", {}).get("level", "INFO")).upper()
logging.basicConfig(
    level=getattr(logging, _lvl, logging.INFO),
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

import sys
print(f"[BOOT] Running script: {__file__}", flush=True)
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


def call_fracto(
    file_bytes: bytes,
    file_name: str,
    *legacy_args,
    parser_app: str = PARSER_APP_ID,
    model: str = MODEL_ID,
    extra_accuracy: str = "true",
    qr_range: str | None = None,
) -> Dict[str, Any]:
    """Thin wrapper delegating to iwe_core.ocr_client.call_fracto."""
    from iwe_core.ocr_client import call_fracto as _core_call
    return _core_call(
        file_bytes,
        file_name,
        *legacy_args,
        parser_app=parser_app,
        model=model,
        extra_accuracy=extra_accuracy,
        qr_range=qr_range,
    )




# ─── Helper to persist results ───────────────────────────────────────────
def save_results(results: List[Dict[str, Any]], pdf_path: str, out_path: str | None = None) -> str:
    """
    Persist OCR results to disk.

    If *out_path* is None, a file named "<original‑stem>_ocr.json" is created
    alongside the input PDF.

    Returns the absolute path to the saved file.
    """
    if out_path is None:
        p = Path(pdf_path).expanduser().resolve()
        out_path = p.with_name(f"{p.stem}_ocr.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(results, fh, indent=2)
    logger.info("Results written to %s", out_path)
    return str(out_path)


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
        python -m mcc <pdf-path> [output.json] [output.xlsx] [KEY=VALUE ...]

    Convenience:
        • If you pass only two arguments and the second one ends with .xlsx / .xlsm / .xls,
          it is treated as the Excel output, and the JSON will default to
          "<pdf‑stem>_ocr.json" next to the PDF.
        • Any KEY=VALUE pairs will be written or overwritten in every row of the Excel output.
    """
    if len(sys.argv) < 2:
        print("Usage: python -m mcc <pdf-path> [output.json] [output.xlsx] [KEY=VALUE ...]")
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
        stem = Path(pdf_path).expanduser().resolve().stem
        logger.info("Output stem derived: %s", stem)
    except Exception:
        stem = Path(pdf_path).stem

    # Preflight: ensure API key is present
    if not _resolve_api_key():
        logger.error("No API key found. Set %s or add api.api_key in config.yaml", API_KEY_ENV)
        sys.exit(3)

    # Timing variables
    overall_start = time.time()
    first_pass_time = 0.0
    second_pass_time = 0.0
    third_pass_time = 0.0

    # 1️⃣ First‑pass OCR (page‑level classification)
    _t0 = time.time()
    results = process_pdf(pdf_path)
    first_pass_time = time.time() - _t0
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
                json_path = excel_ops._write_statements_json(
                    pdf_path, stem,
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
                json_path = excel_ops._write_statements_json(
                    pdf_path, stem,
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
    save_results(results, pdf_path, None)

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

        _t1 = time.time()
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
                selected_json_path = Path(pdf_path).with_name(SELECTED_JSON_NAME_TMPL.format(stem=stem))
                with open(selected_json_path, "w", encoding="utf-8") as fh:
                    json.dump(second_res, fh, indent=2)
                logger.info("Second-pass (per-page) results written to %s", selected_json_path)        
        

        second_pass_time = time.time() - _t1
        # 6️⃣ Save second JSON as configured
        if SAVE_SELECTED_JSON:
            selected_json_path = Path(pdf_path).with_name(SELECTED_JSON_NAME_TMPL.format(stem=stem))
            with open(selected_json_path, "w", encoding="utf-8") as fh:
                json.dump(second_res, fh, indent=2)
            logger.info("Second-pass results written to %s", selected_json_path)

        # 7️⃣  Third pass – group pages by doc_type and process each group separately
        # Robustly handle dict/list shaped parsedData from second pass
        pd_payload = (second_res.get("data", {}) or {}).get("parsedData", {})
        org_type_raw = _second_pass_org_type(pd_payload)
        company_type = normalize_company_type(org_type_raw)
        logger.info("Routing company_type: %s (raw=%r)", company_type, org_type_raw)
        classification = []
        raw_class = _second_pass_container(pd_payload)
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
                "is_continuation": "true" if _is_truthy_val(_second_pass_field(item, "is_continuation")) else "",
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
                    if str(it.get("is_continuation", "")).lower() == "true":
                        flags.append("cont")
                    if str(it.get("has_two", "")).lower() == "true" or str(it.get("Has_multiple_sections", "")).lower() == "true":
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

                        parser_app, model_id, extra_acc = _resolve_routing(doc_type, company_type=company_type)
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
                            futures[fut] = (doc_type, Path(pdf_path).with_name(CFG.get("export", {}).get("filenames", {}).get("group_json", "{stem}_{slug}_ocr.json").format(stem=stem, slug=slug)), None)
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
                                futures[fut] = (doc_type, Path(pdf_path).with_name(CFG.get("export", {}).get("filenames", {}).get("group_json", "{stem}_{slug}_ocr.json").format(stem=f"{stem}", slug=f"{slug}_p{pno}")), pno)

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

                            rows_list = _extract_rows(parsed)
                            if _debug_enabled():
                                try:
                                    oa = [r for r in (rows_list or []) if str(r.get("Particulars","" )).strip().lower() == "other assets"]
                                    _dprint(f"rows extracted for {doc_type}: {len(rows_list)} | other_assets={oa[:1]}")
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
                                if _debug_enabled():
                                    try:
                                        _sample = df[df["Particulars"].astype(str).str.strip().str.lower()=="other assets"].head(1)
                                        if not _sample.empty:
                                            _dprint(f"df after sanitize [{doc_type}] other_assets=", _sample.to_dict("records")[:1])
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
                        json_written_path = excel_ops._write_statements_json(
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
                    third_pass_time = time.time() - _t2
    

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
                        _ = excel_ops._write_statements_json(
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

            # Timing summary
            total_time = time.time() - overall_start
            logger.info(
                "Timing summary → First pass: %.2fs | Second pass: %.2fs | Third pass: %.2fs | Total: %.2fs",
                first_pass_time, second_pass_time, third_pass_time, total_time
            )



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
        rows = _extract_rows(res.get("data", []))
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
            out_xlsx = None
            pdf_hint = None
            jsons = []
            it = iter(argv[1:])
            for tok in it:
                if tok == "--pdf":
                    try:
                        pdf_hint = next(it)
                    except StopIteration:
                        pdf_hint = None
                    continue
                if out_xlsx is None and tok.lower().endswith((".xlsx",".xlsm",".xls")):
                    out_xlsx = tok
                    continue
                jsons.append(tok)
            if not jsons:
                print("from-json mode: provide one or more *_ocr.json files")
                sys.exit(1)
            base = Path(jsons[0]).expanduser().resolve()
            if out_xlsx is None:
                out_xlsx = str(base.with_name(f"{base.stem.replace('_ocr','')}_statements.xlsx"))
            stub_pdf = pdf_hint or str(base.with_suffix('.pdf'))
            # Build combined_sheets and periods map
            import pandas as pd, json as _json
            combined_sheets: dict[str, pd.DataFrame] = {}
            periods_by_doc: dict[str, dict] = {}
            for jp in jsons:
                p = Path(jp).expanduser().resolve()
                try:
                    obj = _json.loads(Path(p).read_text(encoding='utf-8'))
                except Exception:
                    print(f"[from-json] Skip unreadable JSON: {p}")
                    continue
                pd_payload = ((obj.get('data') or {}).get('parsedData')) or obj.get('parsedData') or {}
                if not isinstance(pd_payload, (dict,list)):
                    continue
                # Derive doc type and normalize
                try:
                    dt_guess = _doc_type_from_payload(pd_payload)
                except Exception:
                    dt_guess = None
                if not dt_guess:
                    slug = p.stem
                    dt_guess = normalize_doc_type(slug.replace('_',' '))
                doc_type = normalize_doc_type(dt_guess)
                # Extract rows and make DF
                rows = _extract_rows(pd_payload)
                if not rows:
                    continue
                # Union all keys for stable columns
                all_keys = []
                for r in rows:
                    for k in r.keys():
                        if k not in all_keys:
                            all_keys.append(k)
                df = pd.DataFrame([{k: r.get(k, '') for k in all_keys} for r in rows], columns=all_keys)
                df = sanitize_statement_df(doc_type, df)
                combined_sheets[doc_type] = df
                # Periods
                try:
                    by_id, _labels = _extract_period_maps_from_payload(pd_payload if isinstance(pd_payload, dict) else {})
                    if by_id:
                        periods_by_doc[doc_type] = by_id
                except Exception:
                    pass
            if not combined_sheets:
                print("from-json mode: no sheets built from provided JSONs")
                sys.exit(2)
            # Call shared workbook writer
            stem = Path(out_xlsx).stem.replace('_statements','')
            xlsx_path = excel_ops._write_statements_workbook(stub_pdf, stem, combined_sheets, routing_used=None, periods_by_doc=periods_by_doc)
            # Move/rename to desired out_xlsx if different
            try:
                out_xlsx_p = Path(out_xlsx).expanduser().resolve()
                if str(out_xlsx_p) != str(Path(xlsx_path).expanduser().resolve()):
                    Path(xlsx_path).replace(out_xlsx_p)
                    xlsx_path = str(out_xlsx_p)
            except Exception:
                pass
            print(f"[from-json] Workbook written → {xlsx_path}")
            sys.exit(0)
    except Exception as _e:
        print("[from-json] Failed:", _e)
    # Default CLI
    _cli()
