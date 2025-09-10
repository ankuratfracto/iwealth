"""JSON transformation, period extraction, and combined export helpers.

Flattens OCR `parsedData` structures into row lists, extracts period
metadata across varied shapes, scans per‑group artifacts for labels, and
writes a consolidated JSON artifact alongside the generated workbook.
"""

from __future__ import annotations

from typing import List, Dict, Any, Tuple
from pathlib import Path
import json
import logging
import re

from iwe_core.config import CFG
from iwe_core.grouping import normalize_doc_type

logger = logging.getLogger(__name__)


# ----- Debug helpers (centralized) -----------------------------------------


# ----- Small utils -----------------------------------------------------------
def _coerce_number_like(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if s == "" or s.lower() in {"na","n/a","nil","none","nan","-","–","—"}:
        return None
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    try:
        s = re.sub(r"[^0-9.\-]", "", s)
    except Exception:
        pass
    try:
        v = float(s)
        return -v if neg else v
    except Exception:
        return None


# ----- Row/period extraction -------------------------------------------------
def extract_rows(parsed: Any, doc_type: str | None = None) -> List[Dict[str, Any]]:
    """Flatten Fracto parsedData into a simple list of row dicts.

    Handles common shapes:
      - list[dict]
      - dict of lists
      - nested dicts that contain lists (recursive walk)
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
        # Promote common meta keys and normalise calculation references
        for meta_key in ("id", "row_type", "parent_id"):
            if meta_key in d:
                row[meta_key] = d.get(meta_key)
        # New name: calculation_references (accept legacy 'components')
        if "calculation_references" in d:
            row["calculation_references"] = d.get("calculation_references")
        elif "components" in d:
            row["calculation_references"] = d.get("components")
        try:
            _sec = d.get("section_id") or d.get("sectionId")
        except Exception:
            _sec = None
        row["section"] = str(_sec or current_section or "").strip()
        for k, v in d.items():
            key = str(k).strip()
            if key.lower() in {"id","row_type","parent_id","components","calculation_references","section","section_id","sectionId"}:
                continue
            row[key] = v
        rows.append(row)

    if isinstance(parsed, dict):
        # Direct dict-of-lists
        for k, v in parsed.items():
            if isinstance(v, list):
                current_section = str(k)
                for item in v:
                    _add_row(item)
        current_section = ""
        # Common "rows" container
        if "rows" in parsed and isinstance(parsed["rows"], list):
            for item in parsed["rows"]:
                _add_row(item)
        # Fallback: recursive walk to find nested lists
        if not rows:
            def _walk(node, sec: str = ""):
                nonlocal current_section
                if isinstance(node, dict):
                    for k, v in node.items():
                        if isinstance(v, list):
                            prev = current_section
                            current_section = str(k) or sec or current_section
                            for item in v:
                                _add_row(item)
                            current_section = prev
                        elif isinstance(v, dict):
                            _walk(v, sec or str(k))
                elif isinstance(node, list):
                    for item in node:
                        _walk(item, sec)
            _walk(parsed)

    elif isinstance(parsed, list):
        for item in parsed:
            _add_row(item)
    return rows


def extract_period_maps_from_payload(pd_payload: dict | list) -> Tuple[dict[str, dict], dict[str, str]]:
    """Extract period metadata from parsedData payload.

    Supports both shapes:
      - dict: { periods: {"c1": {"label": "..."}, ...} }
      - list: { meta: { periods: [{"id": "c1", "label": "..."}, ...] } }
    Also tolerates periods under either `meta` or `general_metadata`.
    """
    if not isinstance(pd_payload, dict):
        return {}, {}

    def _find_periods(obj: dict) -> Any:
        # Direct
        p = obj.get("periods")
        if p:
            return p
        # Under meta/general_metadata
        meta = obj.get("meta") or {}
        if isinstance(meta, dict) and meta.get("periods"):
            return meta.get("periods")
        gm = obj.get("general_metadata") or {}
        if isinstance(gm, dict) and gm.get("periods"):
            return gm.get("periods")
        return None

    periods = _find_periods(pd_payload)

    by_id: dict[str, dict] = {}
    labels: dict[str, str] = {}

    # Dict shape: {"c1": {..}, "c2": {..}}
    if isinstance(periods, dict):
        for cid, meta in (periods or {}).items():
            key = str(cid).lower()
            if isinstance(meta, dict):
                by_id[key] = dict(meta)
                labels[key] = str((meta or {}).get("label", ""))
            else:
                by_id[key] = {}
                labels[key] = str(meta)

    # List shape: [{"id": "c1", "label": "..."}, ...]
    elif isinstance(periods, list):
        for item in periods:
            if not isinstance(item, dict):
                continue
            cid = str(item.get("id") or "").strip().lower()
            if not cid:
                continue
            by_id[cid] = dict(item)
            labels[cid] = str(item.get("label", ""))

    return by_id, labels


def build_periods_map_from_third(third_pass_raw: dict[str, list[dict]] | None) -> dict[str, dict]:
    out: dict[str, dict] = {}
    try:
        for _dt_key, _res_list in (third_pass_raw or {}).items():
            dt = normalize_doc_type(_dt_key)
            out.setdefault(dt, {})
            candidates = _res_list if isinstance(_res_list, list) else [_res_list]
            for _res in candidates:
                if not isinstance(_res, dict):
                    continue
                pd_payload = ((_res.get("data") or {}).get("parsedData") or {})
                cmap, _ = extract_period_maps_from_payload(pd_payload)
                if cmap:
                    for cid, meta in (cmap or {}).items():
                        try:
                            out[dt][str(cid).lower()] = (meta or {}).get("label", "")
                        except Exception:
                            out[dt][str(cid).lower()] = ""
                    break
    except Exception:
        pass
    return out


def doc_type_from_payload(pd_payload: dict | list) -> str | None:
    """
    Infer a canonical doc type from parsedData payload.

    Primary source is meta/general_metadata: use `scope` (e.g., Consolidated)
    and `statement_type`. However, many issuers label P&L as
    "Statement of Financial Results"; map that to Profit & Loss.

    Fallbacks:
      - If meta label doesn't resolve to a known canonical, infer from payload
        keys (e.g., presence of Profit_and_Loss / cash flow sections / BS keys).
    """
    if not isinstance(pd_payload, dict):
        return None

    gm = (pd_payload.get("general_metadata") or {}) if isinstance(pd_payload.get("general_metadata"), dict) else {}
    mm = (pd_payload.get("meta") or {}) if isinstance(pd_payload.get("meta"), dict) else {}
    scope_raw = (gm.get("scope") or mm.get("scope") or "").strip()
    stype_raw = (gm.get("statement_type") or mm.get("statement_type") or "").strip()

    # Normalize helpers
    def _canon(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "").strip().lower())

    scope_c = _canon(scope_raw)
    stype_c = _canon(stype_raw)

    # Map common statement_type variants to a base doc kind
    base: str | None = None
    if stype_c:
        if ("profit" in stype_c and "loss" in stype_c) or ("statement of profit" in stype_c):
            base = "Profit and Loss Statement"
        elif ("balance" in stype_c and "sheet" in stype_c) or ("assets and liabilities" in stype_c):
            base = "Balance Sheet"
        elif ("cash" in stype_c and "flow" in stype_c):
            base = "Cashflow"
        elif "financial results" in stype_c:
            # Indian results releases frequently label P&L this way
            base = "Profit and Loss Statement"

    if base:
        prefix = "Consolidated " if "consolidated" in scope_c else ("Standalone " if "standalone" in scope_c else "")
        label = f"{prefix}{base}".strip()
        out = normalize_doc_type(label)
        if out and out != "Others":
            return out

    # Fallback: try combining scope + stype and normalise
    if stype_raw:
        combo = f"{scope_raw} {stype_raw}".strip()
        out = normalize_doc_type(combo)
        if out and out != "Others":
            return out

    # Last-resort: infer from payload structure/keys
    try:
        keys = {str(k).strip().lower() for k in (pd_payload or {}).keys()}
    except Exception:
        keys = set()
    # Look inside one level for dict-of-lists conventions
    nested_keys = set()
    try:
        for v in (pd_payload or {}).values():
            if isinstance(v, dict):
                nested_keys.update(str(k).strip().lower() for k in v.keys())
    except Exception:
        pass
    all_keys = keys | nested_keys

    base2: str | None = None
    if any("profit_and_loss" in k for k in all_keys) or ("profit" in " ".join(all_keys) and "loss" in " ".join(all_keys)):
        base2 = "Profit and Loss Statement"
    elif ("cashflow" in " ".join(all_keys)) or ("cash" in " ".join(all_keys) and "flow" in " ".join(all_keys)):
        base2 = "Cashflow"
    elif ("equity_and_liabilities" in all_keys) or ("assets" in " ".join(all_keys) and "liabilit" in " ".join(all_keys)):
        base2 = "Balance Sheet"

    if base2:
        prefix = "Consolidated " if "consolidated" in scope_c else ("Standalone " if "standalone" in scope_c else "")
        label = f"{prefix}{base2}".strip()
        return normalize_doc_type(label)

    return None


def scan_group_jsons_for_periods(pdf_path: str, stem: str) -> tuple[dict[str, dict], dict[str, dict]]:
    by_doc: dict[str, dict] = {}
    labels_by_doc: dict[str, dict] = {}
    try:
        base = Path(pdf_path).expanduser().resolve()
        folders = []
        parent = base.parent
        if parent.exists():
            folders.append(parent)
        # Also check one level up to tolerate layout mismatches
        if parent.parent and parent.parent.exists() and parent.parent != parent:
            folders.append(parent.parent)
        seen: set[Path] = set()
        def _ingest_json(path):
            nonlocal by_doc, labels_by_doc
            if path in seen:
                return
            seen.add(path)
            try:
                obj = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                return
            pd_payload = ((obj.get("data") or {}).get("parsedData")) or obj.get("parsedData") or None
            if not isinstance(pd_payload, dict):
                return
            by_id, labels = extract_period_maps_from_payload(pd_payload)
            if not (by_id or labels):
                return
            # Guess doc type from payload; fallback from slug
            dt_guess = doc_type_from_payload(pd_payload) or normalize_doc_type(path.stem.replace('_ocr','').replace('_',' '))
            dt = normalize_doc_type(dt_guess)
            if by_id:
                by_doc.setdefault(dt, {}).update(by_id)
            if labels:
                labels_by_doc.setdefault(dt, {}).update({str(k).lower(): str(v) for k, v in labels.items()})

        for folder in folders:
            # Primary: files that match the current stem
            for p in folder.glob(f"{stem}_*_ocr.json"):
                _ingest_json(p)
            # Fallback: if nothing found for this stem, ingest any *_ocr.json once
            if not labels_by_doc:
                for p in folder.glob("*_ocr.json"):
                    _ingest_json(p)
    except Exception:
        pass
    return by_doc, labels_by_doc


# ----- JSON writing ----------------------------------------------------------
def write_statements_json(
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
    # Periods from in-memory third-pass
    periods_by_doctype: dict[str, dict] = {}
    try:
        if third_pass_raw:
            for _dt_key, _res_list in (third_pass_raw or {}).items():
                dt_norm = normalize_doc_type(_dt_key)
                candidates = _res_list if isinstance(_res_list, list) else [_res_list]
                for _res in candidates:
                    if not isinstance(_res, dict):
                        continue
                    pd_payload = ((_res.get("data") or {}).get("parsedData") or {})
                    by_id, _ = extract_period_maps_from_payload(pd_payload)
                    if by_id:
                        periods_by_doctype[dt_norm] = by_id
                        break
    except Exception:
        pass

    # Fallback: scan disk for *_ocr.json per-group files
    try:
        _by_doc, _labels = scan_group_jsons_for_periods(pdf_path, stem)
        if _by_doc:
            for _k, _v in _by_doc.items():
                periods_by_doctype.setdefault(_k, {}).update(_v)
    except Exception:
        pass

    allowed = [lbl for lbl in (CFG.get("labels", {}).get("canonical", []) or []) if lbl != "Others"]

    def _coerce_row_numbers(row: dict) -> dict:
        out = dict(row)
        try:
            for k in list(out.keys()):
                if re.fullmatch(r"(?i)[cp]\d+", str(k)):
                    v = out[k]
                    nv = _coerce_number_like(v)
                    if v in ("", None):
                        continue
                    out[k] = nv if nv is not None else out[k]
        except Exception:
            pass
        return out

    docs: dict[str, dict] = {}
    for doc_type in allowed:
        rows = combined_rows.get(doc_type) or []
        try:
            rows = [_coerce_row_numbers(r) for r in rows]
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
            entry["rows"] = rows
        docs[doc_type] = entry

    out = {
        "file": Path(pdf_path).name,
        "status": "ok",
        "company_type": company_type or "",
        "documents": docs,
    }

    combined_json_cfg = (CFG.get("export", {}).get("combined_json", {}) or {})
    if combined_json_cfg.get("include_first_pass") and first_pass_results is not None:
        out["first_pass"] = first_pass_results
    if combined_json_cfg.get("include_second_pass") and second_pass_result is not None:
        out["second_pass"] = second_pass_result
    if combined_json_cfg.get("include_third_pass_raw") and third_pass_raw:
        out["third_pass"] = third_pass_raw

    json_name_tmpl = combined_json_cfg.get("filename") \
        or CFG.get("export", {}).get("filenames", {}).get("statements_json") \
        or "{stem}_statements.json"
    base_pdf = Path(pdf_path).expanduser().resolve()
    out_path = Path(out_path_override).expanduser().resolve() if out_path_override else base_pdf.with_name(json_name_tmpl.format(stem=stem))
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=2)
    logger.info("Combined JSON written to %s", out_path)
    return str(out_path)


def save_results(results: List[Dict[str, Any]], pdf_path: str, out_path: str | None = None) -> str:
    """Persist first‑pass results next to PDF (or to given path).

    Uses `export.filenames.first_pass_json` when available, otherwise defaults
    to "{stem}_ocr.json". This avoids relying on the third‑pass group template
    (which may include placeholders like {slug} that are not available here).
    """
    filenames_cfg = (CFG.get("export", {}).get("filenames", {}) or {})
    # Prefer explicit first-pass template; fall back to a safe default
    name_tmpl = filenames_cfg.get("first_pass_json") or "{stem}_ocr.json"
    base_pdf = Path(pdf_path).expanduser().resolve()
    stem = base_pdf.stem
    if out_path:
        path = Path(out_path)
    else:
        # Be tolerant of custom templates that reference placeholders not
        # available at first‑pass time. Fall back to a safe default.
        try:
            filename = name_tmpl.format(stem=stem)
        except Exception as e:
            try:
                import logging as _logging
                _logging.getLogger(__name__).warning(
                    "first_pass_json template %r not compatible for first-pass (error=%s); using default {stem}_ocr.json",
                    name_tmpl, e,
                )
            except Exception:
                pass
            filename = f"{stem}_ocr.json"
        path = base_pdf.with_name(filename)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(results, fh, indent=2)
    return str(path)
