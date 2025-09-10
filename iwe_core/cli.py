"""CLI helpers for building Excel workbooks from group JSONs.

Provides utilities to combine per‑group third‑pass JSON outputs into a
single, styled statements workbook and a simple argv parser for use from
command‑line wrappers.
"""

from __future__ import annotations

from typing import List, Dict, Any
from pathlib import Path
import json as _json

from iwe_core import json_ops
from iwe_core import excel_ops as _excel
from iwe_core.grouping import normalize_doc_type


def from_json_to_workbook(out_xlsx: str | None, json_paths: List[str], pdf_hint: str | None = None) -> str:
    """
    Build a combined workbook from one or more third‑pass group JSON files.

    - out_xlsx: optional desired output path for the workbook. If None, derives from first JSON.
    - json_paths: list of *_ocr.json files produced per group.
    - pdf_hint: optional original PDF path, used for naming and periods lookup.
    Returns the final workbook path.
    """
    if not json_paths:
        raise ValueError("Provide at least one *_ocr.json path")

    base = Path(json_paths[0]).expanduser().resolve()
    if out_xlsx is None:
        out_xlsx = str(base.with_name(f"{base.stem.replace('_ocr','')}_statements.xlsx"))
    stub_pdf = pdf_hint or str(base.with_suffix(".pdf"))

    import pandas as pd
    combined_sheets: Dict[str, Any] = {}
    periods_by_doc: Dict[str, Dict] = {}

    for jp in json_paths:
        p = Path(jp).expanduser().resolve()
        try:
            obj = _json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            # skip unreadable files silently
            continue

        pd_payload = ((obj.get("data") or {}).get("parsedData")) or obj.get("parsedData") or {}
        if not isinstance(pd_payload, (dict, list)):
            continue

        try:
            dt_guess = json_ops.doc_type_from_payload(pd_payload)
        except Exception:
            dt_guess = None
        if not dt_guess:
            slug = p.stem
            dt_guess = normalize_doc_type(slug.replace("_", " "))
        doc_type = normalize_doc_type(dt_guess)

        rows = json_ops.extract_rows(pd_payload)
        if not rows:
            continue

        all_keys: List[str] = []
        for r in rows:
            for k in r.keys():
                if k not in all_keys:
                    all_keys.append(k)
        df = pd.DataFrame([{k: r.get(k, "") for k in all_keys} for r in rows], columns=all_keys)
        df = df
        combined_sheets[doc_type] = df

        try:
            by_id, _labels = json_ops.extract_period_maps_from_payload(pd_payload if isinstance(pd_payload, dict) else {})
            if by_id:
                periods_by_doc[doc_type] = by_id
        except Exception:
            pass

    if not combined_sheets:
        raise RuntimeError("No sheets built from provided JSONs")

    stem = Path(out_xlsx).stem.replace("_statements", "")
    xlsx_path = _excel._write_statements_workbook(stub_pdf, stem, combined_sheets, routing_used=None, periods_by_doc=periods_by_doc)

    # Move/rename to desired out_xlsx if different
    try:
        out_xlsx_p = Path(out_xlsx).expanduser().resolve()
        if str(out_xlsx_p) != str(Path(xlsx_path).expanduser().resolve()):
            Path(xlsx_path).replace(out_xlsx_p)
            xlsx_path = str(out_xlsx_p)
    except Exception:
        pass
    return xlsx_path


def run_from_json_argv(argv: List[str]) -> int:
    """Parse argv for from-json mode and build workbook."""
    out_xlsx = None
    pdf_hint = None
    jsons: List[str] = []

    it = iter(argv)
    for tok in it:
        if tok == "--pdf":
            try:
                pdf_hint = next(it)
            except StopIteration:
                pdf_hint = None
            continue
        if out_xlsx is None and tok.lower().endswith((".xlsx", ".xlsm", ".xls")):
            out_xlsx = tok
            continue
        jsons.append(tok)

    if not jsons:
        print("from-json mode: provide one or more *_ocr.json files")
        return 1
    try:
        final_path = from_json_to_workbook(out_xlsx, jsons, pdf_hint)
        print(f"[from-json] Workbook written → {final_path}")
        return 0
    except Exception as e:
        print(f"[from-json] Failed: {e}")
        return 2


__all__ = [
    "from_json_to_workbook",
    "run_from_json_argv",
]
