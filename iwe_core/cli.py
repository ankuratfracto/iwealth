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
from iwe_core import analytics as _analytics
from iwe_core import excel_ops as _excel
from iwe_core.grouping import normalize_doc_type
from iwe_core.config import CFG


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
    # Derive a clean stem from first JSON (drop trailing _ocr)
    stem_base = base.stem.replace('_ocr', '')
    if out_xlsx is None:
        out_xlsx = str(base.with_name(f"{stem_base}_statements.xlsx"))
    stub_pdf = pdf_hint or str(base.with_suffix(".pdf"))

    import pandas as pd
    combined_sheets: Dict[str, Any] = {}
    periods_by_doc: Dict[str, Dict] = {}
    third_pass_raw: Dict[str, list] = {}

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

        # Preserve raw payloads to enrich combined JSON (optional)
        try:
            third_pass_raw.setdefault(doc_type, []).append(obj)
        except Exception:
            pass

    if not combined_sheets:
        raise RuntimeError("No sheets built from provided JSONs")

    # If caller provided an explicit workbook name, prefer its stem for JSON naming
    if out_xlsx is not None:
        stem = Path(out_xlsx).stem.replace('_statements', '')
    else:
        stem = stem_base
    xlsx_path = _excel._write_statements_workbook(stub_pdf, stem, combined_sheets, routing_used=None, periods_by_doc=periods_by_doc)

    # Also emit a combined statements JSON alongside the workbook
    try:
        combined_rows = {k: ([] if (v is None or getattr(v, "empty", False)) else v.to_dict(orient="records")) for k, v in (combined_sheets or {}).items()}
        json_ops.write_statements_json(
            stub_pdf,
            stem,
            combined_rows,
            groups=None,
            routing_used=None,
            company_type=None,
            out_path_override=None,
            first_pass_results=None,
            second_pass_result=None,
            third_pass_raw=third_pass_raw,
        )
    except Exception:
        # Keep workbook creation successful even if JSON writing fails
        pass

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
        # Also tell user where JSON landed
        try:
            base = Path(final_path).expanduser().resolve()
            json_guess = str(base.with_name(base.stem.replace('_statements','') + '_statements.json'))
            if Path(json_guess).exists():
                print(f"[from-json] Combined JSON → {json_guess}")
        except Exception:
            pass
        return 0
    except Exception as e:
        print(f"[from-json] Failed: {e}")
        return 2


__all__ = [
    "from_json_to_workbook",
    "run_from_json_argv",
    "run_analyze_argv",
]

def _compute_analytics_for_combined_json(obj: Dict[str, Any]) -> Dict[str, Any]:
    data = dict(obj)
    docs = (data.get("documents") or {})
    analytics_out: Dict[str, Any] = {}
    for doc_type, entry in docs.items():
        rows = entry.get("rows") or []
        labels_raw = entry.get("periods") or {}
        # Normalize labels to string map
        labels = {}
        for k, v in (labels_raw or {}).items():
            if isinstance(v, dict):
                labels[str(k).lower()] = str(v.get("label", ""))
            else:
                labels[str(k).lower()] = str(v)
        try:
            units = _analytics.detect_units_and_currency({}, rows)
        except Exception:
            units = {}
        period_idx = _analytics.build_period_index(labels)
        qflags = _analytics.quality_flags(doc_type, rows, labels)
        footrefs = _analytics.extract_footnote_refs(rows)
        period_math = _analytics.compute_period_math(doc_type, rows, labels)
        common_size = _analytics.compute_common_size(doc_type, rows, labels)
        if common_size and not bool(((CFG.get("analytics", {}) or {}).get("common_size", {}) or {}).get("include_rows", True)):
            common_size = {k: v for k, v in common_size.items() if k != "rows"}
        analytics_out[doc_type] = {
            "units": units,
            "period_index": period_idx,
            "quality": qflags,
            "footnotes": footrefs,
            "period_math": period_math,
            "common_size": common_size,
            "restatements": {},
        }
    data["analytics"] = analytics_out
    try:
        # Build a core pack across statements using the rows already in combined JSON
        combined_rows = {dt: (docs.get(dt, {}).get("rows") or []) for dt in docs.keys()}
        periods_by_doc = {dt: (docs.get(dt, {}).get("periods") or {}) for dt in docs.keys()}
        core = _analytics.compute_core_pack(combined_rows, periods_by_doc, cfg=(CFG.get("analytics", {}) or {}))
        if core:
            data["analytics"]["core"] = core
            # Print and log a concise summary for visibility
            try:
                import logging as _logging
                _log = _logging.getLogger(__name__)
                gm = core.get("growth_margin", {}).get("margins_pct", {})
                cc = core.get("cash_conversion", {})
                wc = core.get("working_capital_ccc", {})
                pr = core.get("profitability", {})
                print("[analyze] Core summary:")
                print(f"  margins: gross={len(gm.get('gross', {}))} ebitda={len(gm.get('ebitda', {}))} ebit={len(gm.get('ebit', {}))} np={len(gm.get('np', {}))}")
                print(f"  cash: ocf={len(cc.get('ocf', {}))} fcf={len(cc.get('fcf', {}))} fcf_margin={len(cc.get('fcf_margin_pct', {}))} ccr={len(cc.get('cash_conversion_ratio', {}))}")
                print(f"  wc: dso={len(wc.get('dso_days', {}))} dpo={len(wc.get('dpo_days', {}))} dio={len(wc.get('dio_days', {}))} ccc={len(wc.get('ccc_days', {}))}")
                print(f"  profit: roe={len(pr.get('roe_pct', {}))} roic={len(pr.get('roic_pct', {}))} spread={len(pr.get('spread_vs_wacc_pct', {}))}")
                # Show sample values (first 3 keys) for quick sanity
                def _sample(d: dict):
                    try:
                        keys = sorted(d.keys())[:3]
                        return {k: d[k] for k in keys}
                    except Exception:
                        return {}
                print("[analyze] Samples:")
                print("  EBITDA margin sample:", _sample(gm.get('ebitda', {}) or {}))
                print("  NP margin sample:", _sample(gm.get('np', {}) or {}))
                print("  OCF sample:", _sample(cc.get('ocf', {}) or {}))
                print("  FCF sample:", _sample(cc.get('fcf', {}) or {}))
                print("  DSO/DPO/DIO sample:",
                      _sample(wc.get('dso_days', {}) or {}),
                      _sample(wc.get('dpo_days', {}) or {}),
                      _sample(wc.get('dio_days', {}) or {}))
                _log.info("[analyze] core sizes → margins:%s cash:%s wc:%s profitability:%s",
                          {k: len(v or {}) for k, v in (gm or {}).items()},
                          {k: len(v or {}) for k, v in (cc or {}).items()},
                          {k: len(v or {}) for k, v in (wc or {}).items()},
                          {k: len(v or {}) for k, v in (pr or {}).items()},)
            except Exception:
                pass
        else:
            print("[analyze] Core pack computed empty — check KPI patterns and period matching")
    except Exception as e:
        print(f"[analyze] core computation failed: {e}")
    return data

def run_analyze_argv(argv: List[str]) -> int:
    """Compute analytics for existing *_statements.json files and update in place.

    Usage: a.py analyze <file1_statements.json> [file2_statements.json ...] [--out-dir DIR]
    """
    out_dir = None
    jsons: List[str] = []
    it = iter(argv)
    for tok in it:
        if tok == "--out-dir":
            try:
                out_dir = next(it)
            except StopIteration:
                out_dir = None
            continue
        jsons.append(tok)
    if not jsons:
        print("analyze: provide one or more *_statements.json files")
        return 1
    for jp in jsons:
        p = Path(jp).expanduser().resolve()
        try:
            obj = _json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[analyze] skip unreadable {p}: {e}")
            continue
        try:
            updated = _compute_analytics_for_combined_json(obj)
            out_path = p
            if out_dir:
                out_path = Path(out_dir).expanduser().resolve() / p.name
            Path(out_path).write_text(_json.dumps(updated, indent=2), encoding="utf-8")
            print(f"[analyze] analytics updated → {out_path}")
        except Exception as e:
            print(f"[analyze] failed for {p}: {e}")
            return 2
    return 0
