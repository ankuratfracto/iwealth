"""Helpers for optional mid-pass page classification before the second OCR stage."""

from __future__ import annotations

from typing import List, Dict, Any, Optional, Tuple
import logging
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import CFG
from .ocr_client import call_fracto
from .pdf_ops import build_pdf_from_pages


def filter_pages_via_mid_pass(
    pdf_bytes: bytes,
    pages: List[int],
    *,
    stem: str | None = None,
    logger_obj: Optional[logging.Logger] = None,
    output_dir: Optional[Path] = None,
) -> Tuple[List[int], List[Dict[str, Any]]]:
    """Optionally prune pages via a config-driven classifier before second pass."""

    cfg = (CFG.get("passes", {}) or {}).get("pre_second", {}) or {}
    if not bool(cfg.get("enable", False)) or not pages:
        return pages, []

    log = logger_obj or logging.getLogger(__name__)
    parser_app = str(cfg.get("parser_app", ""))
    model = str(cfg.get("model", ""))
    extra_accuracy = str(cfg.get("extra_accuracy", False)).lower()
    field_name = str(cfg.get("field", "classification"))
    keep_labels_cfg = cfg.get("keep_labels") or []
    drop_labels_cfg = cfg.get("drop_labels") or ["others"]
    keep_labels = {str(lbl).strip().lower() for lbl in keep_labels_cfg if str(lbl).strip()}
    drop_labels = {str(lbl).strip().lower() for lbl in drop_labels_cfg if str(lbl).strip()}
    keep_if_missing = bool(cfg.get("keep_if_missing", True))
    filename_tmpl = str(cfg.get("filename_template", "{stem}_page_{page}_classify.pdf"))
    save_json = bool(cfg.get("save_json", False))
    json_tmpl = str(cfg.get("json_name_template", "{stem}_presecond.json"))

    diagnostics: Dict[int, Dict[str, Any]] = {}
    decisions: Dict[int, bool] = {}
    stem_safe = stem or "document"

    def _extract_label(payload: Any) -> str:
        label = ""
        if isinstance(payload, dict):
            for key in (field_name, field_name.lower(), field_name.upper()):
                if key in payload and payload.get(key) not in (None, ""):
                    label = str(payload.get(key))
                    break
        elif isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    val = item.get(field_name) or item.get(field_name.lower()) or item.get(field_name.upper())
                    if val not in (None, ""):
                        label = str(val)
                        break
        return label.strip().lower()

    def _classify_page(page: int) -> Tuple[int, Dict[str, Any], bool]:
        try:
            page_bytes = build_pdf_from_pages(pdf_bytes, [page])
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("[mid-pass] failed to build PDF for page %s: %s", page, exc)
            diag = {"page": page, "label": "", "keep": True, "reason": "build_failed"}
            return page, diag, True

        pdf_name = filename_tmpl.format(stem=stem_safe, page=page)
        try:
            resp = call_fracto(
                page_bytes,
                pdf_name,
                parser_app=parser_app,
                model=model,
                extra_accuracy=extra_accuracy,
            )
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("[mid-pass] classification call failed for page %s: %s", page, exc)
            diag = {"page": page, "label": "", "keep": True, "reason": "call_failed"}
            return page, diag, True

        if resp.get("status") != "ok":
            log.warning(
                "[mid-pass] parser returned status=%s for page %s; keeping page",
                resp.get("status"),
                page,
            )
            diag = {"page": page, "label": "", "keep": True, "reason": f"status_{resp.get('status')}"}
            return page, diag, True

        payload = (resp.get("data", {}) or {}).get("parsedData", {})
        label = _extract_label(payload)

        keep = True
        reason = "keep"
        if keep_labels:
            keep = label in keep_labels or (keep_if_missing and label == "")
            reason = "keep_set" if keep else "drop_not_in_keep"
        else:
            drop_hit = label in drop_labels and label != ""
            keep = not drop_hit or (label == "" and keep_if_missing)
            reason = "drop_match" if drop_hit else "keep_default"

        diag = {"page": page, "label": label, "keep": keep, "reason": reason}
        return page, diag, keep

    conc_cfg = CFG.get("concurrency", {}) or {}
    max_workers = int(conc_cfg.get("max_parallel", 9) or 1)
    max_workers = max(1, min(max_workers, len(pages)))

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {pool.submit(_classify_page, page): page for page in pages}
        for fut in as_completed(future_map):
            page = future_map[fut]
            try:
                page_res, diag, keep = fut.result()
            except Exception as exc:  # pragma: no cover - defensive
                log.warning("[mid-pass] unexpected failure for page %s: %s", page, exc)
                diagnostics[page] = {"page": page, "label": "", "keep": True, "reason": "exception"}
                decisions[page] = True
                continue
            diagnostics[page_res] = diag
            decisions[page_res] = keep

    ordered_pages = sorted(pages)
    ordered_diags = [diagnostics[p] for p in ordered_pages if p in diagnostics]
    kept_pages = [p for p in ordered_pages if decisions.get(p, False)]

    if save_json and output_dir is not None:
        try:
            base = Path(output_dir).expanduser()
            base.mkdir(parents=True, exist_ok=True)
            json_name = json_tmpl.format(stem=stem_safe)
            json_path = (base / json_name).resolve()
            payload = {
                "pages": ordered_diags,
                "kept_pages": kept_pages,
                "total_pages": len(pages),
            }
            json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            log.info("[mid-pass] saved diagnostics to %s", json_path)
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("[mid-pass] failed to save diagnostics JSON: %s", exc)

    return kept_pages, ordered_diags


__all__ = ["filter_pages_via_mid_pass"]
