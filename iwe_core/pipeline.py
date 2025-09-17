"""End‑to‑end OCR pipeline orchestration.

Runs first/second/third OCR passes, selects pages, routes per document
type, aggregates rows, and writes both the styled Excel workbook and a
combined JSON artifact. Designed to be called from CLI wrappers.
"""

from __future__ import annotations

from typing import List, Dict, Any
from pathlib import Path
import os
import json
import time
import logging
import copy

from .config import CFG
from . import json_ops
from . import excel_ops
from . import analytics as _analytics
from .ocr_client import call_fracto, call_fracto_parallel, resolve_api_key
from concurrent.futures import ThreadPoolExecutor, as_completed
from .selection import (
    _select_by_criteria,
    _first_pass_has_table,
    _second_pass_container,
    _second_pass_field,
    _second_pass_org_type,
    expand_selected_pages,
)
from .grouping import build_groups, normalize_doc_type
from .pdf_ops import build_pdf_from_pages, get_page_count_from_bytes
from .mid_pass import filter_pages_via_mid_pass


logger = logging.getLogger(__name__)

# ----- Routing config snapshot (reuse logic similar to a.py) ---------------
_ROUTING_CFG = CFG.get("routing", {}) or {}
_ROUTING_COMPANY_DEFAULT = str(CFG.get("company_type_prior", {}).get("default", "corporate")).lower()
_ROUTING_FALLBACK_ORDER = _ROUTING_CFG.get(
    "fallback_order",
    ["company_type_and_doc_type", "corporate_and_doc_type", "third_defaults"],
)
_ROUTING_ALLOWED_PARSERS = set((_ROUTING_CFG.get("allowed_parsers") or []) or [])
_ROUTING_BLOCKED_PARSERS = set((_ROUTING_CFG.get("blocked_parsers") or []) or [])
_ROUTING_SKIP_ON_DISABLED = bool(_ROUTING_CFG.get("skip_on_disabled", False))

def _resolve_routing(doc_type: str, company_type: str | None = None) -> tuple[str | None, str | None, str | None]:
    """Resolve (parser_app, model_id, extra_accuracy) from CFG['routing'].

    Fallback order is driven by CFG['routing']['fallback_order'] and finally
    falls back to passes.third.defaults.
    """
    dt = (doc_type or "").strip().lower()
    ct = (company_type or _ROUTING_COMPANY_DEFAULT or "corporate").strip().lower()

    # For visibility, show available doc_type keys
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
                if str(hit.get("enable", True)).strip().lower() in {"false", "0", "no", "off"}:
                    if _ROUTING_SKIP_ON_DISABLED:
                        logger.info("[routing] %s/%s is disabled and skip_on_disabled=true → skipping", ct_key, dt_key)
                        return (None, None, None)
                    return None
                third = (CFG.get("passes", {}).get("third", {}) or {}).get("defaults", {})
                parser = hit.get("parser") or third.get("parser_app", "")
                model  = hit.get("model")  or third.get("model", "tv7")
                extra  = str(hit.get("extra", third.get("extra_accuracy", True))).lower()
                if _ROUTING_ALLOWED_PARSERS and parser not in _ROUTING_ALLOWED_PARSERS:
                    logger.info("[routing] parser %s not in allowed_parsers; falling back", parser)
                    return None
                if parser in _ROUTING_BLOCKED_PARSERS:
                    logger.info("[routing] parser %s is blocked; falling back", parser)
                    return None
                logger.info("[routing] matched %s/%s → parser=%s, model=%s, extra=%s", ct_key, dt_key, parser, model, extra)
                return (parser, model, extra)
        return None

    for mode in _ROUTING_FALLBACK_ORDER:
        logger.info("[routing] attempt=%s ct=%s dt=%s", mode, ct, dt)
        if mode == "company_type_and_doc_type":
            r = _lookup(ct, dt)
            if r == (None, None, None):
                return r
            if r:
                return r
        elif mode == "corporate_and_doc_type":
            r = _lookup("corporate", dt)
            if r:
                return r
        elif mode == "third_defaults":
            third = (CFG.get("passes", {}).get("third", {}) or {}).get("defaults", {})
            parser = third.get("parser_app", "")
            model  = third.get("model", "tv7")
            extra  = str(third.get("extra_accuracy", True)).lower()
            logger.info("[routing] no route; using third_defaults → parser=%s, model=%s, extra=%s", parser, model, extra)
            return (parser, model, extra)

    third = (CFG.get("passes", {}).get("third", {}) or {}).get("defaults", {})
    parser = third.get("parser_app", "")
    model  = third.get("model", "tv7")
    extra  = str(third.get("extra_accuracy", True)).lower()
    logger.info("[routing] exhausted fallbacks; using third_defaults → parser=%s, model=%s, extra=%s", parser, model, extra)
    return (parser, model, extra)

def _process_pdf_first_pass(pdf_path: str) -> List[Dict[str, Any]]:
    pdf_p = Path(pdf_path).expanduser().resolve()
    with open(pdf_p, "rb") as fh:
        pdf_bytes = fh.read()
    extra = str(CFG.get("passes", {}).get("first", {}).get("extra_accuracy", False)).lower()
    return call_fracto_parallel(pdf_bytes, pdf_p.name, extra_accuracy=extra)


def _pick_selected_pages(results: List[Dict[str, Any]]) -> List[int]:
    sel_cfg = CFG.get("passes", {}).get("first", {}).get("selection", {}) or {}
    use_criteria = bool((sel_cfg.get("criteria") or {}).get("rules"))
    if use_criteria:
        pages = [idx + 1 for idx, res in enumerate(results) if _select_by_criteria(res)]
    else:
        pages = [idx + 1 for idx, res in enumerate(results) if _first_pass_has_table(res)]
    # Optionally expand neighbours
    radius = int(sel_cfg.get("neighbor_radius", 0))
    if radius > 0:
        pages = expand_selected_pages(pages, len(results), radius=radius)
    return pages




def run_cli(argv: List[str]) -> int:
    if not argv:
        print("Usage: python a.py <pdf-path> [output.json] [output.xlsx] [KEY=VALUE ...]")
        return 1

    pdf_path = argv[0]
    json_out = None

    overrides: Dict[str, str] = {}
    remaining: List[str] = []
    for arg in argv[1:]:
        if "=" in arg:
            k, v = arg.split("=", 1)
            overrides[k.strip()] = v
        else:
            remaining.append(arg)
    if remaining:
        if not remaining[0].lower().endswith((".xlsx", ".xlsm", ".xls")):
            json_out = remaining[0]

    # Quick filter toggles via overrides
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
        if "FILTER_MIN" in overrides:
            os.environ["FRACTO_FILTER_MIN_PAGES"] = str(overrides["FILTER_MIN"]).strip()
    except Exception:
        pass

    if not Path(pdf_path).expanduser().exists():
        logger.error("File not found: %s", pdf_path)
        return 2

    if not resolve_api_key():
        api_env = CFG.get("api", {}).get("api_key_env", "FRACTO_API_KEY")
        logger.error("No API key found. Set %s or add api.api_key in config.yaml", api_env)
        return 3

    overall_start = time.time()
    first_pass_time = 0.0
    pre_second_time = 0.0
    second_pass_time = 0.0
    third_pass_time = 0.0
    setup_time = 0.0
    pdf_p = Path(pdf_path).expanduser().resolve()

    with open(pdf_p, "rb") as fh:
        orig_bytes = fh.read()

    setup_time = time.time() - overall_start

    first_enabled = bool(CFG.get("passes", {}).get("first", {}).get("enable", True))
    results: List[Dict[str, Any]] = []

    first_start = time.time()
    if first_enabled:
        results = _process_pdf_first_pass(pdf_path)
        json_ops.save_results(results, str(pdf_p), None)

        selected_pages = _pick_selected_pages(results)
        if not selected_pages:
            selected_pages = [
                idx + 1
                for idx, res in enumerate(results)
                if ((res.get("data", {}).get("parsedData", {}).get("Document_type", "Others") or "").lower() != "others")
            ]
        try:
            _radius = int(os.getenv("FRACTO_EXPAND_NEIGHBORS", "0"))
            if _radius > 0:
                selected_pages = expand_selected_pages(selected_pages, len(results), radius=_radius)
        except Exception:
            pass
    else:
        total_pages = get_page_count_from_bytes(orig_bytes)
        selected_pages = list(range(1, total_pages + 1))
        logger.info(
            "First pass disabled via config; using all %d page(s) for pre-second classifier",
            len(selected_pages),
        )

    first_pass_time = time.time() - first_start

    if not selected_pages:
        logger.error("No pages selected for second pass")
        return 4

    pre_second_start = time.time()
    filtered_pages, mid_diag = filter_pages_via_mid_pass(
        orig_bytes,
        selected_pages,
        stem=pdf_p.stem,
        logger_obj=logger,
        output_dir=pdf_p.parent,
    )
    pre_second_time = time.time() - pre_second_start
    if mid_diag:
        try:
            summary = "; ".join(
                f"{item['page']}={item['label'] or 'unclassified'}{'(drop)' if not item['keep'] else ''}"
                for item in mid_diag
            )
            logger.info("[mid-pass] page classifications → %s", summary)
        except Exception:
            pass
        dropped = [item["page"] for item in mid_diag if not item.get("keep")]
        if dropped:
            logger.info("[mid-pass] dropping pages before second pass: %s", dropped)
    if filtered_pages:
        selected_pages = filtered_pages
    elif mid_diag:
        logger.warning("[mid-pass] classifier removed all pages; reverting to original selection")

    second_start = time.time()
    selected_bytes = build_pdf_from_pages(orig_bytes, selected_pages)

    # Second pass
    stem = pdf_p.stem
    sel_name = CFG.get("passes", {}).get("second", {}).get("selected_pdf_name", "{stem}_selected.pdf").format(stem=stem)
    second_res = call_fracto(
        selected_bytes,
        sel_name,
        parser_app=CFG.get("passes", {}).get("second", {}).get("parser_app", ""),
        model=CFG.get("passes", {}).get("second", {}).get("model", ""),
        extra_accuracy=str(CFG.get("passes", {}).get("second", {}).get("extra_accuracy", False)).lower(),
    )
    if bool(CFG.get("passes", {}).get("second", {}).get("save_selected_json", True)):
        sel_json = CFG.get("passes", {}).get("second", {}).get("selected_json_name", "{stem}_selected_ocr.json").format(stem=stem)
        with open(Path(pdf_p).with_name(sel_json), "w", encoding="utf-8") as fh:
            json.dump(second_res, fh, indent=2)
    second_pass_time = time.time() - second_start

    # Third pass setup
    third_start = time.time()
    pd_payload = (second_res.get("data", {}) or {}).get("parsedData", {})
    org_type_raw = _second_pass_org_type(pd_payload)
    company_type = (str(org_type_raw).strip().lower() if org_type_raw else "corporate")
    try:
        logger.info("Routing company_type: %s (raw=%r)", company_type or "corporate", org_type_raw)
    except Exception:
        pass
    raw_class = _second_pass_container(pd_payload)
    classification: List[dict] = []
    for i, item in enumerate(raw_class, start=1):
        if not isinstance(item, dict):
            continue
        main_dt   = _second_pass_field(item, "doc_type")
        has_two   = str(_second_pass_field(item, "has_two")).strip().lower() in ("1","true","yes","y","on")
        second_dt = _second_pass_field(item, "second_doc_type")
        classification.append({
            "page_number": int(_second_pass_field(item, "page_number", i)),
            "doc_type": main_dt,
            "has_two": "true" if has_two else "",
            "second_doc_type": second_dt,
            "is_continuation": "true" if str(_second_pass_field(item, "is_continuation")).strip().lower() in ("1","true","yes","y","on") else "",
            "continuation_of": _second_pass_field(item, "continuation_of"),
        })
    if not classification:
        tmp: List[dict] = []
        if results:
            for sel_idx, orig_pno in enumerate(selected_pages, start=1):
                if not (1 <= orig_pno <= len(results)):
                    continue
                res = results[orig_pno - 1] or {}
                pdict = (res.get("data", {}) or {}).get("parsedData", {}) or {}
                dt = pdict.get("Document_type")
                if dt and str(dt).strip().lower() != "others":
                    tmp.append({"page_number": sel_idx, "doc_type": dt})
        elif mid_diag:
            label_map = {int(it.get("page", 0)): str(it.get("label") or "") for it in mid_diag if isinstance(it, dict)}
            for sel_idx, orig_pno in enumerate(selected_pages, start=1):
                raw = label_map.get(orig_pno, "")
                if not raw:
                    continue
                human = raw.replace("_", " ")
                dt_norm = normalize_doc_type(human)
                if dt_norm and dt_norm != "Others":
                    tmp.append({"page_number": sel_idx, "doc_type": dt_norm})
        classification = tmp

    # Log visibility for second-pass → third-pass flow
    try:
        _dbg_cls = [
            {
                "page_number": item.get("page_number"),
                "doc_type": item.get("doc_type"),
                "has_two": item.get("has_two"),
                "second_doc_type": item.get("second_doc_type"),
            }
            for item in classification if isinstance(item, dict)
        ]
        logger.info("[routing] second-pass classification (raw) → %s", _dbg_cls)
    except Exception:
        pass

    groups = build_groups(selected_pages, classification, orig_bytes, first_pass_results=results)
    if not groups:
        # Create an empty workbook with configured sheets
        try:
            sheet_order = CFG.get("export", {}).get("statements_workbook", {}).get("sheet_order") \
                or CFG.get("labels", {}).get("canonical", []) \
                or []
            combined_rows = {sn: [] for sn in sheet_order}
            # Write workbook (empty sheets) and JSON
            excel_ops._write_statements_workbook(str(pdf_p), stem, {}, routing_used=None, periods_by_doc={})
            json_ops.write_statements_json(
                str(pdf_p), stem,
                combined_rows, {}, {}, company_type,
                out_path_override=json_out,
                first_pass_results=results,
                second_pass_result=second_res,
                third_pass_raw={}
            )
        except Exception:
            pass
        third_pass_time = time.time() - third_start
        total_time = time.time() - overall_start
        measured_sum = setup_time + first_pass_time + pre_second_time + second_pass_time + third_pass_time
        drift = total_time - measured_sum
        if abs(drift) > 1e-6:
            third_pass_time += drift
            measured_sum = setup_time + first_pass_time + pre_second_time + second_pass_time + third_pass_time
        logger.info(
            "Timing summary → setup: %.2fs | first: %.2fs | pre-second: %.2fs | second: %.2fs | third: %.2fs | total: %.2fs",
            setup_time, first_pass_time, pre_second_time, second_pass_time, third_pass_time, total_time,
        )
        return 0

    # Third pass per group (parallelized)
    import pandas as pd
    combined_sheets: dict[str, pd.DataFrame] = {}
    routing_used: dict[str, dict] = {}
    third_pass_raw: dict[str, list[dict]] = {}

    # Materialize work items (skip disabled upfront)
    work_items: list[tuple[str, list[int], tuple[str | None, str | None, str | None]]] = []
    for doc_type, page_list in groups.items():
        page_list = sorted(page_list)
        parser_app, model_id, extra_acc = _resolve_routing(doc_type, company_type=company_type)
        if parser_app is None:
            routing_used[doc_type] = {"parser_app": None, "model": None, "extra": None, "company_type": company_type, "skipped": True, "reason": "disabled"}
            logger.info("↷ Skipping %s via company_type=%s (disabled; no fallback)", doc_type, company_type)
            continue
        routing_used[doc_type] = {"parser_app": parser_app, "model": model_id, "extra": extra_acc, "company_type": company_type}
        work_items.append((doc_type, page_list, (parser_app, model_id, extra_acc)))

    max_workers = int((CFG.get("concurrency", {}) or {}).get("max_parallel", 9)) or 4
    def _process_group(doc_type: str, pages: list[int], parser_app: str, model_id: str, extra_acc: str):
        try:
            group_bytes = build_pdf_from_pages(orig_bytes, pages)
            logger.info(
                "→ Routing %s via company_type=%s → parser=%s, model=%s, extra=%s, pages=%s",
                doc_type, company_type, parser_app, model_id, extra_acc, pages,
            )
            res = call_fracto(
                group_bytes,
                f"{stem}_{doc_type.lower().replace(' ', '_').replace('&','and').replace('/','_')}.pdf",
                parser_app=parser_app,
                model=model_id,
                extra_accuracy=extra_acc,
            )
            # Persist per-group JSON artifact next to the PDF
            try:
                slug = (
                    doc_type.lower().replace(" ", "_").replace("&", "and").replace("/", "_")
                )
                group_json_name = CFG.get("export", {}).get("filenames", {}).get("group_json", "{stem}_{slug}_ocr.json")
                group_json_path = Path(pdf_p).with_name(group_json_name.format(stem=stem, slug=slug))
                with open(group_json_path, "w", encoding="utf-8") as _fh:
                    json.dump(res, _fh, indent=2)
                logger.info("Third-pass group JSON written: %s", group_json_path)
            except Exception:
                pass
            parsed_payload = (res.get("data", {}) or {}).get("parsedData", {})
            # Robust row extraction
            try:
                from . import json_ops as _json_ops
                rows = _json_ops.extract_rows(parsed_payload, doc_type=doc_type) or []
            except Exception:
                rows = []
            # DataFrame conversion
            df = None
            if rows:
                try:
                    all_keys: List[str] = []
                    for r in rows:
                        for k in r.keys():
                            if k not in all_keys:
                                all_keys.append(k)
                    import pandas as _pd
                    df = _pd.DataFrame([{k: r.get(k, "") for k in all_keys} for r in rows], columns=all_keys)
                except Exception:
                    df = None
            return (doc_type, parsed_payload, df)
        except Exception as exc:
            logger.error("Group %s failed: %s", doc_type, exc)
            return (doc_type, {}, None)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(_process_group, dt, pages, pa, mid, ex)
            for (dt, pages, (pa, mid, ex)) in work_items
        ]
        for fut in as_completed(futures):
            dt, payload, df = fut.result()
            try:
                third_pass_raw.setdefault(dt, []).append(payload)
            except Exception:
                pass
            if df is not None:
                combined_sheets[dt] = df

    # Statement-level validation and optional retries -------------------------------------------------
    validation_reports: dict[str, dict] = {}
    validation_history: list[dict] = []
    retry_events: list[dict] = []

    def _latest_payload(dt: str) -> dict:
        payloads = third_pass_raw.get(dt) or []
        if isinstance(payloads, list) and payloads:
            for payload in reversed(payloads):
                if isinstance(payload, dict) and payload:
                    return payload
        elif isinstance(payloads, dict):
            return payloads
        return {}

    def _rows_for_doc(dt: str) -> list[dict]:
        payload = _latest_payload(dt)
        if payload:
            try:
                rows = json_ops.extract_rows(payload, doc_type=dt) or []
            except Exception:
                rows = []
            if rows:
                return rows
        df = combined_sheets.get(dt)
        if df is not None:
            try:
                return df.to_dict(orient="records")  # type: ignore[attr-defined]
            except Exception:
                return []
        return []

    def _period_labels_for_doc(dt: str) -> dict[str, str]:
        payload = _latest_payload(dt)
        if not payload:
            return {}
        cmap, labels = json_ops.extract_period_maps_from_payload(payload)
        out: dict[str, str] = {}
        for cid, label in (labels or {}).items():
            out[str(cid).lower()] = str(label)
        if not out and cmap:
            for cid, meta in cmap.items():
                if isinstance(meta, dict):
                    out[str(cid).lower()] = str(meta.get("label", ""))
                else:
                    out[str(cid).lower()] = str(meta)
        return out

    third_validation_cfg = (CFG.get("passes", {}).get("third", {}) or {}).get("validation") or {}
    validation_enabled = bool(third_validation_cfg.get("enable"))
    retry_cfg = third_validation_cfg.get("retry") or {}

    def _evaluate_validations(stage_label: str) -> list[str]:
        temp_reports: dict[str, dict] = {}
        failing: list[str] = []
        doc_types = sorted(set(third_pass_raw) | set(combined_sheets))
        for dt in doc_types:
            rows = _rows_for_doc(dt)
            labels = _period_labels_for_doc(dt)
            try:
                report = _analytics.quality_flags(dt, rows, labels)
            except Exception as exc:
                logger.error("[validation] quality flag computation failed for %s: %s", dt, exc)
                report = {"flags": ["quality_flags_error"], "checks": [], "error": str(exc)}
            temp_reports[dt] = report
            stmt = (report.get("statement_validation") or {})
            if stmt.get("enabled") and not stmt.get("passed"):
                failing.append(dt)
        validation_reports.clear()
        validation_reports.update(temp_reports)
        if failing:
            logger.info("[validation] statements requiring attention → %s", failing)
        validation_history.append(
            {
                "stage": stage_label,
                "timestamp": time.time(),
                "documents": copy.deepcopy(validation_reports),
                "failing_documents": list(failing),
            }
        )
        return failing

    failed_docs: List[str] = []
    if validation_enabled:
        failed_docs = _evaluate_validations("initial")

    if validation_enabled and retry_cfg.get("enable") and failed_docs:
        max_attempts = int(retry_cfg.get("max_attempts") or 1)
        extra_runs = max(0, max_attempts - 1)
        plan_steps = list(retry_cfg.get("plan") or [])[:extra_runs]
        if extra_runs and not plan_steps:
            plan_steps = [{} for _ in range(extra_runs)]
        truthy = {"1", "true", "yes", "y", "on"}
        for dt in list(failed_docs):
            pages = groups.get(dt) or []
            if not pages:
                logger.warning("[validation] cannot retry %s → no pages recorded", dt)
                continue
            base_route = routing_used.get(dt, {}) or {}
            base_parser = base_route.get("parser_app") or ""
            base_model = base_route.get("model") or ""
            base_extra = base_route.get("extra") or "false"
            current_company = base_route.get("company_type") or company_type
            attempts_made = 0
            resolved = False
            for attempt_idx, step in enumerate(plan_steps, start=1):
                attempts_made = attempt_idx
                parser = step.get("parser_app") or base_parser
                model = step.get("model") or base_model
                extra = base_extra
                if "extra_accuracy" in step:
                    extra = "true" if str(step.get("extra_accuracy")).strip().lower() in truthy else "false"
                if step.get("fallback_company_type"):
                    current_company = str(step.get("fallback_company_type")).strip().lower() or current_company
                    route = _resolve_routing(dt, company_type=current_company)
                    if route == (None, None, None):
                        logger.warning("[validation] retry for %s skipped (route disabled)", dt)
                        continue
                    parser, model, extra = route
                parser = parser or base_parser
                model = model or base_model
                extra = extra if extra is not None else base_extra
                if not parser:
                    logger.warning("[validation] retry for %s lacks parser configuration", dt)
                    continue
                logger.info(
                    "[validation] retrying %s attempt=%d parser=%s model=%s extra=%s company=%s",
                    dt, attempt_idx, parser, model, extra, current_company,
                )
                event = {
                    "doc_type": dt,
                    "attempt": attempt_idx,
                    "stage": f"retry_{attempt_idx}",
                    "parser_app": parser,
                    "model": model,
                    "extra_accuracy": extra,
                    "company_type": current_company,
                    "pages": list(pages),
                }
                retry_events.append(event)
                _, payload_retry, df_retry = _process_group(dt, pages, parser, model, extra)
                if df_retry is not None:
                    combined_sheets[dt] = df_retry
                else:
                    combined_sheets.pop(dt, None)
                third_pass_raw.setdefault(dt, []).append(payload_retry)
                routing_used[dt] = {
                    "parser_app": parser,
                    "model": model,
                    "extra": extra,
                    "company_type": current_company,
                    "validation_retry": True,
                    "retry_attempt": attempt_idx,
                }
                failed_docs = _evaluate_validations(event["stage"])
                event["report"] = copy.deepcopy(validation_reports.get(dt))
                event["failing_documents_after"] = list(failed_docs)
                if dt not in failed_docs:
                    event["outcome"] = "resolved"
                    resolved = True
                    break
                else:
                    event["outcome"] = "unresolved"
            if not resolved and attempts_made:
                logger.warning("[validation] %s still failing after %d retry attempt(s)", dt, attempts_made)

    if validation_enabled and not retry_cfg.get("enable") and failed_docs:
        logger.warning("[validation] failures detected but retry disabled → %s", failed_docs)

    if validation_enabled:
        failed_docs = _evaluate_validations("final")

    report_cfg = (third_validation_cfg.get("report") or {})
    if validation_enabled and bool(report_cfg.get("enable", True)):
        try:
            filename = str(report_cfg.get("filename", "{stem}_validation_report.json")).format(stem=stem)
        except Exception:
            filename = f"{stem}_validation_report.json"
        report_path = Path(pdf_p).with_name(filename)
        diagnostics = {
            "file": pdf_p.name,
            "generated_at": time.time(),
            "validation_enabled": validation_enabled,
            "failed_documents": list(failed_docs),
            "history": validation_history,
            "retry_events": retry_events,
            "final_reports": copy.deepcopy(validation_reports),
        }
        try:
            with open(report_path, "w", encoding="utf-8") as _fh:
                json.dump(diagnostics, _fh, indent=2)
            logger.info("[validation] diagnostics written: %s", report_path)
        except Exception as exc:
            logger.warning("[validation] failed to write diagnostics: %s", exc)


    # Build combined_rows using latest payloads/DataFrames
    combined_rows: dict[str, list[dict]] = {}
    doc_union = sorted(set(third_pass_raw) | set(combined_sheets))
    for dt in doc_union:
        df = combined_sheets.get(dt)
        if df is not None and not getattr(df, "empty", False):
            try:
                combined_rows[dt] = df.to_dict(orient="records")  # type: ignore[attr-defined]
                continue
            except Exception:
                pass
        combined_rows[dt] = _rows_for_doc(dt)

    periods_by_doc: dict[str, dict] = {}
    for dt_key, payloads in (third_pass_raw or {}).items():
        dt_norm = normalize_doc_type(dt_key)
        candidates = payloads if isinstance(payloads, list) else [payloads]
        iterable = list(candidates)
        for payload in reversed(iterable):
            if not isinstance(payload, dict):
                continue
            cmap, _ = json_ops.extract_period_maps_from_payload(payload)
            if cmap:
                periods_by_doc[dt_norm] = cmap
                break

    # If nothing parsed into DataFrames, still write empty workbook + JSON for visibility
    if not combined_sheets:
        try:
            sheet_order = CFG.get("export", {}).get("statements_workbook", {}).get("sheet_order") \
                or CFG.get("labels", {}).get("canonical", []) \
                or []
            empty_sheets = {sn: pd.DataFrame() for sn in sheet_order}
            _ = excel_ops._write_statements_workbook(str(pdf_p), stem, empty_sheets, routing_used=routing_used, periods_by_doc=periods_by_doc)
        except Exception:
            pass
        json_ops.write_statements_json(
            str(pdf_p), stem, combined_rows, groups, routing_used, company_type,
            out_path_override=json_out, first_pass_results=results, second_pass_result=second_res, third_pass_raw=third_pass_raw,
        )
        third_pass_time = time.time() - third_start
        total_time = time.time() - overall_start
    else:
        _ = excel_ops._write_statements_workbook(str(pdf_p), stem, combined_sheets, routing_used=routing_used, periods_by_doc=periods_by_doc)
        json_ops.write_statements_json(
            str(pdf_p), stem, combined_rows, groups, routing_used, company_type,
            out_path_override=json_out, first_pass_results=results, second_pass_result=second_res, third_pass_raw=third_pass_raw,
        )
        third_pass_time = time.time() - third_start
        total_time = time.time() - overall_start

    measured_sum = setup_time + first_pass_time + pre_second_time + second_pass_time + third_pass_time
    drift = total_time - measured_sum
    if abs(drift) > 1e-6:
        third_pass_time += drift
        measured_sum = setup_time + first_pass_time + pre_second_time + second_pass_time + third_pass_time
    logger.info(
        "Timing summary → setup: %.2fs | first: %.2fs | pre-second: %.2fs | second: %.2fs | third: %.2fs | total: %.2fs",
        setup_time, first_pass_time, pre_second_time, second_pass_time, third_pass_time, total_time,
    )
    return 0


__all__ = ["run_cli"]
