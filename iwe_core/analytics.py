"""Analytics foundations: units, periods, common-size, and quality flags.

This module adds a light-weight, dependency-minimal analytics layer on top of
the parsed statements data (rows + period metadata) produced by OCR. It focuses
on making downstream data trustworthy and ready for metrics by providing:

- Unit detection (₹ crore/lakh/million/thousand) and multipliers
- Period parsing and a fiscal calendar snapshot (quarterly/annual)
- Common-size helpers (denominator detection stubs)
- Data quality flags (basic subtotal checks, sparsity, tie checks)
- Footnote reference extraction (line-items ↔ note tokens)

The functions are written to be tolerant of missing/partial data and avoid
changing existing Excel exports by default. The JSON writer can opt-in to
embed analytics metadata.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple
import re
from datetime import date

from .config import CFG


# --- Units & currency -------------------------------------------------------

def _compile_unit_regexes() -> List[re.Pattern]:
    pats = []
    try:
        for p in (CFG.get("units_and_scaling", {}) or {}).get("unit_header_patterns", []) or []:
            try:
                pats.append(re.compile(str(p), re.I))
            except Exception:
                continue
    except Exception:
        pass
    if not pats:
        pats.append(re.compile(r"₹\s*(in\.?|in)\s*(crore|cr|million|mn|lakh|thousand)", re.I))
    return pats


def _unit_multiplier(unit: str | None) -> Tuple[str | None, float]:
    if not unit:
        return None, 1.0
    key = str(unit).strip().lower()
    m = (CFG.get("units_and_scaling", {}) or {}).get("multipliers", {}) or {}
    # friendly aliases
    alias = {
        "cr": "crore",
        "mn": "million",
    }
    key = alias.get(key, key)
    try:
        mul = float(m.get(key) or 1.0)
    except Exception:
        mul = 1.0
    return key, mul


def _walk_strings(node: Any, out: List[str], depth: int = 0, max_items: int = 500) -> None:
    if len(out) >= max_items:
        return
    if isinstance(node, str):
        s = node.strip()
        if s:
            out.append(s)
        return
    if isinstance(node, dict):
        for v in node.values():
            _walk_strings(v, out, depth + 1, max_items)
    elif isinstance(node, list):
        for v in node:
            _walk_strings(v, out, depth + 1, max_items)


def detect_units_and_currency(pd_payload: dict | None, rows: List[dict] | None) -> Dict[str, Any]:
    """Detect currency symbol and unit keyword from payload/rows text.

    Returns { currency: 'INR'|'USD'|..., unit: 'crore'|'lakh'|..., multiplier: float, source: 'meta'|'rows'|'' }
    """
    texts: List[str] = []
    # 1) Pull strings from payload metadata where available
    try:
        gm = (pd_payload or {}).get("general_metadata") or {}
        mm = (pd_payload or {}).get("meta") or {}
        for obj in (gm, mm):
            if isinstance(obj, dict):
                for v in obj.values():
                    if isinstance(v, str):
                        texts.append(v)
    except Exception:
        pass
    # 2) Add a few row fields that often carry headers/sections
    try:
        for r in (rows or []):
            for k in ("section", "Section"):
                v = r.get(k)
                if isinstance(v, str):
                    texts.append(v)
    except Exception:
        pass

    # 3) As a last resort, walk the payload recursively and collect strings
    try:
        _walk_strings(pd_payload or {}, texts, max_items=800)
    except Exception:
        pass

    blob = " \n".join(texts)
    currency = None
    if "₹" in blob or "INR" in blob.upper():
        currency = "INR"
    elif "$" in blob:
        currency = "USD"

    unit = None
    source = ""
    for pat in _compile_unit_regexes():
        m = pat.search(blob)
        if m:
            try:
                unit = (m.group(2) or m.group(0) or "").strip()
            except Exception:
                unit = (m.group(0) or "").strip()
            source = "meta"
            break

    # Fallback: scan particulars text for inline unit hints
    if not unit and rows:
        for r in rows:
            s = str(r.get("Particulars") or r.get("particulars") or "")
            if not s:
                continue
            for pat in _compile_unit_regexes():
                m = pat.search(s)
                if m:
                    try:
                        unit = (m.group(2) or m.group(0) or "").strip()
                    except Exception:
                        unit = (m.group(0) or "").strip()
                    source = "rows"
                    break
            if unit:
                break

    unit_key, mul = _unit_multiplier(unit)
    return {"currency": currency or "INR", "unit": unit_key, "multiplier": mul, "source": source}


# --- Period parsing / fiscal calendar --------------------------------------

_MONTHS = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


def _parse_period_label(label: str) -> Tuple[date | None, str | None, str | None, str | None]:
    """Parse a period label to (end_date, freq, fiscal_quarter, fiscal_year_tag).

    Heuristics for common Indian reporting labels like 'Q1 FY24', 'Quarter ended 30-Jun-2023',
    'Year ended 31-Mar-2024', 'Mar 31, 2024', '31.03.2024'.
    Returns (None, None, None, None) if no confident parse.
    """
    s = str(label or "").strip()
    if not s:
        return None, None, None, None
    sl = s.lower()

    # Helper to parse any explicit date first (we'll still set freq based on wording)
    def _extract_any_date(s: str) -> date | None:
        m1 = re.search(r"(\d{1,2})[-./\s]*(\w{3,9})[-./\s]*(\d{2,4})", s)
        if m1:
            d = int(m1.group(1)); mm = _MONTHS.get(m1.group(2)[:3], None); yr = int(m1.group(3)); yr = 2000 + yr if yr < 100 else yr
            if mm:
                try:
                    return date(yr, mm, min(d, 31))
                except Exception:
                    return None
        m2 = re.search(r"(\w{3,9})\s*(\d{1,2}),?\s*(\d{4})", s)
        if m2:
            mm = _MONTHS.get(m2.group(1)[:3].lower(), None); d = int(m2.group(2)); yr = int(m2.group(3))
            if mm:
                try:
                    return date(yr, mm, min(d, 31))
                except Exception:
                    return None
        return None

    # 1) Year-ended statements
    if "year ended" in sl or "for the year ended" in sl:
        dt = _extract_any_date(sl)
        if dt is None:
            # Try to pick year from text; assume Mar 31
            m = re.search(r"\b(20\d{2})\b", sl)
            if m:
                yy = int(m.group(1)); dt = date(yy, 3, 31)
        if dt:
            fy = f"FY{str(dt.year)[-2:]}"
            return dt, "annual", None, fy

    # 2) Quarter-ended / three months ended
    if ("quarter ended" in sl) or ("three months ended" in sl) or ("3 months ended" in sl):
        dt = _extract_any_date(sl)
        if dt:
            # Map month to Indian FY quarter
            # FY ends in March (3): Q1=Apr-Jun(6), Q2=Jul-Sep(9), Q3=Oct-Dec(12), Q4=Jan-Mar(3)
            m = dt.month
            if m in (4,5,6):
                q = 1; fy_end = dt.year
            elif m in (7,8,9):
                q = 2; fy_end = dt.year
            elif m in (10,11,12):
                q = 3; fy_end = dt.year
            else:  # Jan-Mar
                q = 4; fy_end = dt.year
            return dt, "quarter", f"Q{q}", f"FY{str(fy_end)[-2:]}"

    # 3) Explicit quarter tokens like Q1 FY24
    m = re.search(r"\bq([1-4])\b.*?(fy\s*(\d{2,4})|(\d{4}))", sl)
    if m:
        q = int(m.group(1))
        # Extract FY year end
        yy = m.group(3) or m.group(4)
        year = 2000 + int(yy) if yy and len(yy) == 2 else (int(yy) if yy else None)
        # Assume Indian FY ends in March → quarter end months: Q1:Jun, Q2:Sep, Q3:Dec, Q4:Mar
        if year:
            month = [None, 6, 9, 12, 3][q]
            # Q4 FY2024 ends Mar 31, 2024; Q1 FY2024 ends Jun 30, 2023 (FY year maps to Mar end)
            if q in (1, 2, 3):
                fy_end = year
                cal_year = fy_end - 1 if q in (1, 2, 3) else fy_end
            else:  # Q4
                fy_end = year
                cal_year = fy_end
            # Pick last day approx (avoid calendar library dependency)
            day = 30 if month in (4, 6, 9, 11) else (28 if month == 2 else 31)
            dd = date(cal_year, month, day)
            return dd, "quarter", f"Q{q}", f"FY{str(fy_end)[-2:]}"

    # 4) Standalone date variants (no freq wording)
    dt = _extract_any_date(sl)
    if dt:
        return dt, None, None, None

    # 5) Year only (annual)
    m = re.search(r"\b(20\d{2})\b", sl)
    if m and ("year" in sl or "annual" in sl):
        yr = int(m.group(1))
        return date(yr, 3, 31), "annual", None, f"FY{str(yr)[-2:]}"

    return None, None, None, None


def build_period_index(labels_by_id: Dict[str, str]) -> Dict[str, Any]:
    """Build a period index enriched with parse results and fiscal hints.

    Returns a dict with:
      - by_id: { c1: { label, end_date, freq, fiscal_quarter, fiscal_year } }
      - fiscal_year_end_month: int (best guess from dates; defaults to 3)
    """
    out_by_id: Dict[str, Any] = {}
    months = []
    for cid, label in (labels_by_id or {}).items():
        end_date, freq, fq, fy = _parse_period_label(label)
        if end_date:
            months.append(end_date.month)
        out_by_id[str(cid).lower()] = {
            "label": label,
            "end_date": end_date.isoformat() if end_date else None,
            "freq": freq,
            "fiscal_quarter": fq,
            "fiscal_year": fy,
        }
    # Guess FY end month by mode of observed months; default to March (3)
    fy_end = 3
    if months:
        try:
            from collections import Counter
            fy_end = Counter(months).most_common(1)[0][0]
        except Exception:
            fy_end = 3
    # Fill missing freq/FY/FQ when we have end_date
    for cid, meta in out_by_id.items():
        if meta.get("end_date") and not meta.get("freq"):
            try:
                y, m, d = map(int, str(meta["end_date"]).split("-")[:3])
                # Heuristic: March year-end → annual if month == fy_end
                if m == int(fy_end):
                    meta["freq"] = "annual"
                    meta["fiscal_year"] = f"FY{str(y)[-2:]}"
                # Else, mark as point-in-time (Balance Sheet interim) if not quarterly wording
                else:
                    meta["freq"] = meta.get("freq") or "point"
            except Exception:
                pass
        # If we know end_date and freq=quarter, derive quarter tag when missing
        if meta.get("end_date") and meta.get("freq") == "quarter" and not meta.get("fiscal_quarter"):
            try:
                y, m, d = map(int, str(meta["end_date"]).split("-")[:3])
                if m in (4,5,6):   meta["fiscal_quarter"] = "Q1"; fy = y
                elif m in (7,8,9): meta["fiscal_quarter"] = "Q2"; fy = y
                elif m in (10,11,12): meta["fiscal_quarter"] = "Q3"; fy = y
                else: meta["fiscal_quarter"] = "Q4"; fy = y
                meta["fiscal_year"] = meta.get("fiscal_year") or f"FY{str(fy)[-2:]}"
            except Exception:
                pass
    return {"by_id": out_by_id, "fiscal_year_end_month": int(fy_end)}


# --- Common-size helpers ----------------------------------------------------

_DEFAULT_DENOMS = {
    "Profit And Loss Statement": [
        r"^\s*revenue\s*from\s*operations\b",
        r"^\s*total\s*income\b",
        r"^\s*income\s*from\s*operations\b",
    ],
    "Balance Sheet": [
        r"^\s*total\s*assets\b",
        r"^\s*total\s*equity\s*and\s*liabilit(?:y|ies)\b",
    ],
    "Cashflow": [
        r"^\s*revenue\b",
    ],
}


def _canon_doc_kind(doc_type: str) -> str:
    s = (doc_type or "").lower()
    if "loss" in s or "profit" in s:
        return "Profit And Loss Statement"
    if "balance" in s:
        return "Balance Sheet"
    if "cash" in s:
        return "Cashflow"
    return "Others"


def pick_common_size_denominator(doc_type: str, rows: List[dict]) -> Dict[str, Any] | None:
    """Pick a denominator row candidate for common-size statements.

    Returns { label: str, row_id: any, match: 'regex' } or None.
    """
    kind = _canon_doc_kind(doc_type)
    if kind == "Others":
        return None
    patterns = (CFG.get("analytics", {}).get("common_size", {}).get("denominators", {}) or {}).get(kind) or _DEFAULT_DENOMS.get(kind, [])
    parts = [(idx, str(r.get("Particulars") or r.get("particulars") or "")) for idx, r in enumerate(rows or [])]
    # prefer explicit 'id' if available
    ids = [r.get("id") for r in (rows or [])]
    for pat_s in patterns:
        try:
            pat = re.compile(str(pat_s), re.I)
        except Exception:
            continue
        for idx, label in parts:
            if pat.search(label):
                rid = (rows[idx] or {}).get("id", idx)
                return {"label": label, "row_id": rid, "match": pat_s}
    return None


# --- Data quality flags -----------------------------------------------------

def _num_cols_from_rows(rows: List[dict]) -> List[str]:
    cols = set()
    for r in rows or []:
        for k in r.keys():
            if re.fullmatch(r"(?i)[cp]\d+", str(k)):
                cols.add(k)
    return sorted(cols, key=lambda x: int(re.findall(r"\d+", x)[0]) if re.findall(r"\d+", x) else 0)


def _coerce_num(v) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if s == "" or s.lower() in {"na", "n/a", "nil", "none", "nan", "-", "–", "—"}:
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
        val = float(s)
        return -val if neg else val
    except Exception:
        return None


def quality_flags(doc_type: str, rows: List[dict], period_labels: Dict[str, str]) -> Dict[str, Any]:
    """Compute basic quality flags without depending on Excel generation.

    - sparsity: large share of missing values
    - tie_check: for Balance Sheet only (Total Assets vs. Total Equity & Liabilities)
    - denominator_presence: whether a reasonable common-size denominator exists
    """
    flags: List[str] = []
    checks: List[Dict[str, Any]] = []

    cols = _num_cols_from_rows(rows)
    n = len(rows or [])
    if not cols or n == 0:
        flags.append("no_numeric_columns")
        return {"flags": flags, "checks": checks}

    # sparsity: fraction of numeric cells that are None
    total_cells = n * len(cols)
    missing = 0
    for r in rows:
        for c in cols:
            if _coerce_num(r.get(c)) is None:
                missing += 1
    if total_cells > 0 and (missing / total_cells) > 0.6:
        flags.append("sparse_numeric_values")

    # Denominator presence
    denom = pick_common_size_denominator(doc_type, rows)
    if not denom:
        flags.append("common_size_denominator_not_found")

    # Tie check for Balance Sheet
    if "balance" in (doc_type or "").lower():
        ta_idx = None
        tel_idx = None
        for i, r in enumerate(rows):
            name = str(r.get("Particulars") or r.get("particulars") or "").lower()
            if ta_idx is None and re.search(r"^\s*total\s*assets\b", name):
                ta_idx = i
            if tel_idx is None and re.search(r"^\s*total\s*equity\s*and\s*liabilit(y|ies)\b", name):
                tel_idx = i
        if ta_idx is not None and tel_idx is not None:
            # tolerance: use config validation profile when available; else 0.5%
            tol_pct = float(((CFG.get("validation", {}) or {}).get("checks", {}) or {}).get("balance_sheet", {}).get("pct_tolerance", 0.005))
            for c in cols:
                a = _coerce_num(rows[ta_idx].get(c))
                b = _coerce_num(rows[tel_idx].get(c))
                if a is None or b is None:
                    continue
                diff = abs(float(a) - float(b))
                tol = max(1.0, tol_pct * max(abs(float(a)), abs(float(b))))
                ok = diff <= tol
                if not ok:
                    flags.append("balance_sheet_tie_mismatch")
                checks.append({
                    "check": "balance_sheet_tie",
                    "column": c,
                    "total_assets": a,
                    "equity_plus_liabilities": b,
                    "diff": diff,
                    "tolerance": tol,
                    "status": "OK" if ok else "MISMATCH",
                })

    return {"flags": sorted(set(flags)), "checks": checks}


# --- Footnote linkage -------------------------------------------------------

_NOTE_PATTERNS = [
    re.compile(r"\bnote\s*\d+\b", re.I),
    re.compile(r"\(i+\)", re.I),  # (i), (ii), ...
    re.compile(r"\[[0-9]+\]"),     # [1], [2]
    re.compile(r"\^\d+"),          # superscript-like markers
]


def extract_footnote_refs(rows: List[dict]) -> Dict[str, List[str]]:
    """Extract light-weight footnote references from the 'Particulars' text.

    Returns { row_key: [ref1, ref2, ...] } where row_key prefers 'id' and
    falls back to the row index. This does not attempt to resolve the note
    text; it only normalizes and lists references present with each line item.
    """
    out: Dict[str, List[str]] = {}
    for i, r in enumerate(rows or []):
        key = str(r.get("id") if r.get("id") is not None else i)
        s = str(r.get("Particulars") or r.get("particulars") or "")
        refs: List[str] = []
        if not s:
            continue
        for pat in _NOTE_PATTERNS:
            for m in pat.finditer(s):
                token = m.group(0)
                if token not in refs:
                    refs.append(token)
        if refs:
            out[key] = refs
    return out


__all__ = [
    "detect_units_and_currency",
    "build_period_index",
    "pick_common_size_denominator",
    "quality_flags",
    "extract_footnote_refs",
    "compute_common_size",
    "compute_period_math",
]


# --- Common-size table computation -----------------------------------------

def _rows_from_df_like(rows: List[dict]) -> List[dict]:
    # Already rows; keep as-is (helper kept for parity with excel_ops)
    return rows or []


def _period_cols_in_rows(rows: List[dict]) -> List[str]:
    return _num_cols_from_rows(rows)


def compute_common_size(doc_type: str, rows: List[dict], labels_by_id: Dict[str, str]) -> Dict[str, Any] | None:
    denom = pick_common_size_denominator(doc_type, rows)
    if not denom:
        return None
    cols = _period_cols_in_rows(rows)
    if not cols:
        return {"denominator": denom, "rows": []}
    # Locate denominator row index
    denom_idx = None
    for i, r in enumerate(rows):
        if r.get("id") == denom.get("row_id"):
            denom_idx = i; break
        name = str(r.get("Particulars") or r.get("particulars") or "")
        if name == denom.get("label") and denom_idx is None:
            denom_idx = i
    if denom_idx is None:
        # fallback: first row that matches label substring
        for i, r in enumerate(rows):
            name = str(r.get("Particulars") or r.get("particulars") or "")
            if denom.get("label") and denom.get("label") in name:
                denom_idx = i; break
    if denom_idx is None:
        return {"denominator": denom, "rows": []}

    out_rows = []
    for r in rows:
        name = str(r.get("Particulars") or r.get("particulars") or "").strip()
        rid = r.get("id")
        rec = {"id": rid, "Particulars": name}
        for c in cols:
            num = _coerce_num(r.get(c))
            den = _coerce_num(rows[denom_idx].get(c))
            if num is None or den in (None, 0):
                rec[c] = None
            else:
                rec[c] = (float(num) / float(den)) * 100.0
        out_rows.append(rec)
    return {"denominator": denom, "rows": out_rows, "period_columns": cols}


# --- Period math for headline series ---------------------------------------

_PL_HEADLINE_PATTERNS = [
    r"^\s*revenue\s*from\s*operations\b",
    r"^\s*total\s*income\b",
]
_BS_HEADLINE_PATTERNS = [
    r"^\s*total\s*assets\b",
]
_CF_HEADLINE_PATTERNS = [
    r"net\s*(increase|decrease).*cash",
    r"cash\s*and\s*cash\s*equivalents\s*at\s*the\s*end",
]


def _find_row_index_by_patterns(rows: List[dict], pats: List[str]) -> int | None:
    for i, r in enumerate(rows or []):
        name = str(r.get("Particulars") or r.get("particulars") or "")
        for ps in pats:
            try:
                if re.search(ps, name, re.I):
                    return i
            except Exception:
                continue
    return None


def _series_for_row(rows: List[dict], idx: int, labels_by_id: Dict[str, str]) -> List[Tuple[str, date, float | None]]:
    cols = _period_cols_in_rows(rows)
    # Build mapping col->end_date
    pidx = build_period_index(labels_by_id or {})
    by_id = (pidx.get("by_id") or {})
    items: List[Tuple[str, date, float | None]] = []
    for c in cols:
        label_meta = by_id.get(str(c).lower())
        dt = None
        if label_meta and label_meta.get("end_date"):
            try:
                y, m, d = map(int, str(label_meta["end_date"]).split("-")[:3])
                dt = date(y, m, d)
            except Exception:
                dt = None
        val = _coerce_num(rows[idx].get(c))
        if dt is not None:
            items.append((c, dt, val))
    items.sort(key=lambda t: t[1])
    return items


def _pct(a: float | None, b: float | None) -> float | None:
    if a is None or b in (None, 0):
        return None
    try:
        return (a / b - 1.0) * 100.0
    except Exception:
        return None


def compute_period_math(doc_type: str, rows: List[dict], labels_by_id: Dict[str, str]) -> Dict[str, Any] | None:
    kind = _canon_doc_kind(doc_type)
    if kind == "Others":
        return None
    if kind == "Profit And Loss Statement":
        idx = _find_row_index_by_patterns(rows, _PL_HEADLINE_PATTERNS)
    elif kind == "Balance Sheet":
        idx = _find_row_index_by_patterns(rows, _BS_HEADLINE_PATTERNS)
    else:
        idx = _find_row_index_by_patterns(rows, _CF_HEADLINE_PATTERNS)
    if idx is None:
        # fallback to denominator row
        denom = pick_common_size_denominator(doc_type, rows)
        if denom:
            for i, r in enumerate(rows or []):
                if r.get("id") == denom.get("row_id") or (str(r.get("Particulars") or r.get("particulars") or "") == denom.get("label")):
                    idx = i; break
    if idx is None:
        return None

    series = _series_for_row(rows, idx, labels_by_id)
    if not series:
        return None

    # QoQ and YoY for quarterly; YoY for annual
    qoq: Dict[str, float | None] = {}
    yoy: Dict[str, float | None] = {}

    # Build maps by date
    by_date = {dt: (col, val) for (col, dt, val) in series}
    dates_sorted = [dt for (_, dt, _) in series]

    for i, dt in enumerate(dates_sorted):
        col, val = by_date[dt]
        # QoQ (previous point)
        if i >= 1:
            prev_dt = dates_sorted[i-1]
            _, prev_val = by_date[prev_dt]
            qoq[col] = _pct(val, prev_val)
        else:
            qoq[col] = None
        # YoY (12 months prior)
        prev_year = date(dt.year - 1, dt.month, dt.day)
        if prev_year in by_date:
            _, prev_val = by_date[prev_year]
            yoy[col] = _pct(val, prev_val)
        else:
            yoy[col] = None

    # TTM (sum of last four quarters) — only if we have ≥ 4 observations within ~1 year range
    ttm: Dict[str, float | None] = {}
    for i, dt in enumerate(dates_sorted):
        if i >= 3:
            vals = [by_date[dates_sorted[i - k]][1] for k in range(0, 4)]
            if any(v is None for v in vals):
                ttm[by_date[dt][0]] = None
            else:
                ttm[by_date[dt][0]] = float(sum(vals))
        else:
            ttm[by_date[dt][0]] = None

    # CAGR across annual series if available
    # Identify annual points as those with month == fiscal_year_end_month
    pidx = build_period_index(labels_by_id or {})
    fy_end = int(pidx.get("fiscal_year_end_month", 3) or 3)
    annual_points = [(dt, by_date[dt][1]) for dt in dates_sorted if dt.month == fy_end]
    cagr = None
    if len(annual_points) >= 2:
        start, sv = annual_points[0]
        end, ev = annual_points[-1]
        n_years = max(1, end.year - start.year)
        if sv not in (None, 0) and ev not in (None, 0):
            try:
                cagr = (float(ev) / float(sv)) ** (1.0 / n_years) - 1.0
                cagr *= 100.0
            except Exception:
                cagr = None

    return {
        "row": {
            "index": idx,
            "label": str(rows[idx].get("Particulars") or rows[idx].get("particulars") or ""),
            "id": rows[idx].get("id"),
        },
        "qoq_pct": qoq,
        "yoy_pct": yoy,
        "ttm": ttm,
        "cagr_pct": cagr,
    }


def compute_common_size_cashflow(cf_rows: List[dict], pl_rows: List[dict], labels_by_id: Dict[str, str]) -> Dict[str, Any] | None:
    """Compute CF common-size using P&L revenue as denominator when available."""
    cols = _period_cols_in_rows(cf_rows)
    if not cols:
        return None
    # Find P&L revenue row
    pats = _DEFAULT_DENOMS.get("Profit And Loss Statement", [])
    idx = _find_row_index_by_patterns(pl_rows or [], pats)
    if idx is None:
        # fallback to CF intrinsic denominator if any
        return compute_common_size("Cashflow", cf_rows, labels_by_id)
    out_rows = []
    for r in cf_rows or []:
        name = str(r.get("Particulars") or r.get("particulars") or "").strip()
        rid = r.get("id")
        rec = {"id": rid, "Particulars": name}
        for c in cols:
            num = _coerce_number_like(r.get(c))
            den = _coerce_number_like(pl_rows[idx].get(c))
            if num is None or den in (None, 0):
                rec[c] = None
            else:
                rec[c] = (float(num) / float(den)) * 100.0
        out_rows.append(rec)
    denom = {"label": str(pl_rows[idx].get("Particulars") or pl_rows[idx].get("particulars") or ""), "row_id": pl_rows[idx].get("id"), "match": "P&L revenue cross-link"}
    return {"denominator": denom, "rows": out_rows, "period_columns": cols}


def compute_period_math_multi(doc_type: str, rows: List[dict], labels_by_id: Dict[str, str]) -> Dict[str, Any] | None:
    cfg = (((CFG.get("analytics", {}) or {}).get("period_math", {}) or {}).get("kpis", {}) or {})
    kind = _canon_doc_kind(doc_type)
    kpi_list = cfg.get(kind) or []
    if not kpi_list:
        return None
    out: Dict[str, Any] = {}
    for item in kpi_list:
        name = str(item.get("name") or "").strip() if isinstance(item, dict) else None
        pats = item.get("patterns") if isinstance(item, dict) else None
        if not name or not pats:
            continue
        idx = _find_row_index_by_patterns(rows, pats)
        if idx is None:
            continue
        series = _series_for_row(rows, idx, labels_by_id)
        if not series:
            continue
        # Build maps by date
        by_date = {dt: (col, val) for (col, dt, val) in series}
        dates_sorted = [dt for (_, dt, _) in series]
        qoq: Dict[str, float | None] = {}
        yoy: Dict[str, float | None] = {}
        for i, dt in enumerate(dates_sorted):
            col, val = by_date[dt]
            if i >= 1:
                prev_dt = dates_sorted[i-1]
                _, prev_val = by_date[prev_dt]
                qoq[col] = _pct(val, prev_val)
            else:
                qoq[col] = None
            prev_year = date(dt.year - 1, dt.month, dt.day)
            if prev_year in by_date:
                _, prev_val = by_date[prev_year]
                yoy[col] = _pct(val, prev_val)
            else:
                yoy[col] = None
        out[name] = {"qoq_pct": qoq, "yoy_pct": yoy}
    return out or None


# Helpers for core pack
def _series_by_date(rows: List[dict], labels_by_id: Dict[str, str], patterns: List[str]) -> Dict[date, float]:
    idx = _find_row_index_by_patterns(rows, patterns)
    if idx is None:
        return {}
    series = _series_for_row(rows, idx, labels_by_id)
    out: Dict[date, float] = {}
    for _, dt, val in series:
        if val is None:
            continue
        out[dt] = float(val)
    return out


def _align_dates(*series: Dict[date, float]) -> List[date]:
    if not series:
        return []
    common = None
    for s in series:
        dts = set(s.keys())
        common = dts if common is None else (common & dts)
    return sorted(list(common or set()))


def _days_for_dt(dt: date, fy_end_month: int = 3) -> int:
    return 365 if dt.month == int(fy_end_month or 3) else 90


def compute_core_pack(
    combined_rows_by_doc: Dict[str, List[dict]] | None,
    periods_by_doc: Dict[str, Dict[str, Any]] | None,
    cfg: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    cfg = cfg or {}
    rows_by_doc = combined_rows_by_doc or {}
    pmap_by_doc = periods_by_doc or {}

    def _labels_for_doc(doc: str) -> Dict[str, str]:
        raw = (pmap_by_doc.get(doc) or {})
        out: Dict[str, str] = {}
        for k, v in raw.items():
            if isinstance(v, dict):
                out[str(k).lower()] = str(v.get("label", ""))
            else:
                out[str(k).lower()] = str(v)
        return out

    # Prefer consolidated docs, else standalone
    PL = "Consolidated Profit and Loss Statement" if "Consolidated Profit and Loss Statement" in rows_by_doc else "Standalone Profit and Loss Statement"
    BS = "Consolidated Balance Sheet" if "Consolidated Balance Sheet" in rows_by_doc else "Standalone Balance Sheet"
    CF = "Consolidated Cashflow" if "Consolidated Cashflow" in rows_by_doc else "Standalone Cashflow"

    pl_rows = rows_by_doc.get(PL) or []
    bs_rows = rows_by_doc.get(BS) or []
    cf_rows = rows_by_doc.get(CF) or []
    pl_labels = _labels_for_doc(PL)
    bs_labels = _labels_for_doc(BS)
    cf_labels = _labels_for_doc(CF)
    fy_end = 3
    try:
        fy_end = int(build_period_index(pl_labels).get("fiscal_year_end_month", 3) or 3)
    except Exception:
        pass

    def _pat(ns, key, default):
        try:
            return (CFG.get("analytics", {}) or {}).get("kpi_patterns", {}).get(ns, {}).get(key, default)
        except Exception:
            return default

    # Patterns
    rev_pat     = _pat("pl", "revenue", _DEFAULT_DENOMS["Profit And Loss Statement"])
    ebitda_pat  = _pat("pl", "ebitda", [r"\bebitda\b", r"earnings before interest, tax, depreciation and amortisation"])  # spellings
    ebit_pat    = _pat("pl", "ebit", [r"\bprofit\s*before\s*interest\s*and\s*tax\b", r"\boperating\s*profit\b", r"\bebit\b"])
    np_pat      = _pat("pl", "net_profit", [r"\bprofit\s*after\s*tax\b", r"\bpat\b", r"\bnet\s*profit\b"])
    gp_pat      = _pat("pl", "gross_profit", [r"\bgross\s*profit\b"])
    cogs_pat    = _pat("pl", "cogs", [r"\bcost\s*of\s*(goods|materials)\b", r"\bcosts?\s*of\s*revenue\b"])

    ocf_pat     = _pat("cf", "ocf", [r"\bnet\s*cash\s*from\s*operating\s*activities\b", r"\bcash\s*generated\s*from\s*operations\b"])
    capex_pat   = _pat("cf", "capex", [r"\bpurchase\s*of\s*property,?\s*plant\s*and\s*equipment\b", r"\bcapital\s*expenditure\b"])  # negative values

    recv_pat    = _pat("bs", "receivables", [r"\btrade\s*receivables\b", r"\baccounts\s*receivable\b"])
    pay_pat     = _pat("bs", "payables", [r"\btrade\s*payables\b", r"\baccounts\s*payable\b"])
    inv_pat     = _pat("bs", "inventory", [r"\binventor(y|ies)\b", r"\bstock\b"])
    assets_pat  = _pat("bs", "total_assets", [r"^\s*total\s*assets\b"])  # already defaulted
    equity_pat  = _pat("bs", "equity", [r"\btotal\s*equity\b", r"\bshareholders'?\s*funds\b", r"\bnet\s*worth\b"])
    debt_pat    = _pat("bs", "total_debt", [r"\bborrowings\b", r"\btotal\s*debt\b"])  # may need sum of long+short
    cash_pat    = _pat("bs", "cash", [r"\bcash\s*and\s*cash\s*equivalents\b", r"\bcash\b"])

    int_exp_pat = _pat("pl", "interest_expense", [r"\bfinance\s*costs\b", r"\binterest\s*expense\b"])
    tax_exp_pat = _pat("pl", "tax_expense", [r"\btax\s*expense\b", r"\bcurrent\s*tax\b"])  # rough
    pbt_pat     = _pat("pl", "pbt", [r"\bprofit\s*before\s*tax\b", r"\bpbt\b"])

    # Build series by date
    rev  = _series_by_date(pl_rows, pl_labels, rev_pat)
    ebitda = _series_by_date(pl_rows, pl_labels, ebitda_pat)
    ebit   = _series_by_date(pl_rows, pl_labels, ebit_pat)
    np     = _series_by_date(pl_rows, pl_labels, np_pat)
    gp     = _series_by_date(pl_rows, pl_labels, gp_pat)
    cogs   = _series_by_date(pl_rows, pl_labels, cogs_pat)

    ocf    = _series_by_date(cf_rows, cf_labels, ocf_pat)
    capex  = _series_by_date(cf_rows, cf_labels, capex_pat)

    recv   = _series_by_date(bs_rows, bs_labels, recv_pat)
    pay    = _series_by_date(bs_rows, bs_labels, pay_pat)
    inv    = _series_by_date(bs_rows, bs_labels, inv_pat)
    assets = _series_by_date(bs_rows, bs_labels, assets_pat)
    equity = _series_by_date(bs_rows, bs_labels, equity_pat)
    debt   = _series_by_date(bs_rows, bs_labels, debt_pat)
    cash   = _series_by_date(bs_rows, bs_labels, cash_pat)

    int_exp = _series_by_date(pl_rows, pl_labels, int_exp_pat)
    tax_exp = _series_by_date(pl_rows, pl_labels, tax_exp_pat)
    pbt     = _series_by_date(pl_rows, pl_labels, pbt_pat)

    # Growth & margins
    dates = sorted(set(rev.keys()))
    margins = {"gross": {}, "ebitda": {}, "ebit": {}, "np": {}}
    for dt in dates:
        r = rev.get(dt)
        if r in (None, 0):
            continue
        if dt in gp:
            margins["gross"][dt.isoformat()] = (gp[dt] / r) * 100.0
        elif dt in cogs:
            margins["gross"][dt.isoformat()] = ((r - cogs[dt]) / r) * 100.0
        if dt in ebitda:
            margins["ebitda"][dt.isoformat()] = (ebitda[dt] / r) * 100.0
        if dt in ebit:
            margins["ebit"][dt.isoformat()] = (ebit[dt] / r) * 100.0
        if dt in np:
            margins["np"][dt.isoformat()] = (np[dt] / r) * 100.0

    # Cash conversion
    fcf: Dict[str, float] = {}
    fcf_margin: Dict[str, float] = {}
    cash_conv: Dict[str, float] = {}
    capex_intensity: Dict[str, float] = {}
    for dt in _align_dates(ocf, capex, rev, ebitda):
        rid = dt.isoformat()
        ocfv = ocf.get(dt)
        capv = capex.get(dt)
        rv = rev.get(dt)
        ev = ebitda.get(dt)
        if ocfv is not None and capv is not None:
            fcf[rid] = float(ocfv) - float(capv)
        if rv not in (None, 0) and rid in fcf:
            fcf_margin[rid] = (fcf[rid] / rv) * 100.0
        if ev not in (None, 0) and ocfv is not None:
            cash_conv[rid] = (ocfv / ev)
        if rv not in (None, 0) and capv is not None:
            capex_intensity[rid] = (capv / rv) * 100.0

    # Working capital & CCC
    dso: Dict[str, float] = {}
    dpo: Dict[str, float] = {}
    dio: Dict[str, float] = {}
    ccc: Dict[str, float] = {}
    for dt in _align_dates(recv, pay, inv, rev):
        days = _days_for_dt(dt, fy_end)
        rid = dt.isoformat()
        rv = rev.get(dt)
        if rv not in (None, 0):
            if dt in recv:
                dso[rid] = (recv[dt] / rv) * days
        base = cogs.get(dt) if dt in cogs and cogs.get(dt) not in (None, 0) else rv
        if base not in (None, 0):
            if dt in pay:
                dpo[rid] = (pay[dt] / base) * days
            if dt in inv:
                dio[rid] = (inv[dt] / base) * days
        if rid in dso and rid in dpo and rid in dio:
            ccc[rid] = dso[rid] + dio[rid] - dpo[rid]

    # Profitability trees: DuPont ROE
    roe: Dict[str, float] = {}
    dupont: Dict[str, Dict[str, float]] = {}
    for dt in _align_dates(np, rev, assets, equity):
        rid = dt.isoformat()
        rv = rev.get(dt)
        av = assets.get(dt)
        ev = equity.get(dt)
        nv = np.get(dt)
        if None in (rv, av, ev) or rv == 0 or av == 0 or ev == 0:
            continue
        npm = nv / rv
        at = rv / av
        lev = av / ev
        roe[rid] = (npm * at * lev) * 100.0
        dupont[rid] = {"npm_pct": npm * 100.0, "asset_turnover": at, "leverage": lev}

    # ROIC & spread vs WACC (approx)
    roic: Dict[str, float] = {}
    spread: Dict[str, float] = {}
    try:
        wacc = float(((CFG.get("analytics", {}) or {}).get("wacc", 0.12))) * 100.0
    except Exception:
        wacc = 12.0
    for dt in _align_dates(ebit, tax_exp, pbt, assets):
        rid = dt.isoformat()
        ebit_v = ebit.get(dt)
        pbt_v = pbt.get(dt)
        tax_v = tax_exp.get(dt)
        ic = assets.get(dt)
        if None in (ebit_v, ic) or ic == 0:
            continue
        eff_tax = None
        if pbt_v not in (None, 0) and tax_v is not None:
            try:
                eff_tax = max(0.0, min(1.0, float(tax_v) / float(pbt_v)))
            except Exception:
                eff_tax = None
        nopat = ebit_v * (1.0 - (eff_tax if eff_tax is not None else 0.25))
        roic_val = (nopat / ic) * 100.0
        roic[rid] = roic_val
        spread[rid] = roic_val - wacc

    # Earnings quality
    accruals_ratio: Dict[str, float] = {}
    fcf_vs_np: Dict[str, float] = {}
    for dt in _align_dates(np, ocf, assets):
        rid = dt.isoformat()
        nv = np.get(dt)
        ocfv = ocf.get(dt)
        av = assets.get(dt)
        if None not in (nv, ocfv, av) and av != 0:
            accruals_ratio[rid] = ((nv - ocfv) / av) * 100.0
    for dt in _align_dates(np, ocf, capex):
        rid = dt.isoformat()
        nv = np.get(dt)
        ocfv = ocf.get(dt)
        capv = capex.get(dt)
        if None not in (nv, ocfv, capv) and nv != 0:
            fcf_vs_np[rid] = ((ocfv - capv) / nv)

    # Leverage & solvency
    net_debt: Dict[str, float] = {}
    nd_to_ebitda: Dict[str, float] = {}
    int_cov: Dict[str, float] = {}
    for dt, dv in debt.items():
        rid = dt.isoformat()
        net_debt[rid] = float(dv) - float(cash.get(dt, 0.0))
    for dt in _align_dates({date.fromordinal(d.toordinal()): v for d, v in list(debt.items())}, ebitda):
        # align by same dt keys present in both; above trick forces type mapping, but we already have date keys
        rid = dt.isoformat()
        nd = net_debt.get(rid)
        ev = ebitda.get(dt)
        if nd is not None and ev not in (None, 0):
            nd_to_ebitda[rid] = nd / ev
    for dt in _align_dates(ebit, int_exp):
        rid = dt.isoformat()
        ev = ebit.get(dt)
        iv = int_exp.get(dt)
        if iv not in (None, 0) and ev is not None:
            int_cov[rid] = ev / iv

    segments = {"by_segment": [], "by_geo": [], "notes": "No structured segment rows detected"}

    return {
        "growth_margin": {
            "margins_pct": margins,
        },
        "cash_conversion": {
            "ocf": {k.isoformat(): v for k, v in ocf.items()},
            "fcf": fcf,
            "fcf_margin_pct": fcf_margin,
            "cash_conversion_ratio": cash_conv,
            "capex_intensity_pct": capex_intensity,
        },
        "working_capital_ccc": {
            "dso_days": dso,
            "dpo_days": dpo,
            "dio_days": dio,
            "ccc_days": ccc,
        },
        "profitability": {
            "roe_pct": roe,
            "dupont": dupont,
            "roic_pct": roic,
            "spread_vs_wacc_pct": spread,
        },
        "earnings_quality": {
            "accruals_ratio_pct_of_assets": accruals_ratio,
            "fcf_vs_np_ratio": fcf_vs_np,
            "notes": "One-offs isolation/tax normalization requires detailed mapping; left for future",
        },
        "leverage_solvency": {
            "net_debt": net_debt,
            "net_debt_to_ebitda": nd_to_ebitda,
            "interest_coverage": int_cov,
            "debt_maturity": {},
        },
        "segments": segments,
    }
