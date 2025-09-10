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
]
