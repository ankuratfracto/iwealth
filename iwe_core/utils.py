"""Generic utility helpers shared across modules.

Includes company type classification from tokens, strict truth‑flag checks,
and compact integer range formatting for logging/UI.
"""

from __future__ import annotations

from typing import Iterable, Optional


def company_type_from_token(token: str) -> Optional[str]:
    """Classify a token to a company type key: bank | nbfc | insurance | corporate.

    Mirrors existing local helpers but centralized here.
    """
    t = (token or "").strip().lower()
    # Avoid misreading "non banking ..." as "bank"
    if "nbfc" in t or "non banking financial" in t or "non-banking financial" in t:
        return "nbfc"
    if "insur" in t:
        return "insurance"
    if "bank" in t and "non banking" not in t and "non-banking" not in t:
        return "bank"
    if "non financial" in t or "corporate" in t or "company" in t:
        return "corporate"
    return None


def is_true_flag(x) -> bool:
    """Strict truth flag used by classifiers: true/1/yes/y/on (case-insensitive)."""
    try:
        return str(x).strip().lower() in ("true", "1", "yes", "y", "on")
    except Exception:
        return False


def format_ranges(nums: Iterable[int]) -> str:
    """Format a list of ints into compact ranges like '1-3,5,7-9'."""
    arr = sorted({int(n) for n in (nums or [])})
    if not arr:
        return ""
    out = []
    start = prev = arr[0]
    for n in arr[1:]:
        if n == prev + 1:
            prev = n
            continue
        out.append(f"{start}-{prev}" if start != prev else f"{start}")
        start = prev = n
    out.append(f"{start}-{prev}" if start != prev else f"{start}")
    return ",".join(out)


__all__ = [
    "company_type_from_token",
    "is_true_flag",
    "format_ranges",
]
