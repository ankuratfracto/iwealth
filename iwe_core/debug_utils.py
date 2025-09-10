"""Debug/validation utilities gated by env vars and config.

Centralizes lightweight feature flags and printing helpers (`dprint`,
`vprint`) controlled by environment variables or keys under `CFG['debug']`.
Use these to emit optional diagnostic output without cluttering logs.
"""

from __future__ import annotations

import os
from typing import Any

from .config import CFG


def _truthy(val: Any) -> bool:
    try:
        if isinstance(val, bool):
            return val
        s = str(val).strip().lower()
        return s in {"1", "true", "yes", "y", "on"}
    except Exception:
        return False


def debug_flag_from_cfg(env_name: str, cfg_key: str, default: bool = False) -> bool:
    """Resolve a debug flag using env var override, else config, else default.

    - env_name: environment variable to check (e.g., IWEALTH_DEBUG_JSON)
    - cfg_key: key under CFG['debug'] (e.g., 'json_extraction')
    """
    try:
        env = os.getenv(env_name)
        if env is not None:
            return _truthy(env)
        dbg = (CFG.get("debug", {}) or {}).get(cfg_key)
        return _truthy(dbg) if dbg is not None else bool(default)
    except Exception:
        return bool(default)


def debug_enabled() -> bool:
    return debug_flag_from_cfg("IWEALTH_DEBUG_JSON", "json_extraction", False)


def valdbg_enabled() -> bool:
    return debug_flag_from_cfg("IWEALTH_DEBUG_VALIDATION", "validation", False)


def dprint(*args, **kwargs) -> None:
    if debug_enabled():
        try:
            print("[JSONDBG]", *args, **kwargs, flush=True)
        except Exception:
            pass


def vprint(*args, **kwargs) -> None:
    if valdbg_enabled():
        try:
            print("[VALDBG]", *args, **kwargs, flush=True)
        except Exception:
            pass


# Backwards-compat aliases commonly used in modules
_dprint = dprint
_vprint = vprint
_debug_enabled = debug_enabled
_valdbg_enabled = valdbg_enabled
_debug_flag_from_cfg = debug_flag_from_cfg

__all__ = [
    "debug_flag_from_cfg",
    "debug_enabled",
    "valdbg_enabled",
    "dprint",
    "vprint",
    "_dprint",
    "_vprint",
    "_debug_enabled",
    "_valdbg_enabled",
    "_debug_flag_from_cfg",
]
