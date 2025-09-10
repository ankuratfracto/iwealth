from __future__ import annotations

from pathlib import Path
import os
import yaml


def _deep_update(dst: dict, src: dict | None) -> dict:
    for k, v in (src or {}).items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_update(dst[k], v)
        else:
            dst[k] = v
    return dst


def _find_config_path() -> Path:
    """
    Locate config.yaml near the project root. Prefers repository root
    (parent of this package), falling back to CWD.
    """
    pkg_dir = Path(__file__).resolve().parent
    root = pkg_dir.parent
    cand = root / "config.yaml"
    if cand.exists():
        return cand
    # Fallback to CWD
    return Path.cwd() / "config.yaml"


def load_config() -> dict:
    cfg_path = _find_config_path()
    cfg = yaml.safe_load(cfg_path.read_text()) if cfg_path.exists() else {}
    # optional local overrides
    local = (cfg.get("paths", {}) or {}).get("config_local", "config.local.yaml")
    lp = (cfg_path.parent / local)
    if lp.exists():
        _deep_update(cfg, yaml.safe_load(lp.read_text()))
    # env overrides (examples)
    if os.getenv("FRACTO_API_KEY"):
        cfg.setdefault("api", {})["api_key_env"] = "FRACTO_API_KEY"
    if os.getenv("FRACTO_EXPAND_NEIGHBORS"):
        cfg.setdefault("passes", {}).setdefault("first", {}).setdefault("selection", {})[
            "neighbor_radius"
        ] = int(os.getenv("FRACTO_EXPAND_NEIGHBORS"))
    return cfg


# Global singleton config for convenience
CFG: dict = load_config()

__all__ = ["load_config", "CFG"]

