"""Configuration loading and logging setup for iWealth.

Locates and loads `config.yaml` (with optional local overrides), exposes a
singleton `CFG` dict, and provides `configure_logging()` to initialize rich
console/file logging with optional JSON formatting and fine‑grained controls.
"""

from __future__ import annotations

from pathlib import Path
import os
import yaml
import logging
from logging.handlers import RotatingFileHandler
import json as _json
import time as _time
import sys as _sys


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


def configure_logging() -> str | None:
    """
    Configure root logging from CFG['logging'].

    Supports keys:
      - level: str (e.g., "INFO", "DEBUG")
      - json: bool (emit JSON lines if true)
      - include_timing: bool (include asctime)
      - include_logger_name: bool
      - include_module: bool (module name)
      - include_path: bool (pathname instead of module)
      - include_lineno: bool
      - include_process: bool
      - include_thread: bool
      - utc: bool (use UTC timestamps)
      - datefmt: str (e.g., "%H:%M:%S")
      - console: bool (enable console handler, default true)
      - console_level: str (override console level)
      - file: str (path to log file; created if missing)
      - file_level: str (override file level)
      - rotate_max_bytes: int (default 5_000_000)
      - rotate_backups: int (default 5)
      - capture_warnings: bool (redirect warnings to logging)
      - capture_prints: bool (mirror stdout/stderr to logging)
      - print_level_out: str (level for stdout, default INFO)
      - print_level_err: str (level for stderr, default ERROR)
      - force_reconfigure: bool (clear existing handlers)

    Returns the resolved logfile path if a file handler is configured.
    """
    log_cfg = (CFG.get("logging") or {})

    # Optional preset: quiet | normal | verbose. Acts as defaults, user keys win.
    _presets = {
        "quiet": {
            "level": "INFO",
            "console_level": "INFO",
            "file_level": "INFO",
            "loggers": {"iwe_core": "INFO"},
            "libs": {"urllib3": "WARNING", "requests": "WARNING"},
        },
        "normal": {
            "level": "INFO",
            "console_level": "INFO",
            "file_level": "INFO",
            "loggers": {},
            "libs": {"urllib3": "WARNING"},
        },
        "verbose": {
            "level": "DEBUG",
            "console_level": "DEBUG",
            "file_level": "DEBUG",
            "loggers": {"iwe_core": "DEBUG"},
            "libs": {"urllib3": "INFO", "requests": "INFO"},
        },
    }
    _preset_name = str(log_cfg.get("preset", "")).strip().lower()
    _preset = _presets.get(_preset_name, {}) if _preset_name else {}

    level_name = str(log_cfg.get("level", _preset.get("level", "INFO"))).upper()
    level = getattr(logging, level_name, logging.INFO)

    # Build formatters
    use_json = bool(log_cfg.get("json", False))
    include_timing = bool(log_cfg.get("include_timing", True))
    include_logger_name = bool(log_cfg.get("include_logger_name", False))
    include_module = bool(log_cfg.get("include_module", False))
    include_path = bool(log_cfg.get("include_path", False))
    include_lineno = bool(log_cfg.get("include_lineno", False))
    include_process = bool(log_cfg.get("include_process", False))
    include_thread = bool(log_cfg.get("include_thread", False))
    utc = bool(log_cfg.get("utc", False))
    datefmt = str(log_cfg.get("datefmt", "%H:%M:%S")) if not use_json else None

    class _JsonFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
            payload = {
                "time": self.formatTime(record) if include_timing else None,
                "level": record.levelname,
                "name": record.name if include_logger_name else None,
                "module": record.pathname if include_path else (record.module if include_module else None),
                "lineno": record.lineno if include_lineno else None,
                "process": record.process if include_process else None,
                "thread": record.threadName if include_thread else None,
                "message": record.getMessage(),
            }
            # Remove None fields
            payload = {k: v for k, v in payload.items() if v is not None}
            return _json.dumps(payload, ensure_ascii=False)

    if use_json:
        formatter = _JsonFormatter()
    else:
        parts = []
        if include_timing:
            parts.append("%(asctime)s")
        parts.append("%(levelname)-8s")
        if include_logger_name:
            parts.append("%(name)s")
        loc = None
        if include_path:
            loc = "%(pathname)s"
        elif include_module:
            loc = "%(module)s"
        if loc:
            if include_lineno:
                parts.append(f"{loc}:%(lineno)d")
            else:
                parts.append(loc)
        if include_process:
            parts.append("pid=%(process)d")
        if include_thread:
            parts.append("thr=%(threadName)s")
        parts.append("%(message)s")
        fmt = " ".join(parts)
        formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
    if utc:
        converter = _time.gmtime
        try:
            formatter.converter = converter  # type: ignore[attr-defined]
        except Exception:
            pass

    root = logging.getLogger()
    force_reconf = bool(log_cfg.get("force_reconfigure", False))
    if force_reconf:
        for h in list(root.handlers):
            root.removeHandler(h)
    # Otherwise, keep any pre-existing handlers and add ours.

    root.setLevel(level)

    # Console handler
    console_level_name = str(log_cfg.get("console_level", _preset.get("console_level", level_name))).upper()
    if bool(log_cfg.get("console", True)):
        ch = logging.StreamHandler()
        ch.setLevel(getattr(logging, console_level_name, level))
        ch.setFormatter(formatter)
        root.addHandler(ch)

    logfile_used: str | None = None
    # Optional rotating file handler
    file_path = str(log_cfg.get("file", "")).strip()
    if file_path:
        try:
            # Resolve relative to repo root (same as config path parent)
            cfg_path = _find_config_path()
            base_dir = cfg_path.parent
            log_path = (base_dir / file_path).expanduser().resolve()
            log_path.parent.mkdir(parents=True, exist_ok=True)

            max_bytes = int(log_cfg.get("rotate_max_bytes", 5_000_000))
            backups = int(log_cfg.get("rotate_backups", 5))

            fh = RotatingFileHandler(str(log_path), maxBytes=max_bytes, backupCount=backups, encoding="utf-8")
            file_level_name = str(log_cfg.get("file_level", _preset.get("file_level", level_name))).upper()
            fh.setLevel(getattr(logging, file_level_name, level))
            fh.setFormatter(formatter)
            root.addHandler(fh)
            logfile_used = str(log_path)
        except Exception:
            # Fail open: logging to console still works
            pass

    # Capture warnings
    if bool(log_cfg.get("capture_warnings", True)):
        try:
            logging.captureWarnings(True)
        except Exception:
            pass

    # Optionally mirror prints to logging
    if bool(log_cfg.get("capture_prints", False)):
        class _StreamToLogger:
            def __init__(self, logger: logging.Logger, level: int):
                self._logger = logger
                self._level = level
                self._buf = ""
            def write(self, message: str):
                try:
                    msg = message.rstrip()
                    if msg:
                        self._logger.log(self._level, msg)
                except Exception:
                    pass
            def flush(self):
                return
        try:
            out_level = getattr(logging, str(log_cfg.get("print_level_out", "INFO")).upper(), logging.INFO)
            err_level = getattr(logging, str(log_cfg.get("print_level_err", "ERROR")).upper(), logging.ERROR)
            _sys.stdout = _StreamToLogger(logging.getLogger("stdout"), out_level)  # type: ignore[assignment]
            _sys.stderr = _StreamToLogger(logging.getLogger("stderr"), err_level)  # type: ignore[assignment]
        except Exception:
            pass

    # Per-logger level overrides (fine-grained tuning)
    try:
        # Merge preset→user (user wins)
        _preset_loggers = _preset.get("loggers") or {}
        user_loggers = log_cfg.get("loggers") or {}
        log_overrides = ({**_preset_loggers, **user_loggers} if isinstance(user_loggers, dict) else _preset_loggers) if isinstance(_preset_loggers, dict) else (user_loggers if isinstance(user_loggers, dict) else {})
        for name, spec in (log_overrides or {}).items():
            lg = logging.getLogger(str(name))
            if isinstance(spec, dict):
                lvl = spec.get("level")
                if lvl is not None:
                    lg.setLevel(getattr(logging, str(lvl).upper(), logging.INFO))
                if "propagate" in spec:
                    try:
                        lg.propagate = bool(spec.get("propagate"))
                    except Exception:
                        pass
            else:
                lg.setLevel(getattr(logging, str(spec).upper(), logging.INFO))
    except Exception:
        pass

    # Common library noise controls (urllib3/requests/etc.)
    try:
        if bool(log_cfg.get("libs_enable", True)):
            # Merge preset→user (user wins)
            _preset_libs = _preset.get("libs") or {}
            user_libs = log_cfg.get("libs") or log_cfg.get("lib_levels") or {}
            libs = ({**_preset_libs, **user_libs} if isinstance(user_libs, dict) else _preset_libs) if isinstance(_preset_libs, dict) else (user_libs if isinstance(user_libs, dict) else {})
            for name, lvl in (libs or {}).items():
                logging.getLogger(str(name)).setLevel(getattr(logging, str(lvl).upper(), logging.WARNING))
    except Exception:
        pass

    return logfile_used
