from __future__ import annotations

import io
import time
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
try:
    from urllib3.util.retry import Retry
except Exception:  # pragma: no cover
    Retry = None  # type: ignore

from .config import CFG
from .pdf_ops import build_pdf_from_pages, get_page_count_from_bytes


logger = logging.getLogger(__name__)


def _get_api_settings() -> dict:
    api = CFG.get("api", {}) or {}
    return {
        "endpoint": api.get("endpoint", "https://prod-ml.fracto.tech/upload-file-smart-ocr"),
        "api_key_env": api.get("api_key_env", "FRACTO_API_KEY"),
        "api_key_cfg": api.get("api_key", ""),
        "timeout": int(api.get("timeout_seconds", 600)),
        "qr_enable": bool((api.get("qr_range", {}) or {}).get("enable", False)),
        "qr_value": str((api.get("qr_range", {}) or {}).get("value", "")).strip(),
    }


def resolve_api_key() -> str:
    s = _get_api_settings()
    env = s["api_key_env"]
    key = (env and requests.utils.os.environ.get(env, "")) or s["api_key_cfg"]
    return str(key or "")


_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        sess = requests.Session()
        # Configure retries if available
        try:
            if Retry is not None:
                retry = Retry(
                    total=3,
                    backoff_factor=0.5,
                    status_forcelist=(429, 500, 502, 503, 504),
                    allowed_methods=("POST", "GET"),
                )
                adapter = HTTPAdapter(max_retries=retry)
                sess.mount("http://", adapter)
                sess.mount("https://", adapter)
        except Exception:
            pass
        _session = sess
    return _session


def call_fracto(
    file_bytes: bytes,
    file_name: str,
    *legacy_args,
    parser_app: Optional[str] = None,
    model: Optional[str] = None,
    extra_accuracy: str = "true",
    qr_range: str | None = None,
) -> Dict[str, Any]:
    """
    Upload a PDF to Fracto OCR and return the JSON response payload inside
    a dict compatible with existing callers: {file, status, data|error}.
    """
    # Backwards compatibility for legacy positional calls
    if legacy_args:
        if len(legacy_args) == 1 and isinstance(legacy_args[0], str) and legacy_args[0].strip() != "":
            extra_accuracy = str(legacy_args[0]).strip().lower()
        else:
            if len(legacy_args) >= 1 and isinstance(legacy_args[0], str) and legacy_args[0].strip():
                parser_app = legacy_args[0]
            if len(legacy_args) >= 2 and isinstance(legacy_args[1], str) and legacy_args[1].strip():
                model = legacy_args[1]
            if len(legacy_args) >= 3 and isinstance(legacy_args[2], str) and legacy_args[2].strip():
                extra_accuracy = legacy_args[2]

    s = _get_api_settings()
    endpoint = s["endpoint"]
    timeout = s["timeout"]

    # Defaults from config if not provided
    parser_app = parser_app or (CFG.get("passes", {}).get("first", {}) or {}).get("parser_app", "")
    model = model or (CFG.get("passes", {}).get("first", {}) or {}).get("model", "tv7")

    files = {
        "file": (file_name, io.BytesIO(file_bytes), "application/pdf"),
    }
    data = {
        "parserApp": parser_app,
        "model": model,
        "extra_accuracy": extra_accuracy,
    }

    # Optional qr_range: prefer explicit param else config
    qr_val = qr_range if (qr_range is not None and str(qr_range).strip() != "") else (s["qr_value"] if s["qr_enable"] else None)
    if qr_val is not None:
        data["qr_range"] = str(qr_val)

    api_key = resolve_api_key()
    if not api_key:
        logger.error("Missing API key. Set %s or add api.api_key in config.yaml.", s["api_key_env"])
        return {"file": file_name, "status": "error", "error": f"Missing API key: set env {s['api_key_env']} or config.api.api_key"}
    headers = {"x-api-key": api_key}

    try:
        logger.info("→ OCR %s (parser=%s, model=%s, extra_accuracy=%s)", file_name, parser_app, model, extra_accuracy)
        start = time.time()
        resp = _get_session().post(
            endpoint,
            headers=headers,
            files=files,
            data=data,
            timeout=timeout,
        )
        resp.raise_for_status()
        elapsed = time.time() - start
        logger.info("✓ %s processed in %.2fs", file_name, elapsed)
        payload = resp.json()
        return {"file": file_name, "status": "ok", "data": payload}
    except requests.HTTPError as exc:
        code = getattr(exc.response, "status_code", None)
        body = ""
        try:
            body = (exc.response.text or "")[:300]
        except Exception:
            pass
        if code == 403:
            logger.error(
                "✗ %s failed: HTTP 403 Forbidden. Verify API key and access to parserApp=%s. Endpoint=%s. Body: %s",
                file_name, parser_app, endpoint, body
            )
        else:
            logger.error("✗ %s failed: HTTP %s. Body: %s", file_name, code, body)
        return {"file": file_name, "status": "error", "http_status": code, "error": body or str(exc)}
    except Exception as exc:
        logger.error("✗ %s failed: %s", file_name, exc)
        return {"file": file_name, "status": "error", "error": str(exc)}


def _split_pdf_bytes(pdf_bytes: bytes, chunk_size: int, min_tail: int) -> list[bytes]:
    total = get_page_count_from_bytes(pdf_bytes)
    if total <= chunk_size:
        return [pdf_bytes]
    ranges: list[tuple[int, int]] = []
    start = 1
    while start <= total:
        end = min(start + chunk_size - 1, total)
        tail = total - end
        if tail < min_tail and ranges:
            s0, e0 = ranges[-1]
            ranges[-1] = (s0, total)
            break
        ranges.append((start, end))
        start = end + 1
    return [build_pdf_from_pages(pdf_bytes, range(s, e + 1)) for s, e in ranges]


def call_fracto_parallel(pdf_bytes: bytes, file_name: str, *, extra_accuracy: str = "true") -> List[Dict[str, Any]]:
    """
    Chunk the PDF according to config and OCR concurrently.
    """
    conc = CFG.get("concurrency", {}) or {}
    chunk_size = int(conc.get("chunk_size_pages", 1))
    max_parallel = int(conc.get("max_parallel", 9))
    min_tail = int(conc.get("min_tail_combine", 1))

    chunks = _split_pdf_bytes(pdf_bytes, chunk_size, min_tail)
    if len(chunks) == 1:
        return [call_fracto(pdf_bytes, file_name, extra_accuracy=extra_accuracy)]

    logger.info("Splitting %s into %d chunks of %d pages each", file_name, len(chunks), chunk_size)

    results: List[Optional[Dict[str, Any]]] = [None] * len(chunks)
    with ThreadPoolExecutor(max_workers=max_parallel) as pool:
        base_stem = Path(file_name).stem
        futures = {
            pool.submit(
                call_fracto,
                chunk,
                f"{base_stem}_page_{i + 1}.pdf",
                extra_accuracy=extra_accuracy,
            ): i
            for i, chunk in enumerate(chunks)
        }
        for fut in as_completed(futures):
            idx = futures[fut]
            try:
                results[idx] = fut.result()
            except Exception as exc:
                logger.error("Chunk %d failed: %s", idx + 1, exc)
                results[idx] = {"file": file_name, "status": "error", "error": str(exc)}

    final = [r for r in results if r is not None]
    return final


__all__ = ["call_fracto", "call_fracto_parallel", "resolve_api_key"]

