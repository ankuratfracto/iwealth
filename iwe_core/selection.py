from __future__ import annotations

import re

from .config import CFG


TRUTHY_SET = {str(x).strip().lower() for x in (CFG.get("truthy_values") or ["true","1","yes","y","on"])}


def _is_truthy_val(v) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    if s in TRUTHY_SET:
        return True
    try:
        return float(s) != 0.0
    except Exception:
        return False


def _json_get_first(obj, path: str):
    cur = obj
    for step in (path or "").split("."):
        if isinstance(cur, dict):
            if step in cur:
                cur = cur[step]
            else:
                return None
        elif isinstance(cur, list):
            if step.isdigit():
                idx = int(step)
                if 0 <= idx < len(cur):
                    cur = cur[idx]
                else:
                    return None
            else:
                found = None
                for el in cur:
                    if isinstance(el, dict) and step in el:
                        found = el[step]
                        break
                if found is None:
                    return None
                cur = found
        else:
            return None
    return cur


def _json_any_truthy(obj, paths: list[str]) -> bool:
    for p in (paths or []):
        val = _json_get_first(obj, p)
        if isinstance(val, list):
            if any(_is_truthy_val(x) for x in val):
                return True
        else:
            if _is_truthy_val(val):
                return True
    return False


def _schema_paths(alias: str) -> list[str]:
    node = CFG.get("schema", {})
    for key in (alias or "").split("."):
        if not isinstance(node, dict) or key not in node:
            return []
        node = node[key]
    return list(node) if isinstance(node, list) else ([] if node is None else [str(node)])


def _select_by_criteria(res: dict) -> bool:
    sel_cfg = CFG.get("passes", {}).get("first", {}).get("selection", {}) or {}
    crit = sel_cfg.get("criteria") or {}
    mode = str(crit.get("mode", "any")).strip().lower()
    rules = crit.get("rules") or []
    if not isinstance(rules, list) or not rules:
        return False

    ctx_root = res or {}
    ctx_data = (ctx_root.get("data", {}) or {})
    ctx_pd   = (ctx_data.get("parsedData", {}) or {})

    outcomes: list[bool] = []
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        paths: list[str] = []
        if "alias" in rule:
            paths = _schema_paths(str(rule["alias"]))
        if not paths and "paths" in rule:
            paths = [str(p) for p in (rule.get("paths") or [])]
        if not paths and "path" in rule:
            paths = [str(rule.get("path"))]
        op = str(rule.get("op", "truthy")).lower()
        # Extract values for all paths from multiple contexts
        vals = []
        for p in paths:
            tried = set()
            # try as-is on parsedData
            key = ("pd", p)
            if key not in tried:
                tried.add(key)
                v = _json_get_first(ctx_pd, p)
                if isinstance(v, list):
                    vals.extend(v)
                elif v is not None:
                    vals.append(v)
            # try as-is on data
            key = ("data", p)
            if key not in tried:
                tried.add(key)
                v = _json_get_first(ctx_data, p)
                if isinstance(v, list):
                    vals.extend(v)
                elif v is not None:
                    vals.append(v)
            # try as-is on root
            key = ("root", p)
            if key not in tried:
                tried.add(key)
                v = _json_get_first(ctx_root, p)
                if isinstance(v, list):
                    vals.extend(v)
                elif v is not None:
                    vals.append(v)
            # try prefixed variants
            for pref in ("data.", "data.parsedData."):
                pp = pref + p
                key = ("root", pp)
                if key in tried:
                    continue
                tried.add(key)
                v = _json_get_first(ctx_root, pp)
                if isinstance(v, list):
                    vals.extend(v)
                elif v is not None:
                    vals.append(v)
        ok = False
        if op == "truthy":
            ok = any(_is_truthy_val(v) for v in vals)
        elif op in ("eq", "equals"):
            ok = any(str(v) == str(rule.get("value")) for v in vals)
        elif op in ("neq", "not_equals"):
            ok = any(str(v) != str(rule.get("value")) for v in vals)
        elif op == "contains":
            needle = str(rule.get("value", "")).lower()
            ok = any(needle in str(v).lower() for v in vals)
        elif op == "in":
            choices = set(map(str, rule.get("values") or []))
            ok = any(str(v) in choices for v in vals)
        elif op == "regex":
            pat = re.compile(str(rule.get("value", "")), re.I)
            ok = any(bool(pat.search(str(v))) for v in vals)
        else:
            ok = any(_is_truthy_val(v) for v in vals)
        outcomes.append(ok)
    return any(outcomes) if mode == "any" else all(outcomes)


def _first_pass_has_table(res: dict) -> bool:
    paths = (CFG.get("schema", {}).get("first_pass", {}) or {}).get("has_table") or []
    pdict = (res.get("data", {}) or {}).get("parsedData", {})
    if paths:
        if isinstance(pdict, list):
            return any(_json_any_truthy(item, paths) for item in pdict if isinstance(item, dict))
        return _json_any_truthy(pdict, paths)
    # Legacy fallback (single key)
    field = (CFG.get("passes", {}).get("first", {}).get("selection", {}) or {}).get("has_table_field", "has_table")
    if isinstance(pdict, list):
        for item in pdict:
            if isinstance(item, dict) and field in item and _is_truthy_val(item.get(field)):
                return True
        return False
    return _is_truthy_val(pdict.get(field))


def _second_pass_container(pd_payload: dict | list) -> list:
    if isinstance(pd_payload, list):
        return pd_payload
    paths = (CFG.get("schema", {}).get("second_pass", {}) or {}).get("classification_container") or []
    for p in paths:
        lst = _json_get_first(pd_payload, p)
        if isinstance(lst, list):
            return lst
    return []


def _second_pass_field(item: dict, field_name: str, default=None):
    paths = (CFG.get("schema", {}).get("second_pass", {}).get("fields", {}) or {}).get(field_name) or []
    for p in paths:
        v = _json_get_first(item, p) if "." in p else item.get(p)
        if v is not None and v != "":
            return v
    return default


def _second_pass_org_type(pd_payload: dict | list):
    if isinstance(pd_payload, list):
        return None
    paths = (CFG.get("schema", {}).get("second_pass", {}) or {}).get("organisation_type") or []
    for p in paths:
        v = _json_get_first(pd_payload, p)
        if v:
            return v
    return None


__all__ = [
    "TRUTHY_SET",
    "_is_truthy_val",
    "_json_get_first",
    "_json_any_truthy",
    "_schema_paths",
    "_select_by_criteria",
    "_first_pass_has_table",
    "_second_pass_container",
    "_second_pass_field",
    "_second_pass_org_type",
]
