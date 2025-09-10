#!/usr/bin/env python3
"""
Find duplicate or similar Python functions across a repository.

Approach:
- Walk the tree for .py files (excluding typical ignored dirs).
- Parse each file's AST and extract function definitions (incl. methods).
- Build a normalized structural token stream that abstracts identifiers and
  literal values but preserves control-flow and operator structure.
- Group exact duplicates by normalized token hash.
- Compute near-duplicate pairs using difflib on token sequences.

Usage:
  python scripts/find_similar_functions.py [--root .] [--threshold 0.9]
"""

from __future__ import annotations

import argparse
import ast
import difflib
import hashlib
import os
from dataclasses import dataclass
from typing import Iterable, List, Tuple


EXCLUDE_DIRS = {".git", "__pycache__", ".venv", "venv", "env", ".mypy_cache", ".pytest_cache", "logs"}


@dataclass
class FuncInfo:
    file: str
    lineno: int
    end_lineno: int
    qualname: str
    tokens: Tuple[str, ...]

    @property
    def loc(self) -> str:
        return f"{self.file}:{self.lineno}"


class Tokenizer(ast.NodeVisitor):
    """Produces a normalized structural token stream for a function body.

    The goal is to ignore specific identifiers and literal values while preserving
    the structure, operators, and statement types so that functions with the same
    logic but different variable names compare as equal/similar.
    """

    def __init__(self) -> None:
        self.tokens: List[str] = []

    # Helpers
    def push(self, t: str) -> None:
        self.tokens.append(t)

    # Generic
    def generic_visit(self, node: ast.AST) -> None:
        # Fallback: include node type to retain structure
        self.push(node.__class__.__name__)
        super().generic_visit(node)

    # Literals and names
    def visit_Constant(self, node: ast.Constant) -> None:
        # Only record the type, not the value
        self.push(f"Const:{type(node.value).__name__}")

    def visit_Name(self, node: ast.Name) -> None:
        # Abstract variable names
        self.push("Name")

    def visit_Attribute(self, node: ast.Attribute) -> None:
        self.push("Attr")
        # Visit value only (ignore attribute string)
        self.visit(node.value)

    # Calls
    def visit_Call(self, node: ast.Call) -> None:
        self.push(f"Call:{len(node.args)}:{len(node.keywords)}")
        # Visit func structurally (ignore identifier specifics)
        self.visit(node.func)
        for a in node.args:
            self.visit(a)
        for kw in node.keywords:
            if kw.value is not None:
                self.visit(kw.value)

    # Operators
    def visit_BinOp(self, node: ast.BinOp) -> None:
        self.push(f"BinOp:{node.op.__class__.__name__}")
        self.visit(node.left)
        self.visit(node.right)

    def visit_UnaryOp(self, node: ast.UnaryOp) -> None:
        self.push(f"UnaryOp:{node.op.__class__.__name__}")
        self.visit(node.operand)

    def visit_BoolOp(self, node: ast.BoolOp) -> None:
        self.push(f"BoolOp:{node.op.__class__.__name__}:{len(node.values)}")
        for v in node.values:
            self.visit(v)

    def visit_Compare(self, node: ast.Compare) -> None:
        ops = ",".join(op.__class__.__name__ for op in node.ops)
        self.push(f"Compare:{ops}:{len(node.comparators)}")
        self.visit(node.left)
        for c in node.comparators:
            self.visit(c)

    # Statements
    def visit_Assign(self, node: ast.Assign) -> None:
        self.push(f"Assign:{len(node.targets)}")
        for t in node.targets:
            self.visit(t)
        self.visit(node.value)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        self.push(f"AugAssign:{node.op.__class__.__name__}")
        self.visit(node.target)
        self.visit(node.value)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        self.push("AnnAssign")
        self.visit(node.target)
        if node.value:
            self.visit(node.value)

    def visit_Return(self, node: ast.Return) -> None:
        self.push("Return")
        if node.value:
            self.visit(node.value)

    def visit_Raise(self, node: ast.Raise) -> None:
        self.push("Raise")
        if node.exc:
            self.visit(node.exc)

    def visit_If(self, node: ast.If) -> None:
        self.push("If")
        self.visit(node.test)
        for s in node.body:
            self.visit(s)
        if node.orelse:
            self.push("Else")
            for s in node.orelse:
                self.visit(s)

    def visit_For(self, node: ast.For) -> None:
        self.push("For")
        self.visit(node.target)
        self.visit(node.iter)
        for s in node.body:
            self.visit(s)
        if node.orelse:
            self.push("Else")
            for s in node.orelse:
                self.visit(s)

    def visit_While(self, node: ast.While) -> None:
        self.push("While")
        self.visit(node.test)
        for s in node.body:
            self.visit(s)
        if node.orelse:
            self.push("Else")
            for s in node.orelse:
                self.visit(s)

    def visit_Try(self, node: ast.Try) -> None:
        self.push("Try")
        for s in node.body:
            self.visit(s)
        for h in node.handlers:
            self.push("Except")
            if h.type:
                self.visit(h.type)
            for s in h.body:
                self.visit(s)
        if node.orelse:
            self.push("Else")
            for s in node.orelse:
                self.visit(s)
        if node.finalbody:
            self.push("Finally")
            for s in node.finalbody:
                self.visit(s)

    def visit_With(self, node: ast.With) -> None:
        self.push(f"With:{len(node.items)}")
        for it in node.items:
            self.visit(it.context_expr)
            if it.optional_vars:
                self.visit(it.optional_vars)
        for s in node.body:
            self.visit(s)

    def visit_ListComp(self, node: ast.ListComp) -> None:
        self.push(f"ListComp:{len(node.generators)}")
        self.visit(node.elt)
        for g in node.generators:
            self.visit(g)

    def visit_DictComp(self, node: ast.DictComp) -> None:
        self.push(f"DictComp:{len(node.generators)}")
        self.visit(node.key)
        self.visit(node.value)
        for g in node.generators:
            self.visit(g)

    def visit_SetComp(self, node: ast.SetComp) -> None:
        self.push(f"SetComp:{len(node.generators)}")
        self.visit(node.elt)
        for g in node.generators:
            self.visit(g)

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:
        self.push(f"GenExp:{len(node.generators)}")
        self.visit(node.elt)
        for g in node.generators:
            self.visit(g)

    def visit_comprehension(self, node: ast.comprehension) -> None:  # type: ignore[override]
        self.push("Comprehension")
        self.visit(node.target)
        self.visit(node.iter)
        for if_ in node.ifs:
            self.visit(if_)


def iter_py_files(root: str) -> Iterable[str]:
    for dirpath, dirnames, filenames in os.walk(root):
        # prune excluded dirs
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIRS and not d.startswith('.')]
        for fn in filenames:
            if fn.endswith('.py'):
                yield os.path.join(dirpath, fn)


def qualname(stack: List[str], name: str) -> str:
    return ".".join([*stack, name]) if stack else name


def extract_functions(path: str) -> List[FuncInfo]:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            src = f.read()
    except (OSError, UnicodeDecodeError):
        return []

    try:
        tree = ast.parse(src, filename=path)
    except SyntaxError:
        return []

    results: List[FuncInfo] = []

    class Walker(ast.NodeVisitor):
        def __init__(self) -> None:
            self.stack: List[str] = []

        def visit_ClassDef(self, node: ast.ClassDef) -> None:
            self.stack.append(node.name)
            self.generic_visit(node)
            self.stack.pop()

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            self._handle_func(node)

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
            self._handle_func(node)

        def _handle_func(self, node: ast.AST) -> None:
            name = getattr(node, 'name', '<lambda>')
            qn = qualname(self.stack, name)

            # Build a body node list without the docstring expr if present
            body_nodes = list(getattr(node, 'body', []))
            if body_nodes and isinstance(body_nodes[0], ast.Expr) and isinstance(getattr(body_nodes[0], 'value', None), ast.Constant) and isinstance(body_nodes[0].value.value, str):
                body_nodes = body_nodes[1:]

            # Tokenize the structure
            tok = Tokenizer()
            for n in body_nodes:
                tok.visit(n)
            tokens = tuple(tok.tokens)

            lineno = getattr(node, 'lineno', 1)
            end_lineno = getattr(node, 'end_lineno', lineno)
            results.append(FuncInfo(file=os.path.relpath(path), lineno=lineno, end_lineno=end_lineno, qualname=qn, tokens=tokens))

            # Recurse into nested defs
            self.generic_visit(node)

    Walker().visit(tree)
    return results


def group_exact(funcs: List[FuncInfo]):
    buckets = {}
    for f in funcs:
        h = hashlib.sha256("\n".join(f.tokens).encode("utf-8")).hexdigest()
        buckets.setdefault(h, []).append(f)
    groups = [v for v in buckets.values() if len(v) > 1]
    # sort groups by size desc then by first location
    groups.sort(key=lambda g: (-len(g), min((fi.file, fi.lineno) for fi in g)))
    return groups


def find_similar(funcs: List[FuncInfo], threshold: float = 0.90, max_pairs: int = 2000):
    pairs: List[Tuple[float, FuncInfo, FuncInfo]] = []
    n = len(funcs)
    # Precompute sizes
    sizes = [len(f.tokens) for f in funcs]
    for i in range(n):
        ti = funcs[i].tokens
        si = sizes[i]
        if si == 0:
            continue
        for j in range(i + 1, n):
            sj = sizes[j]
            # Quick filter by size: within 20% length
            if sj == 0:
                continue
            mn, mx = (si, sj) if si < sj else (sj, si)
            if mn / mx < 0.8:
                continue
            # Compute similarity on token sequences
            ratio = difflib.SequenceMatcher(a=ti, b=funcs[j].tokens, autojunk=False).ratio()
            if ratio >= threshold:
                pairs.append((ratio, funcs[i], funcs[j]))
                if len(pairs) >= max_pairs:
                    return sorted(pairs, key=lambda x: (-x[0], x[1].file, x[2].file))
    return sorted(pairs, key=lambda x: (-x[0], x[1].file, x[2].file))


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", default=".", help="Project root to scan (default: .)")
    ap.add_argument("--threshold", type=float, default=0.92, help="Similarity threshold for near-duplicates (default: 0.92)")
    ap.add_argument("--limit", type=int, default=100, help="Max similar pairs to display (default: 100)")
    args = ap.parse_args()

    files = list(iter_py_files(args.root))
    funcs: List[FuncInfo] = []
    for p in files:
        funcs.extend(extract_functions(p))

    print(f"Scanned {len(files)} Python files; extracted {len(funcs)} functions.")

    # Exact duplicates
    groups = group_exact(funcs)
    if groups:
        print("\nExact duplicates (normalized structure match):")
        for g in groups:
            print(f"- Group of {len(g)}:")
            for f in sorted(g, key=lambda fi: (fi.file, fi.lineno)):
                print(f"  * {f.qualname} @ {f.loc}")
    else:
        print("\nNo exact duplicates found (by normalized structure).")

    # Near duplicates
    pairs = find_similar(funcs, threshold=args.threshold)
    if pairs:
        print(f"\nNear-duplicates (similarity >= {args.threshold:.2f}):")
        for k, (ratio, f1, f2) in enumerate(pairs[: args.limit], 1):
            print(f"{k:>3}. {ratio:.3f}  {f1.qualname} @ {f1.loc}  ~  {f2.qualname} @ {f2.loc}")
    else:
        print(f"\nNo near-duplicates found at threshold {args.threshold:.2f}.")


if __name__ == "__main__":
    main()

