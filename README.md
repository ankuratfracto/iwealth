# iWealth

## Install

Use a virtual environment and install runtime dependencies only for running the app/CLI:

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\\Scripts\\activate
pip install --upgrade pip
pip install -r requirements.txt
```

For development (adds security tooling but not required in production images):

```bash
pip install -r requirements-dev.txt
```

## Security Checks (Dev)

Run static analysis (Bandit) and dependency audit (pip-audit):

```bash
# 1) Bandit — scan Python sources for common issues
bandit -q -r . -x .venv,venv,__pycache__

# 2) pip-audit — scan dependencies for known CVEs
pip-audit -r requirements.txt -r requirements-dev.txt
```

Notes:
- Both commands exit non‑zero on findings. In CI, treat that as a failure.
- Keep secrets in environment variables (e.g., `FRACTO_API_KEY`) or a secret manager — never commit them.

## Analytics Metadata

The combined JSON now includes an optional `analytics` section per document
(enabled by default via `analytics.enable: true` in `config.yaml`). It captures:

- units: detected currency/unit and multiplier (e.g., INR, crore → 10,000,000)
- period_index: parsed period labels, inferred end dates, and fiscal hints
- quality: basic data quality flags and simple checks (e.g., BS tie)
- footnotes: light-weight note reference tokens found in line items

This does not modify Excel values by default; it provides trustworthy metadata
for downstream normalization and metrics. You can now also:

- Include common-size tables as extra sheets by setting `export.statements_workbook.include_common_size_sheets: true` (default true)
- Compute analytics for existing combined JSONs: `python a.py analyze <*_statements.json> [--out-dir DIR]`
