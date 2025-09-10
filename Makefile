VENV := .venv
PYTHON := python3
PIP := $(VENV)/bin/pip
BANDIT := $(VENV)/bin/bandit
PIP_AUDIT := $(VENV)/bin/pip-audit

.PHONY: help venv install install-dev bandit pip-audit audit

help:
	@echo "Targets:"
	@echo "  venv         - create local virtualenv in $(VENV)"
	@echo "  install      - install runtime deps into $(VENV)"
	@echo "  install-dev  - install runtime + dev deps (bandit, pip-audit)"
	@echo "  bandit       - run Bandit static analysis"
	@echo "  pip-audit    - run dependency vulnerability audit"
	@echo "  audit        - run both Bandit and pip-audit"

venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

install-dev: venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt -r requirements-dev.txt

bandit: install-dev
	$(BANDIT) -q -r . -x .venv,venv,__pycache__

pip-audit: install-dev
	$(PIP_AUDIT) -r requirements.txt -r requirements-dev.txt

audit: bandit pip-audit
	@echo "Security checks completed"

