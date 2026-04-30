#!/usr/bin/env bash
set -euo pipefail
echo "==> Creating virtual environment"
python -m venv .venv && source .venv/bin/activate
echo "==> Installing dependencies"
pip install --upgrade pip wheel
pip install -r requirements-dev.txt && pip install -e .
[ ! -f .env ] && cp .env.example .env && echo "Edit .env with your credentials"
echo "==> Running unit tests"
SPARK_LOCAL_IP=127.0.0.1 pytest tests/unit/ -v --tb=short
echo "✅  Bootstrap complete. Run: source .venv/bin/activate"
