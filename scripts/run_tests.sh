#!/usr/bin/env bash
set -euo pipefail
SPARK_LOCAL_IP=127.0.0.1 pytest tests/ \
  --cov=src --cov-report=html:htmlcov \
  --cov-report=term-missing --cov-fail-under=80 -v "$@"
