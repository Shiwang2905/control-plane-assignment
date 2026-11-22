#!/bin/sh
# The full quality gate. Fails fast on the first broken step.
set -eu

echo "==> ruff lint";            ruff check .
echo "==> ruff format (check)";  ruff format --check .
echo "==> mypy (strict)";        mypy
echo "==> bandit (security)";    bandit -q -c pyproject.toml -r src
echo "==> pytest";               pytest -q --cov --cov-report=term-missing "$@"
