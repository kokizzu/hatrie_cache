#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  BENCHMARK.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU005_SESSION_TRANSACTION_SETTINGS.md \
  hat/hatCache/sql_transaction_session.go \
  hat/hatCache/t_u05_sql_transaction_session_benchmark_test.go \
  hat/hatCache/t_u05_sql_transaction_session_test.go \
  scripts/deliver-t-u05.sh \
  scripts/test-t-u05.sh
git diff --cached --check
git commit -m 'feat(cache): add session transaction settings [skip ci]'
git push origin HEAD
