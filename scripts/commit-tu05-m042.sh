#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  README.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  PRODUCT_IDEA_GAPS.md \
  BENCHMARK.md \
  TU05_SESSION_TRANSACTION_SETTINGS.md \
  hat/hatSql/session.go \
  hat/hatSql/tu05_session_transaction_settings.go \
  hat/hatSql/tu05_session_transaction_settings_test.go \
  hat/hatSql/tu05_session_transaction_settings_benchmark_test.go \
  scripts/format-tu05-m042.sh \
  scripts/test-tu05-m042.sh \
  scripts/benchmark-tu05-m042-baseline.sh \
  scripts/benchmark-tu05-m042.sh \
  scripts/test-tu05-package-m042.sh \
  scripts/race-tu05-m042.sh \
  scripts/vet-tu05-m042.sh \
  scripts/status-tu05-m042.sh \
  scripts/commit-tu05-m042.sh \
  scripts/push-tu05-m042.sh
git commit -m "feat(sql): add session transaction settings"
