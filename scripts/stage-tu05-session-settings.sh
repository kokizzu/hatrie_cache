#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  TU05_SESSION_TRANSACTION_SETTINGS.md \
  hat/hatSql/mu016_transactional_view.go \
  hat/hatSql/session.go \
  hat/hatSql/tu05_session_transaction_settings.go \
  hat/hatSql/tu05_session_transaction_settings_test.go \
  scripts/benchmark-tu05-session-settings.sh \
  scripts/commit-tu05-session-settings.sh \
  scripts/format-tu05-session-settings.sh \
  scripts/push-tu05-session-settings.sh \
  scripts/race-tu05-session-settings.sh \
  scripts/review-tu05-session-settings.sh \
  scripts/stage-tu05-session-settings.sh \
  scripts/test-tu05-session-settings.sh \
  scripts/vet-tu05-session-settings.sh
