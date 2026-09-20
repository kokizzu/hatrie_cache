#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --cached --check
git diff --cached --stat
git diff --cached -- \
  hat/hatSql/session.go \
  hat/hatSql/mu016_transactional_view.go \
  hat/hatSql/tu05_session_transaction_settings.go \
  hat/hatSql/tu05_session_transaction_settings_test.go
git status --short
