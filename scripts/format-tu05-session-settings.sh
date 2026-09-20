#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/session.go \
  hat/hatSql/mu016_transactional_view.go \
  hat/hatSql/tu05_session_transaction_settings.go \
  hat/hatSql/tu05_session_transaction_settings_test.go
