#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/tu05_session_transaction_settings.go \
  hat/hatSql/tu05_session_transaction_settings_test.go \
  hat/hatSql/tu05_session_transaction_settings_benchmark_test.go
