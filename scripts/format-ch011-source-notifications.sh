#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
  hat/hatSql/ch011_source_notifications.go \
  hat/hatSql/ch011_source_notifications_baseline_test.go \
  hat/hatSql/ch011_source_notifications_test.go
