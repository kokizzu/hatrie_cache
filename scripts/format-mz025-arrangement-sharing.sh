#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/m_u05_arrangement_recovery.go \
  hat/hatSql/mz025_arrangement_sharing_test.go \
  hat/hatSql/typed_table_arrangement_snapshot.go \
  hat/hatSql/typed_table_arrangement_stats.go \
  hat/hatSql/typed_table_arrangements.go
