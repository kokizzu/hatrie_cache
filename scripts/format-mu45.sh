#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/typed_table_join_arrangement_selection.go \
  hat/hatSql/m_u45_join_arrangement_selection_test.go \
  hat/hatSql/m_u45_join_arrangement_selection_benchmark_test.go
