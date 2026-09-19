#!/bin/sh
set -eu

gofmt -w \
  hat/hatSql/m_u03_external_snapshot_ingestion.go \
  hat/hatSql/m_u04_multi_source_snapshot.go \
  hat/hatSql/m_u04_multi_source_snapshot_test.go \
  hat/hatSql/m_u04_multi_source_snapshot_benchmark_test.go
