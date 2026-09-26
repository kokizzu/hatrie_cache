#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/m032_snapshot_provider_test.go \
  hat/hatSql/m032_snapshot_provider_benchmark_test.go \
  hat/hatSql/m_u04_multi_source_snapshot.go
