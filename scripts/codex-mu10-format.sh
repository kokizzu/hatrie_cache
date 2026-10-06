#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/differential_temporal_join.go \
  hat/hatSql/differential_temporal_join_compaction_scheduler.go \
  hat/hatSql/m_u10_compaction_scheduler_test.go \
  hat/hatSql/m_u10_compaction_scheduler_benchmark_test.go
