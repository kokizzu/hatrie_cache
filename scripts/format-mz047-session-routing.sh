#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/query.go \
  hat/hatSql/query_manager.go \
  hat/hatSql/mz047_session_compute_routing_test.go \
  hat/hatSql/mz047_session_compute_routing_benchmark_test.go \
  hat/hatSql/mz047_session_compute_routing_baseline_benchmark_test.go
