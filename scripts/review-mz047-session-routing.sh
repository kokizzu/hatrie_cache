#!/usr/bin/env bash
set -euo pipefail

git diff --check -- \
  Makefile README.md ENGINE_IDEAS.md BENCHMARK.md \
  MZ047_SESSION_COMPUTE_ROUTING.md \
  hat/hatSql/query.go hat/hatSql/query_manager.go \
  hat/hatSql/mz047_session_compute_routing_test.go \
  hat/hatSql/mz047_session_compute_routing_benchmark_test.go \
  hat/hatSql/mz047_session_compute_routing_baseline_benchmark_test.go \
  scripts/format-mz047-session-routing.sh \
  scripts/run-mz047-session-routing-benchmark.sh \
  scripts/benchmark-mz047-session-routing-baseline.sh \
  scripts/test-mz047-session-routing.sh \
  scripts/race-mz047-session-routing.sh \
  scripts/verify-mz047-session-routing.sh \
  scripts/review-mz047-session-routing.sh \
  scripts/stage-mz047-session-routing.sh \
  scripts/commit-mz047-session-routing.sh \
  scripts/push-mz047-session-routing.sh
git status --short
git diff --stat -- \
  Makefile README.md ENGINE_IDEAS.md BENCHMARK.md \
  MZ047_SESSION_COMPUTE_ROUTING.md \
  hat/hatSql/query.go hat/hatSql/query_manager.go \
  hat/hatSql/mz047_session_compute_routing_test.go \
  hat/hatSql/mz047_session_compute_routing_benchmark_test.go \
  hat/hatSql/mz047_session_compute_routing_baseline_benchmark_test.go \
  scripts
