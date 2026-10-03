#!/usr/bin/env bash
set -euo pipefail

git status --short
git add Makefile README.md BENCHMARK.md CHG44_QUERY_TRACE_EXPORT.md \
  hat/hatSql/query_trace_export.go \
  hat/hatSql/query_trace_export_test.go \
  hat/hatSql/query_trace_spans_benchmark_test.go \
  scripts/test-chg44-query-trace-export.sh \
  scripts/format-chg44-query-trace-export.sh \
  scripts/test-chg44-query-trace-export-package.sh \
  scripts/benchmark-chg44-query-trace-export.sh \
  scripts/race-chg44-query-trace-export.sh \
  scripts/vet-chg44-query-trace-export.sh \
  scripts/commit-chg44-query-trace-export.sh \
  scripts/push-chg44-query-trace-export.sh
git diff --cached --check
git commit -m 'feat(sql): add opt-in query trace span exporter [skip ci]'
