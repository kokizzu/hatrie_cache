#!/usr/bin/env bash
set -euo pipefail

paths=(
  Makefile
  README.md
  CH044_EXPORTABLE_TRACE.md
  hat/hatSql/query_trace_spans.go
  hat/hatSql/query_trace_exporter_test.go
  hat/hatTrace/otlp_exporter.go
  hat/hatTrace/otlp_exporter_test.go
  hat/hatTrace/otlp_exporter_benchmark_test.go
  scripts/benchmark-chg44.sh
  scripts/benchmark-chg44-exporter.sh
  scripts/format-chg44.sh
  scripts/race-chg44.sh
  scripts/test-chg44-exporter.sh
  scripts/test-chg44-package.sh
  scripts/verify-chg44-docs.sh
  scripts/vet-chg44.sh
  scripts/commit-chg44.sh
  scripts/push-chg44.sh
)

git add -- "${paths[@]}"
git diff --cached --check
git commit --only -m "feat(trace): add opt-in OTLP HTTP exporter" -- "${paths[@]}"
