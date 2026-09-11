#!/usr/bin/env bash
set -euo pipefail

git diff --check -- Makefile README.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md SQL_TEMPORAL_VALIDITY.md hat/hatSql/query.go hat/hatSql/rewrite.go hat/hatSql/time_zone.go hat/hatSql/mz009_temporal_validity_test.go hat/hatSql/mz009_temporal_validity_benchmark_test.go scripts/test-mz009-temporal-validity.sh scripts/test-race-mz009-temporal-validity.sh scripts/benchmark-mz009-temporal-validity.sh scripts/format-mz009-temporal-validity.sh scripts/test-mz009-broad.sh scripts/verify-mz009-docs.sh scripts/review-mz009-temporal-validity.sh scripts/commit-mz009-temporal-validity.sh scripts/push-mz009-temporal-validity.sh
test ! -e scripts/audit-open-inspiration.sh
test ! -e scripts/inspect-time-dispatch.sh
test ! -e scripts/inspect-mz009-surface.sh
printf '%s\n' 'MZ-009 review checks passed.'
test ! -e scripts/audit-open-inspiration.sh
test ! -e scripts/inspect-time-dispatch.sh
test ! -e scripts/inspect-mz009-surface.sh
printf '%s\n' 'MZ-009 review checks passed.'
