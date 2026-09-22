#!/usr/bin/env bash
set -euo pipefail

git add ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md INSPIRATION_ROUND2.md M245_TIMESTAMP_TELEMETRY.md Makefile README.md hat/hatSql/sql_telemetry.go hat/hatSql/m245_timestamp_metrics_test.go hat/hatSql/m245_timestamp_metrics_benchmark_test.go scripts/benchmark-m245.sh scripts/commit-m245.sh scripts/format-m245.sh scripts/push-m245.sh scripts/race-m245.sh scripts/stage-m245.sh scripts/test-m245-package.sh scripts/test-m245.sh scripts/verify-docs-m245.sh scripts/vet-m245.sh
