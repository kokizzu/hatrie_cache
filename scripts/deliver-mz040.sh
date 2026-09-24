#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add Makefile ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md MZ040_INCREMENTAL_PERCENTILE.md \
hat/hatSql/m040_incremental_percentile.go \
hat/hatSql/m040_incremental_percentile_benchmark_test.go \
hat/hatSql/m040_incremental_percentile_test.go \
scripts/benchmark-mz040.sh scripts/format-mz040.sh scripts/test-mz040.sh \
scripts/test-mz040-package.sh scripts/race-mz040.sh scripts/vet-mz040.sh \
scripts/deliver-mz040.sh
git diff --cached --check
git commit -m "perf(sql): fastpath single percentile updates"
git push origin HEAD:master
