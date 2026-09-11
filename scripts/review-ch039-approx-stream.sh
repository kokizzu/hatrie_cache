#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --check
git diff --stat -- ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md ENGINE_IDEAS.md README.md SQL_APPROXIMATE_STREAM.md Makefile hat/hatSql/approx_aggregate.go hat/hatSql/approx_stream_test.go hat/hatSql/approx_stream_benchmark_test.go hat/hatSql/query.go scripts/test-ch039-approx-stream.sh scripts/format-ch039-approx-stream.sh scripts/benchmark-ch039-approx-stream.sh scripts/review-ch039-approx-stream.sh scripts/commit-ch039-approx-stream.sh scripts/push-ch039-approx-stream.sh
rg -n -e 'CH-039|APPROX_COUNT_DISTINCT|APPROX_PERCENTILE|approximate aggregate state' ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md ENGINE_IDEAS.md README.md SQL_APPROXIMATE_STREAM.md hat/hatSql/approx_aggregate.go hat/hatSql/approx_stream_test.go hat/hatSql/approx_stream_benchmark_test.go hat/hatSql/query.go
