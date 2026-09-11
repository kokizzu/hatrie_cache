#!/usr/bin/env bash
set -euo pipefail

git add ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md ENGINE_IDEAS.md README.md SQL_APPROXIMATE_STREAM.md Makefile hat/hatSql/approx_aggregate.go hat/hatSql/approx_stream_test.go hat/hatSql/approx_stream_benchmark_test.go hat/hatSql/query.go scripts/test-ch039-approx-stream.sh scripts/format-ch039-approx-stream.sh scripts/benchmark-ch039-approx-stream.sh scripts/review-ch039-approx-stream.sh scripts/commit-ch039-approx-stream.sh scripts/push-ch039-approx-stream.sh
git commit -m "sql: stream approximate aggregate states"
