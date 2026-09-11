#!/usr/bin/env bash
set -euo pipefail

git add Makefile ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md README.md BENCHMARK.md MZ012_EXACTLY_ONCE_SINK.md hat/hatCache/journal_sink.go hat/hatCache/journal_exactly_once_sink.go hat/hatCache/journal_exactly_once_test.go hat/hatCache/journal_exactly_once_benchmark_test.go scripts/benchmark-mz012-sink.sh scripts/test-mz012-sink.sh scripts/format-mz012-sink.sh scripts/test-race-mz012-sink.sh scripts/test-mz012-broad.sh scripts/verify-mz012-docs.sh scripts/review-mz012.sh scripts/status-mz012.sh scripts/commit-mz012.sh scripts/push-mz012.sh
git diff --cached --check
git commit -m 'Add exactly-once sink transactions'
