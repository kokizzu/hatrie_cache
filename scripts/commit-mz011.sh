#!/usr/bin/env bash
set -euo pipefail

git add Makefile ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md README.md BENCHMARK.md MZ011_SINK_CONNECTORS.md hat/hatCache/journal_sink.go hat/hatCache/journal_sink_test.go hat/hatCache/journal_sink_benchmark_test.go scripts/benchmark-mz011-sink.sh scripts/test-mz011-sink.sh scripts/format-mz011-sink.sh scripts/test-race-mz011-sink.sh scripts/test-mz011-broad.sh scripts/verify-mz011-docs.sh scripts/review-mz011.sh scripts/status-mz011.sh scripts/commit-mz011.sh scripts/push-mz011.sh
git diff --cached --check
git commit -m 'Add command journal sink runners'
