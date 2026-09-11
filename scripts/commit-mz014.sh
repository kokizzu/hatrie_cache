#!/usr/bin/env bash
set -euo pipefail

git add Makefile ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md README.md BENCHMARK.md MZ014_UPSERT_BATCH.md hat/hatDataStructure/upsert_batch.go hat/hatDataStructure/upsert_batch_test.go hat/hatDataStructure/upsert_batch_benchmark_test.go scripts/benchmark-mz014-upsert.sh scripts/test-mz014-upsert.sh scripts/format-mz014-upsert.sh scripts/test-race-mz014-upsert.sh scripts/test-mz014-broad.sh scripts/verify-mz014-docs.sh scripts/review-mz014.sh scripts/status-mz014.sh scripts/commit-mz014.sh scripts/push-mz014.sh
git diff --cached --check
git commit -m 'Add upsert batch consolidation'
