#!/usr/bin/env bash
set -euo pipefail

git add Makefile ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md README.md BENCHMARK.md MZ013_SOURCE_CONNECTOR_CHECKPOINTS.md hat/hatCache/journal_source_checkpoint.go hat/hatCache/journal_source_checkpoint_test.go hat/hatCache/journal_source_checkpoint_benchmark_test.go scripts/benchmark-mz013-source.sh scripts/test-mz013-source.sh scripts/format-mz013-source.sh scripts/test-race-mz013-source.sh scripts/test-mz013-broad.sh scripts/verify-mz013-docs.sh scripts/review-mz013.sh scripts/status-mz013.sh scripts/commit-mz013.sh scripts/push-mz013.sh
git diff --cached --check
git commit -m 'Add source connector checkpoints'
