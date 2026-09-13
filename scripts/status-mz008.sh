#!/usr/bin/env bash
set -eu

git status --short
git diff --check -- README.md INSPIRATION_BACKLOG.md BENCHMARK.md MZ008_FRONTIER_ANTICHAIN.md Makefile hat/hatPipeline/frontier_antichain.go hat/hatPipeline/mz008_frontier_antichain_test.go hat/hatPipeline/mz008_frontier_antichain_benchmark_test.go scripts/benchmark-mz008.sh scripts/format-mz008.sh scripts/status-mz008.sh scripts/test-mz008-full.sh scripts/test-mz008-race.sh scripts/test-mz008.sh scripts/commit-mz008.sh scripts/push-mz008.sh
git diff --stat -- README.md INSPIRATION_BACKLOG.md BENCHMARK.md MZ008_FRONTIER_ANTICHAIN.md Makefile hat/hatPipeline/frontier_antichain.go hat/hatPipeline/mz008_frontier_antichain_test.go hat/hatPipeline/mz008_frontier_antichain_benchmark_test.go scripts/benchmark-mz008.sh scripts/format-mz008.sh scripts/status-mz008.sh scripts/test-mz008-full.sh scripts/test-mz008-race.sh scripts/test-mz008.sh scripts/commit-mz008.sh scripts/push-mz008.sh
