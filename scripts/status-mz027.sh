#!/usr/bin/env bash
set -eu

git status --short
git diff --check -- README.md INSPIRATION_BACKLOG.md BENCHMARK.md MZ027_READ_HOLD_DIAGNOSTICS.md Makefile hat/hatPipeline/frontier_retention.go hat/hatPipeline/mz027_read_hold_diagnostics_test.go hat/hatPipeline/mz027_read_hold_diagnostics_benchmark_test.go scripts/format-mz027.sh scripts/test-mz027.sh scripts/test-mz027-race.sh scripts/test-mz027-full.sh scripts/benchmark-mz027.sh scripts/status-mz027.sh
git diff --stat -- README.md INSPIRATION_BACKLOG.md BENCHMARK.md MZ027_READ_HOLD_DIAGNOSTICS.md Makefile hat/hatPipeline/frontier_retention.go hat/hatPipeline/mz027_read_hold_diagnostics_test.go hat/hatPipeline/mz027_read_hold_diagnostics_benchmark_test.go scripts/format-mz027.sh scripts/test-mz027.sh scripts/test-mz027-race.sh scripts/test-mz027-full.sh scripts/benchmark-mz027.sh scripts/status-mz027.sh
