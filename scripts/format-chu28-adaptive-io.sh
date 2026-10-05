#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
(cd "$repo" && gofmt -w hat/hatStorage/chu28_adaptive_io.go hat/hatStorage/chu28_adaptive_io_test.go hat/hatStorage/chu28_adaptive_io_benchmark_test.go hat/hatStorage/compaction_scheduler.go hat/hatStorage/compaction_scheduler_io.go hat/hatStorage/compaction_scheduler_stats.go)
