#!/usr/bin/env bash
set -euo pipefail

files=(
  hat/hatStorage/compaction_scheduler.go
  hat/hatStorage/compaction_scheduler_io.go
  hat/hatStorage/compaction_scheduler_stats.go
  hat/hatStorage/compaction_scheduler_fastpath_test.go
  hat/hatStorage/compaction_scheduler_fastpath_benchmark_test.go
  hat/hatStorage/ch_u28_legacy_fastpath_test.go
)

gofmt -d "${files[@]}"
go test "${files[@]}" -run '^(TestCompactionScheduler|TestCHU28)' -count=1
go test -race "${files[@]}" -run '^(TestCompactionScheduler|TestCHU28)' -count=1
go vet "${files[@]}"
git diff --check -- \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION.md \
  BENCHMARK.md \
  CHU28_DISK_IO_THROTTLING.md \
  "${files[@]}" \
  scripts/benchmark-chu28-standalone.sh \
  scripts/format-chu28-standalone.sh \
  scripts/race-chu28-standalone.sh \
  scripts/test-chu28-standalone.sh \
  scripts/vet-chu28-standalone.sh \
  scripts/verify-chu28-fastpath.sh
