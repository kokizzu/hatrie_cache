#!/usr/bin/env bash
set -euo pipefail

mode="${1:?mode is required}"
case "$mode" in
  format)
    gofmt -w hat/hatStorage/compaction_scheduler.go hat/hatStorage/chu27_priority_merge_scheduler_test.go hat/hatStorage/chu27_priority_merge_scheduler_benchmark_test.go
    ;;
  test)
    go test ./hat/hatStorage -run 'TestCHU27CompactionScheduler' -count=1
    ;;
  baseline)
    go test -tags chu27baseline ./hat/hatStorage -run '^$' -bench '^BenchmarkCompactionSchedulerRunC207$' -benchmem -count=5
    ;;
  benchmark)
    go test ./hat/hatStorage -run '^$' -bench '^BenchmarkCHU27CompactionScheduler' -benchmem -count=5
    ;;
  race)
    go test -race ./hat/hatStorage -run 'TestCHU27CompactionScheduler' -count=1
    ;;
  vet)
    go vet ./hat/hatStorage
    ;;
  package)
    go test ./hat/hatStorage -count=1
    ;;
  race-package)
    go test -race ./hat/hatStorage -count=1
    ;;
  status)
    git status --short --branch
    ;;
  stage)
    git add Makefile scripts/run-chu27-priority.sh hat/hatStorage/compaction_scheduler.go hat/hatStorage/chu27_priority_merge_scheduler_test.go hat/hatStorage/chu27_priority_merge_scheduler_benchmark_test.go CHU27_PRIORITY_MERGE_SCHEDULER.md PRODUCT_IDEA_GAPS.md BENCHMARK.md
    ;;
  commit)
    git commit -m 'adopt priority compaction scheduling [skip ci]'
    ;;
  push)
    git push origin HEAD:master
    ;;
  docs)
    rg -n 'CH-U27|ScheduleWithPriority|45,077|21,542' CHU27_PRIORITY_MERGE_SCHEDULER.md PRODUCT_IDEA_GAPS.md BENCHMARK.md
    ;;
  check)
    git diff --check
    ;;
  *)
    printf 'unsupported mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
