#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatDataStructure/lsm_table.go hat/hatDataStructure/lsm_compaction_scheduler.go \
  hat/hatDataStructure/t216_compaction_scheduler_test.go \
  hat/hatDataStructure/t216_compaction_baseline_benchmark_test.go \
  hat/hatDataStructure/t216_compaction_benchmark_test.go
