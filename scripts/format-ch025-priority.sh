#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
    hat/hatStorage/ch025_compaction_priority.go \
    hat/hatStorage/ch025_compaction_priority_baseline_benchmark_test.go \
    hat/hatStorage/ch025_compaction_priority_test.go \
    hat/hatStorage/compaction_scheduler.go
