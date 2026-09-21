#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatStorage/compaction_scheduler.go hat/hatStorage/ch026_compaction_selector_test.go hat/hatStorage/ch026_compaction_selector_benchmark_test.go
