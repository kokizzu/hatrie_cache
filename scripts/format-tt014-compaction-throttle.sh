#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatStorage/compaction_control.go hat/hatStorage/tt014_compaction_throttling_test.go hat/hatStorage/tt014_compaction_throttling_benchmark_test.go
