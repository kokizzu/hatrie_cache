#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
    hat/hatCache/replica_read_only.go \
    hat/hatCache/tu06_replica_read_only_baseline_benchmark_test.go \
    hat/hatCache/tu06_replica_read_only_test.go
