#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/mz050_timeline_branch.go \
  hat/hatCache/mz050_timeline_branch_test.go \
  hat/hatCache/mz050_timeline_branch_benchmark_test.go
