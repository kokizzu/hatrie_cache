#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/journal.go \
  hat/hatCache/journal_read_fence_test.go \
  hat/hatCache/journal_read_fence_benchmark_test.go
