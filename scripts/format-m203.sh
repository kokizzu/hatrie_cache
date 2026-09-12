#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/journal_subscription.go \
  hat/hatCache/journal_snapshot_free_test.go \
  hat/hatCache/journal_snapshot_free_benchmark_test.go
