#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/conflict_policy.go \
  hat/hatReplication/t210_master_master_conflict_hooks_test.go \
  hat/hatReplication/t210_master_master_conflict_hooks_baseline_benchmark_test.go \
  hat/hatReplication/t210_master_master_conflict_hooks_benchmark_test.go
