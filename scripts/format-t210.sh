#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/conflict_hook.go \
  hat/hatReplication/conflict_policy.go \
  hat/hatReplication/t210_conflict_hooks_test.go \
  hat/hatReplication/t210_conflict_hooks_benchmark_test.go
