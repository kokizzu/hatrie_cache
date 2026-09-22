#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
hat/hatReplication/t203_leader_write_fence.go \
hat/hatReplication/t203_leader_write_fence_test.go \
hat/hatReplication/t203_leader_write_fence_benchmark_test.go
