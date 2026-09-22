#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/write_quorum_policy.go \
  hat/hatCache/t201_per_space_write_quorum_test.go \
  hat/hatCache/t201_per_space_write_quorum_benchmark_test.go \
  hat/hatCache/monitoring.go \
  hat/hatCache/grpc.go
