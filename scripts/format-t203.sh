#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/leader_fencing.go \
  hat/hatCache/monitoring.go \
  hat/hatCache/grpc.go \
  hat/hatCache/grpc_scalar_batch.go \
  hat/hatCache/grpc_structured_batch.go \
  hat/hatCache/t203_leader_fencing_test.go \
  hat/hatCache/t203_leader_fencing_benchmark_test.go
