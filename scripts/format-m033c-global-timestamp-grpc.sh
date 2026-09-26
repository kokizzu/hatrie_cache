#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/grpc.go \
  hat/hatCache/m033c_global_timestamp_grpc.go \
  hat/hatCache/m033c_global_timestamp_grpc_test.go \
  hat/hatCache/m033c_global_timestamp_grpc_benchmark_test.go
