#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/relay_backpressure.go \
  hat/hatReplication/model.go \
  hat/hatReplication/t209_relay_backpressure_test.go \
  hat/hatReplication/t209_relay_backpressure_benchmark_test.go \
  hat/hatCache/replication.go \
  hat/hatCache/t209_relay_backpressure_test.go \
  hat/hatCache/t209_relay_backpressure_benchmark_test.go
