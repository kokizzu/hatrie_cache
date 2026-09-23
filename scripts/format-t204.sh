#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatTopology/election.go \
  hat/hatTopology/supervised_failover.go \
  hat/hatTopology/t204_supervised_failover_test.go \
  hat/hatTopology/t204_supervised_failover_benchmark_test.go \
  hat/hatCache/election.go
