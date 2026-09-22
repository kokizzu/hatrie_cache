#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
hat/hatReplication/t204_supervised_failover.go \
hat/hatReplication/t204_supervised_failover_test.go \
hat/hatReplication/t204_supervised_failover_benchmark_test.go
