#!/usr/bin/env bash
set -euo pipefail

gofmt -w ./hat/hatTopology/failover.go ./hat/hatTopology/failover_test.go ./hat/hatTopology/failover_large_test.go ./hat/hatTopology/failover_benchmark_test.go
