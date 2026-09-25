#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatTopology/tt001_automatic_rebalance.go hat/hatTopology/tt001_automatic_rebalance_test.go hat/hatTopology/tt001_automatic_rebalance_benchmark_test.go
