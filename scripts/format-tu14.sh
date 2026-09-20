#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatTopology/tu14_vshard.go hat/hatTopology/tu14_vshard_test.go hat/hatTopology/tu14_vshard_benchmark_test.go
